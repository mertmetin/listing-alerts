package detect

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"
	"time"

	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/listing"
)

var t0 = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

type step struct {
	src      string
	status   string // StandardStatus; "" with removed=true for Removed
	price    float64
	close    float64
	removed  bool
	backfill bool
	tx       string
}

// changes turns steps into a change stream for one listing, one minute apart.
func changes(key string, steps ...step) []event.Change {
	var out []event.Change
	for i, s := range steps {
		at := t0.Add(time.Duration(i) * time.Minute)
		if s.removed {
			out = append(out, event.NewRemoved(s.src, key, "h", at, at))
			continue
		}
		tx := s.tx
		if tx == "" {
			tx = "For Sale"
		}
		p := s.price
		l := listing.Listing{Key: key, TransactionType: tx, StandardStatus: s.status, ListPrice: &p, ModifiedAt: at}
		if s.close > 0 {
			c := s.close
			l.ClosePrice = &c
		}
		out = append(out, event.NewObserved(s.src, l, at, s.backfill))
	}
	return out
}

func run(cs []event.Change) (State, []event.ListingEvent) {
	var s State
	var all []event.ListingEvent
	for _, c := range cs {
		var evs []event.ListingEvent
		s, evs, _ = Apply(s, c, t0)
		all = append(all, evs...)
	}
	return s, all
}

func types(evs []event.ListingEvent) string {
	var ts []string
	for _, e := range evs {
		ts = append(ts, fmt.Sprintf("%s(public=%v)", e.Type, e.Public))
	}
	return fmt.Sprint(ts)
}

const vow, idx, off = event.SourceVOW, event.SourceIDX, event.SourceOffMLS

func TestLifecycle(t *testing.T) {
	cases := []struct {
		name  string
		steps []step
		want  string
	}{
		{"new then price drop then sold, VOW and IDX",
			[]step{{src: vow, status: "Active", price: 800000}, {src: idx, status: "Active", price: 800000},
				{src: vow, status: "Active", price: 775000}, {src: idx, status: "Active", price: 775000},
				{src: vow, status: "Closed", price: 775000, close: 790000}, {src: idx, removed: true}},
			"[new_listing(public=false) now_public(public=true) price_drop(public=true) sold(public=true)]"},
		{"IDX first: public from the start, VOW adds nothing",
			[]step{{src: idx, status: "Active", price: 500000}, {src: vow, status: "Active", price: 500000}},
			"[new_listing(public=true)]"},
		{"IDX drops the listing before VOW reports the sale: still announced publicly",
			[]step{{src: vow, status: "Active", price: 600000}, {src: idx, status: "Active", price: 600000},
				{src: idx, removed: true}, {src: vow, status: "Closed", price: 600000, close: 610000}},
			"[new_listing(public=false) now_public(public=true) sold(public=true)]"},
		{"IDX opt-out: VOW-only listing never becomes public",
			[]step{{src: vow, status: "Active", price: 450000}, {src: vow, status: "Closed", price: 450000, close: 440000}},
			"[new_listing(public=false) sold(public=false)]"},
		{"conditional, deal falls through, back on market",
			[]step{{src: vow, status: "Active", price: 700000}, {src: vow, status: "Active Under Contract", price: 700000},
				{src: vow, status: "Active", price: 700000}},
			"[new_listing(public=false) pending(public=false) back_on_market(public=false)]"},
		{"expired then relisted with the same key",
			[]step{{src: idx, status: "Active", price: 900000}, {src: idx, status: "Expired", price: 900000},
				{src: idx, status: "Active", price: 875000}},
			"[new_listing(public=true) off_market(public=true) back_on_market(public=true)]"},
		{"lease: leased with close price",
			[]step{{src: vow, status: "Active", price: 2400, tx: "For Lease"}, {src: vow, status: "Closed", price: 2400, close: 2350, tx: "For Lease"}},
			"[new_listing(public=false) leased(public=false)]"},
		{"price increase is not an alert",
			[]step{{src: vow, status: "Active", price: 500000}, {src: vow, status: "Active", price: 525000}},
			"[new_listing(public=false)]"},
		{"baseline is silent, live changes after it alert",
			[]step{{src: vow, status: "Active", price: 500000, backfill: true}, {src: vow, status: "Active", price: 480000}},
			"[price_drop(public=false)]"},
		{"never seen on the market: a closed record alone is not a sale alert",
			[]step{{src: vow, status: "Closed", price: 500000, close: 500000}},
			"[]"},
		{"off-MLS rental removed by the landlord",
			[]step{{src: off, status: "Active", price: 1650, tx: "For Lease"}, {src: off, removed: true}},
			"[new_listing(public=true) off_market(public=true)]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, evs := run(changes("K", tc.steps...))
			if got := types(evs); got != tc.want {
				t.Errorf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}

func TestEventFields(t *testing.T) {
	_, evs := run(changes("K",
		step{src: vow, status: "Active", price: 800000},
		step{src: vow, status: "Active", price: 775000},
		step{src: vow, status: "Closed", price: 775000, close: 790000}))
	drop, sold := evs[1], evs[2]
	if *drop.PrevPrice != 800000 || *drop.Price != 775000 {
		t.Errorf("price drop %v -> %v", *drop.PrevPrice, *drop.Price)
	}
	if *sold.Price != 775000 || *sold.ClosePrice != 790000 || sold.PrevPhase != event.PhaseActive {
		t.Errorf("sold %+v", sold)
	}
}

func TestRedeliveryAndReplayAreNoOps(t *testing.T) {
	cs := changes("K",
		step{src: vow, status: "Active", price: 800000},
		step{src: idx, status: "Active", price: 800000},
		step{src: vow, status: "Active", price: 750000},
		step{src: idx, removed: true})
	s, first := run(cs)
	for i, c := range cs { // replay everything on top of the final state
		var evs []event.ListingEvent
		var out Outcome
		s, evs, out = Apply(s, c, t0)
		if len(evs) != 0 || (out != Duplicate && out != Stale) {
			t.Fatalf("replay of change %d: outcome %s, %d events", i, out, len(evs))
		}
	}
	if len(first) != 3 {
		t.Fatalf("first pass %s", types(first))
	}
}

func TestOutOfOrderSourceIsStale(t *testing.T) {
	cs := changes("K", step{src: vow, status: "Active", price: 1}, step{src: vow, status: "Active", price: 2})
	s, _ := run(cs[1:])
	if _, evs, out := Apply(s, cs[0], t0); out != Stale || len(evs) != 0 {
		t.Fatalf("older change: %s, %d events", out, len(evs))
	}
}

// TestWorkersAgree is the scaling property: any number of workers, each
// owning some listings and applying their changes in order, produce the
// same set of events as one worker. That is what partitioning by
// ListingKey buys.
func TestWorkersAgree(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	var stream []event.Change
	statuses := []string{"Active", "Active", "Active", "Active Under Contract", "Closed", "Expired"}
	srcs := []string{vow, vow, idx, idx, off}
	for k := range 300 {
		key := fmt.Sprintf("L%03d", k)
		var steps []step
		for range 2 + rng.IntN(8) {
			s := step{src: srcs[rng.IntN(len(srcs))], status: statuses[rng.IntN(len(statuses))], price: float64(400000 + 10000*rng.IntN(20))}
			if rng.IntN(8) == 0 {
				s = step{src: s.src, removed: true}
			}
			steps = append(steps, s)
		}
		stream = append(stream, changes(key, steps...)...)
	}
	// Interleave listings like a real topic would, keeping per-listing order.
	stream = interleave(stream, rng)

	ids := func(workers int) []string {
		states := make([]map[string]State, workers)
		for i := range states {
			states[i] = map[string]State{}
		}
		var out []string
		for _, c := range stream {
			w := int(fnv(c.ListingKey) % uint32(workers))
			s, evs, _ := Apply(states[w][c.ListingKey], c, t0)
			states[w][c.ListingKey] = s
			for _, e := range evs {
				out = append(out, e.ID)
			}
		}
		sort.Strings(out)
		return out
	}
	one := ids(1)
	if len(one) < 300 {
		t.Fatalf("stream produced only %d events", len(one))
	}
	for _, n := range []int{2, 6, 12} {
		if got := ids(n); fmt.Sprint(got) != fmt.Sprint(one) {
			t.Fatalf("%d workers: %d events, 1 worker: %d", n, len(got), len(one))
		}
	}
}

// interleave merges per-listing sequences in random order, preserving each
// listing's own order.
func interleave(cs []event.Change, rng *rand.Rand) []event.Change {
	queues := map[string][]event.Change{}
	var keys []string
	for _, c := range cs {
		if _, ok := queues[c.ListingKey]; !ok {
			keys = append(keys, c.ListingKey)
		}
		queues[c.ListingKey] = append(queues[c.ListingKey], c)
	}
	var out []event.Change
	for len(keys) > 0 {
		i := rng.IntN(len(keys))
		k := keys[i]
		out = append(out, queues[k][0])
		queues[k] = queues[k][1:]
		if len(queues[k]) == 0 {
			keys = append(keys[:i], keys[i+1:]...)
		}
	}
	return out
}

func fnv(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
