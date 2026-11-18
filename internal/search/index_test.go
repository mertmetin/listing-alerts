package search

import (
	"fmt"
	"sort"
	"testing"

	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/geo"
)

// reference is the matching rule written the obvious way, over every search.
func reference(ss []*Search, q Query) []string {
	var out []string
	for _, s := range ss {
		if s.Deleted {
			continue
		}
		t := q.Type
		if s.Audience == Public {
			if !q.Public {
				continue
			}
			if t == event.NowPublic {
				t = event.NewListing
			}
		} else if t == event.NowPublic {
			continue
		}
		evs := s.Events
		if len(evs) == 0 {
			evs = DefaultEvents
		}
		wanted := false
		for _, e := range evs {
			wanted = wanted || e == t
		}
		if !wanted || (s.Transaction == "lease") != q.Lease {
			continue
		}
		if (s.MinPrice > 0 && (!q.HasPrice || float32(q.Price) < float32(s.MinPrice))) ||
			(s.MaxPrice > 0 && (!q.HasPrice || float32(q.Price) > float32(s.MaxPrice))) ||
			(s.MinBeds > 0 && q.Beds < s.MinBeds) || (s.MinBaths > 0 && q.Baths < s.MinBaths) {
			continue
		}
		if len(s.Types) > 0 {
			ok := false
			for _, ty := range s.Types {
				ok = ok || ty == q.SubType
			}
			if !ok {
				continue
			}
		}
		in := false
		for _, a := range s.Areas {
			in = in || (q.HasPoint && a.Contains(q.Point))
		}
		for _, c := range s.Cities {
			for _, qc := range q.Cities {
				in = in || c == qc
			}
		}
		if in {
			out = append(out, s.ID)
		}
	}
	sort.Strings(out)
	return out
}

func ids(ss []*Search) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = s.ID
	}
	sort.Strings(out)
	return out
}

func TestIndexMatchesReference(t *testing.T) {
	syn := NewSynth(7)
	var all []*Search
	ix := NewIndex()
	for i := range 10000 {
		s := syn.Search(i)
		all = append(all, s)
		ix.Upsert(Prepare(s), nil)
	}
	matched := 0
	for i := range 2000 {
		q := QueryFor(syn.Event(i))
		want := reference(all, q)
		got := ids(ix.Match(q, nil))
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("event %d (%+v):\n got %d %v\nwant %d %v", i, q, len(got), got, len(want), want)
		}
		matched += len(got)
	}
	t.Logf("2000 events, %d matches against 10000 searches", matched)
	if matched == 0 {
		t.Fatal("nothing matched: test data is too sparse to mean anything")
	}
}

func TestAudienceRules(t *testing.T) {
	area := []geo.Polygon{geo.Circle(geo.Point{Lat: 45.42, Lng: -75.70}, 3, 12)}
	pub := &Search{ID: "pub", Audience: Public, Transaction: "sale", Areas: area}
	vow := &Search{ID: "vow", Audience: VOW, Transaction: "sale", Areas: area}
	ix := NewIndex()
	ix.Upsert(Prepare(pub), nil)
	ix.Upsert(Prepare(vow), nil)
	q := Query{Point: geo.Point{Lat: 45.42, Lng: -75.70}, HasPoint: true, Beds: -1, Baths: -1}

	cases := []struct {
		typ    event.Type
		public bool
		want   string
	}{
		{event.NewListing, false, "[vow]"},    // VOW-only listing: public users never see it
		{event.NewListing, true, "[pub vow]"}, // shown everywhere
		{event.NowPublic, true, "[pub]"},      // public users' "new listing"; old news to VOW users
		{event.Sold, true, "[pub vow]"},       // (price is stripped for public users downstream)
		{event.OffMarket, true, "[]"},         // not in the default alert set
	}
	for _, c := range cases {
		q.Type, q.Public = c.typ, c.public
		if got := fmt.Sprint(ids(ix.Match(q, nil))); got != c.want {
			t.Errorf("%s public=%v: got %s want %s", c.typ, c.public, got, c.want)
		}
	}
}

func TestCityNames(t *testing.T) {
	cases := []struct{ city, county, want string }{
		{"Toronto W08", "Toronto", "[toronto w08 toronto]"},
		{"Barrhaven", "Ottawa", "[barrhaven ottawa]"},
		{"Orleans - Cumberland and Area", "Ottawa", "[orleans - cumberland and area ottawa]"},
		{"Kanata", "", "[kanata]"},
		{"Ottawa", "Ottawa", "[ottawa]"},
	}
	for _, c := range cases {
		if got := fmt.Sprint(CityNames(c.city, c.county)); got != c.want {
			t.Errorf("CityNames(%q, %q) = %s, want %s", c.city, c.county, got, c.want)
		}
	}
}

func TestUpsertDeleteCompact(t *testing.T) {
	ix := NewIndex()
	pt := geo.Point{Lat: 43.65, Lng: -79.38}
	s := &Search{ID: "a", Audience: VOW, Transaction: "sale", Areas: []geo.Polygon{geo.Circle(pt, 2, 10)}, MaxPrice: 500000}
	ix.Upsert(Prepare(s), nil)
	q := Query{Point: pt, HasPoint: true, Price: 700000, HasPrice: true, Beds: -1, Baths: -1, Type: event.NewListing}
	if len(ix.Match(q, nil)) != 0 {
		t.Fatal("over budget matched")
	}
	s2 := *s
	s2.MaxPrice = 800000 // user raised the budget
	ix.Upsert(Prepare(&s2), nil)
	if len(ix.Match(q, nil)) != 1 || ix.Len() != 1 {
		t.Fatal("updated search did not match exactly once")
	}
	s3 := s2
	s3.Deleted = true
	ix.Upsert(Prepare(&s3), nil)
	if len(ix.Match(q, nil)) != 0 || ix.Len() != 0 {
		t.Fatal("deleted search still matches")
	}
	ix.Upsert(Prepare(&s2), nil)
	ix.Compact(nil)
	if st := ix.Stats(); st.Slots != 1 || len(ix.Match(q, nil)) != 1 {
		t.Fatalf("after compact %+v", st)
	}
}

// TestShardsAgree: routing each event to its owning shard and matching
// there gives exactly what one unsharded index gives.
func TestShardsAgree(t *testing.T) {
	syn := NewSynth(11)
	const shards = 5
	ring := NewRing(shards, 64)
	one := NewIndex()
	parts := make([]*Index, shards)
	for i := range parts {
		parts[i] = NewIndex()
	}
	owns := make([]Owns, shards)
	for i := range owns {
		owns[i] = ring.OwnsFunc(i)
	}
	held := 0
	for i := range 20000 {
		p := Prepare(syn.Search(i))
		one.Upsert(p, nil)
		for sh := range parts {
			if parts[sh].Upsert(p, owns[sh]) {
				held++
			}
		}
	}
	t.Logf("20000 searches held %d times over %d shards (%.2fx replication)", held, shards, float64(held)/20000)
	for i := range 3000 {
		q := QueryFor(syn.Event(i))
		want := fmt.Sprint(ids(one.Match(q, nil)))
		if got := fmt.Sprint(ids(parts[ring.QueryOwner(q)].Match(q, nil))); got != want {
			t.Fatalf("event %d: owner shard %s, unsharded %s", i, got, want)
		}
	}
}

func TestRingBalanceAndMovement(t *testing.T) {
	keys := make([]string, 50000)
	for i := range keys {
		keys[i] = "g:" + geo.Encode(geo.Point{Lat: 42 + float64(i%250)*0.025, Lng: -83 + float64(i/250)*0.04}, 4).String() + fmt.Sprint(i)
	}
	r5, r6 := NewRing(5, 128), NewRing(6, 128)
	count := make([]int, 5)
	moved := 0
	for _, k := range keys {
		a, b := r5.Owner(k), r6.Owner(k)
		count[a]++
		if a != b {
			moved++
			if b != 5 {
				t.Fatalf("key %s moved between old shards %d -> %d", k, a, b)
			}
		}
	}
	for s, n := range count {
		if share := float64(n) / float64(len(keys)); share < 0.15 || share > 0.25 {
			t.Errorf("shard %d holds %.1f%% of keys", s, share*100)
		}
	}
	if frac := float64(moved) / float64(len(keys)); frac < 0.10 || frac > 0.24 {
		t.Errorf("adding a 6th shard moved %.1f%% of keys, want about 1/6", frac*100)
	}
}
