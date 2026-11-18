package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/listing"
	"github.com/mertmetin/listing-alerts/internal/reso"
)

// ---- fakes -----------------------------------------------------------------

type memStore struct {
	mu       sync.Mutex
	cursors  map[string]reso.Cursor
	versions map[string]map[string]Version
	failNext error
	commits  int
	listings map[string]*listing.Listing
}

func newMemStore() *memStore {
	return &memStore{cursors: map[string]reso.Cursor{}, versions: map[string]map[string]Version{}, listings: map[string]*listing.Listing{}}
}

func (s *memStore) Cursor(_ context.Context, src string) (reso.Cursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursors[src], nil
}

func (s *memStore) Versions(_ context.Context, src string, keys []string) (map[string]Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]Version{}
	for _, k := range keys {
		if v, ok := s.versions[src][k]; ok {
			out[k] = v
		}
	}
	return out, nil
}

func (s *memStore) PresentKeys(_ context.Context, src string) (map[string]Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]Version{}
	for k, v := range s.versions[src] {
		if v.Present {
			out[k] = v
		}
	}
	return out, nil
}

func (s *memStore) Commit(_ context.Context, src string, cur *reso.Cursor, snaps []Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.failNext; err != nil {
		s.failNext = nil
		return err // nothing applied: atomic
	}
	if s.versions[src] == nil {
		s.versions[src] = map[string]Version{}
	}
	for _, sn := range snaps {
		s.versions[src][sn.Key] = sn.Version
		if sn.Listing != nil {
			s.listings[sn.Key] = sn.Listing
		}
	}
	if cur != nil {
		s.cursors[src] = *cur
	}
	s.commits++
	return nil
}

type memPub struct {
	mu       sync.Mutex
	log      []event.Change
	failNext error
}

func (p *memPub) Publish(_ context.Context, cs []event.Change) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.failNext; err != nil {
		p.failNext = nil
		// Partial publish: the first half made it to the broker.
		p.log = append(p.log, cs[:len(cs)/2]...)
		return err
	}
	p.log = append(p.log, cs...)
	return nil
}

func (p *memPub) ids() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.log))
	for i, c := range p.log {
		out[i] = c.ID
	}
	return out
}

// uniqueIDs is what a consumer that dedupes by ID would see.
func (p *memPub) unique() []event.Change {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := map[string]bool{}
	var out []event.Change
	for _, c := range p.log {
		if !seen[c.ID] {
			seen[c.ID] = true
			out = append(out, c)
		}
	}
	return out
}

// scriptFeed serves canned pages: every Fetch returns the records modified
// after the cursor, ordered, PageSize at a time.
type scriptFeed struct {
	mu       sync.Mutex
	rows     map[string]map[string]any
	pageSize int
	active   func(map[string]any) bool
	keys     []string // for Keys; nil means all rows
	keysErr  error
}

func (f *scriptFeed) set(key string, ts time.Time, price float64, status string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows[key] = map[string]any{
		"ListingKey": key, "ModificationTimestamp": reso.Timestamp(ts), "ListPrice": price,
		"StandardStatus": status, "TransactionType": "For Sale", "InternetEntireListingDisplayYN": true,
	}
}

func (f *scriptFeed) sorted(filter func(map[string]any) bool, byKey bool) []map[string]any {
	var out []map[string]any
	for _, r := range f.rows {
		if filter(r) {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		ti, tj := out[i]["ModificationTimestamp"].(string), out[j]["ModificationTimestamp"].(string)
		if byKey || ti == tj {
			return out[i]["ListingKey"].(string) < out[j]["ListingKey"].(string)
		}
		return ti < tj
	})
	return out
}

func (f *scriptFeed) Fetch(_ context.Context, cur reso.Cursor, now time.Time) (reso.Page, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur.Phase == "" {
		cur = reso.Cursor{Phase: reso.Baseline, BaselineStartedAt: now}
	}
	if cur.Phase == reso.Baseline {
		rows := f.sorted(func(r map[string]any) bool {
			return r["StandardStatus"] == "Active" && r["ListingKey"].(string) > cur.Key
		}, true)
		p := reso.Page{Backfill: true, More: true, Next: cur}
		if len(rows) > f.pageSize {
			rows = rows[:f.pageSize]
		}
		p.Records = rows
		if len(rows) > 0 {
			p.Next.Key = rows[len(rows)-1]["ListingKey"].(string)
		}
		if len(rows) < f.pageSize {
			p.Next = reso.Cursor{Phase: reso.Incremental, TS: cur.BaselineStartedAt.Add(-time.Minute)}
		}
		return p, nil
	}
	ts := reso.Timestamp(cur.TS)
	rows := f.sorted(func(r map[string]any) bool {
		rt := r["ModificationTimestamp"].(string)
		if cur.Key == "" {
			return rt >= ts
		}
		return rt > ts || (rt == ts && r["ListingKey"].(string) > cur.Key)
	}, false)
	if len(rows) > f.pageSize {
		rows = rows[:f.pageSize]
	}
	p := reso.Page{Records: rows, Next: cur, More: len(rows) == f.pageSize}
	if n := len(rows); n > 0 {
		t, _ := time.Parse(time.RFC3339Nano, rows[n-1]["ModificationTimestamp"].(string))
		p.Next = reso.Cursor{Phase: reso.Incremental, TS: t, Key: rows[n-1]["ListingKey"].(string)}
	}
	return p, nil
}

func (f *scriptFeed) Recent(_ context.Context, since time.Time) ([]map[string]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := reso.Timestamp(since)
	return f.sorted(func(r map[string]any) bool { return r["ModificationTimestamp"].(string) >= s }, false), nil
}

func (f *scriptFeed) Keys(context.Context, string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.keysErr != nil {
		return nil, f.keysErr
	}
	if f.keys != nil {
		return f.keys, nil
	}
	var ks []string
	for k := range f.rows {
		ks = append(ks, k)
	}
	return ks, nil
}

var t0 = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

func newPoller(feed *scriptFeed, store *memStore, pub *memPub) *Poller {
	return &Poller{
		Source: event.SourceVOW, Feed: feed, Store: store, Pub: pub, Rec: NopRecorder{},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: NewMetrics(prometheus.NewRegistry()),
		Now: func() time.Time { return t0 }, Lookback: 3 * time.Minute, MinKeepFraction: 0.5,
	}
}

func stepUntilIdle(t *testing.T, p *Poller) {
	t.Helper()
	for i := 0; i < 100; i++ {
		more, err := p.Step(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !more {
			return
		}
	}
	t.Fatal("poller never caught up")
}

// ---- tests -----------------------------------------------------------------

func TestDiffRules(t *testing.T) {
	p := 500000.0
	l := listing.Listing{Key: "A", ListPrice: &p, ModifiedAt: t0}
	prev := map[string]Version{}

	ch, _, sk := Diff("vow", []listing.Listing{l, l}, prev, t0, false)
	if len(ch) != 1 || sk.Unchanged != 1 {
		t.Fatalf("first sighting: %d changes, skips %+v", len(ch), sk)
	}

	older := l
	older.ModifiedAt = t0.Add(-time.Hour)
	p2 := 1.0
	older.ListPrice = &p2
	if ch, _, sk = Diff("vow", []listing.Listing{older}, prev, t0, false); len(ch) != 0 || sk.Stale != 1 {
		t.Fatalf("stale record produced %d changes", len(ch))
	}

	noise := l
	noise.ModifiedAt = t0.Add(time.Minute)
	if ch, _, _ = Diff("vow", []listing.Listing{noise}, prev, t0, false); len(ch) != 0 {
		t.Fatal("timestamp-only bump produced an event")
	}

	prev["A"] = Version{Hash: l.ContentHash(), ModifiedAt: t0, Present: false}
	if ch, _, _ = Diff("vow", []listing.Listing{l}, prev, t0, false); len(ch) != 1 {
		t.Fatal("reappearing listing produced no event")
	}
}

func TestBaselineIsBackfillThenIncrementalIsLive(t *testing.T) {
	feed := &scriptFeed{rows: map[string]map[string]any{}, pageSize: 2}
	for i := range 5 {
		feed.set(fmt.Sprintf("A%d", i), t0.Add(-time.Duration(i+2)*time.Hour), 500000, "Active")
	}
	feed.set("SOLD", t0.Add(-30*time.Hour), 400000, "Closed") // history: not in baseline
	store, pub := newMemStore(), &memPub{}
	p := newPoller(feed, store, pub)

	stepUntilIdle(t, p)
	if n := len(pub.log); n != 5 {
		t.Fatalf("baseline published %d, want 5", n)
	}
	for _, c := range pub.log {
		if !c.Backfill {
			t.Fatalf("baseline event not marked backfill: %+v", c)
		}
	}

	feed.set("A1", t0.Add(-30*time.Second), 479000, "Active") // price drop
	feed.set("NEW", t0.Add(-20*time.Second), 650000, "Active")
	stepUntilIdle(t, p)
	live := pub.log[5:]
	if len(live) != 2 || live[0].ListingKey != "A1" || live[0].Backfill || live[1].ListingKey != "NEW" {
		t.Fatalf("live events %+v", live)
	}

	// Idle polls (cursor + lookback re-read) publish nothing.
	stepUntilIdle(t, p)
	stepUntilIdle(t, p)
	if len(pub.log) != 7 {
		t.Fatalf("idle polls published %d extra", len(pub.log)-7)
	}
}

func TestPublishFailureKeepsCursorAndRetryReusesIDs(t *testing.T) {
	feed := &scriptFeed{rows: map[string]map[string]any{}, pageSize: 100}
	store, pub := newMemStore(), &memPub{}
	store.cursors[event.SourceVOW] = reso.Cursor{Phase: reso.Incremental, TS: t0.Add(-time.Hour)}
	for i := range 6 {
		feed.set(fmt.Sprintf("K%d", i), t0.Add(-time.Duration(10-i)*time.Minute), float64(100+i), "Active")
	}
	p := newPoller(feed, store, pub)

	pub.failNext = errors.New("broker unavailable")
	if _, err := p.Step(context.Background()); err == nil {
		t.Fatal("want publish error")
	}
	if store.commits != 0 {
		t.Fatal("cursor or versions committed after a failed publish")
	}

	stepUntilIdle(t, p)
	got := pub.unique()
	if len(got) != 6 {
		t.Fatalf("after retry a deduping consumer sees %d events, want 6", len(got))
	}
	if len(pub.log) != 9 { // 3 from the partial publish + 6 on retry
		t.Fatalf("published %d records", len(pub.log))
	}
}

func TestCrashBetweenPublishAndCommitIsIdempotent(t *testing.T) {
	feed := &scriptFeed{rows: map[string]map[string]any{}, pageSize: 100}
	store, pub := newMemStore(), &memPub{}
	store.cursors[event.SourceVOW] = reso.Cursor{Phase: reso.Incremental, TS: t0.Add(-time.Hour)}
	feed.set("X1", t0.Add(-10*time.Minute), 900000, "Active")
	feed.set("X2", t0.Add(-9*time.Minute), 800000, "Closed")
	p := newPoller(feed, store, pub)

	store.failNext = errors.New("postgres went away")
	if _, err := p.Step(context.Background()); err == nil {
		t.Fatal("want commit error")
	}
	first := pub.ids()

	// "Restart": a new poller over the same store and log.
	p2 := newPoller(feed, store, pub)
	stepUntilIdle(t, p2)
	all := pub.ids()
	if len(all) != 4 || fmt.Sprint(all[:2]) != fmt.Sprint(all[2:]) || fmt.Sprint(first) != fmt.Sprint(all[:2]) {
		t.Fatalf("re-published IDs differ: %v", all)
	}
	if len(pub.unique()) != 2 {
		t.Fatal("dedupe by ID should leave 2 events")
	}
}

func TestReconcile(t *testing.T) {
	feed := &scriptFeed{rows: map[string]map[string]any{}, pageSize: 100}
	for i := range 4 {
		feed.set(fmt.Sprintf("R%d", i), t0.Add(-time.Duration(i+2)*time.Hour), 2000, "Active")
	}
	store, pub := newMemStore(), &memPub{}
	p := newPoller(feed, store, pub)
	p.Source = event.SourceIDX
	stepUntilIdle(t, p)

	// The feed suddenly lists 1 of 4: looks broken, refuse.
	feed.keys = []string{"R0"}
	if _, err := p.Reconcile(context.Background()); err == nil {
		t.Fatal("reconcile should refuse when the feed shrinks below half")
	}

	// R3 leased and left IDX.
	feed.keys = []string{"R0", "R1", "R2"}
	n, err := p.Reconcile(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("removed %d, err %v", n, err)
	}
	last := pub.log[len(pub.log)-1]
	if last.Kind != event.Removed || last.ListingKey != "R3" {
		t.Fatalf("last event %+v", last)
	}
	if n, _ := p.Reconcile(context.Background()); n != 0 {
		t.Fatal("second reconcile removed again")
	}

	// R3 comes back on the market with the same content: a new Observed.
	feed.set("R3", t0.Add(-10*time.Second), 2000, "Active")
	feed.keys = nil
	stepUntilIdle(t, p)
	last = pub.log[len(pub.log)-1]
	if last.Kind != event.Observed || last.ListingKey != "R3" {
		t.Fatalf("reappearance: last event %+v", last)
	}
}

func TestOffMLSSnapshot(t *testing.T) {
	dir := t.TempDir()
	priv := filepath.Join(dir, "private-listings.json")
	write := func(s string) {
		if err := os.WriteFile(priv, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(`[
	  {"id":"a","city":"Orleans","county":"Ottawa","price":1650,"beds":1},
	  {"id":"b","city":"Ottawa","price":2100,"active":false},
	  {"id":"c","city":"Kanata","price":2400,"expires_at":"2026-01-01T00:00:00Z"}
	]`)
	store, pub := newMemStore(), &memPub{}
	p := &SnapshotPoller{
		Source: event.SourceOffMLS, Files: OffMLSFiles{Paths: []string{priv, filepath.Join(dir, "submitted-listings.json")}},
		Store: store, Pub: pub, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: NewMetrics(prometheus.NewRegistry()), Now: func() time.Time { return t0 },
	}
	if err := p.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(pub.log) != 1 || pub.log[0].ListingKey != "P-a" || *pub.log[0].Listing.ListPrice != 1650 {
		t.Fatalf("events %+v", pub.log)
	}
	if err := p.Step(context.Background()); err != nil || len(pub.log) != 1 {
		t.Fatalf("unchanged snapshot published again (%d), err %v", len(pub.log), err)
	}

	write(`[]`) // landlord marked it rented
	if err := p.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if last := pub.log[len(pub.log)-1]; last.Kind != event.Removed || last.ListingKey != "P-a" {
		t.Fatalf("want Removed P-a, got %+v", last)
	}

	os.Remove(priv)
	if err := p.Step(context.Background()); err == nil {
		t.Fatal("missing files must be an error, not a mass removal")
	}
}
