package detect

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/mertmetin/listing-alerts/internal/event"
)

type memStore struct {
	mu       sync.Mutex
	rows     map[string]State
	failNext error
	// bumpBeforeSave simulates another worker writing between Load and Save.
	bumpBeforeSave string
}

func (m *memStore) Load(_ context.Context, keys []string) (map[string]State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]State{}
	for _, k := range keys {
		if s, ok := m.rows[k]; ok {
			out[k] = s
		}
	}
	return out, nil
}

func (m *memStore) Save(_ context.Context, states []State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if k := m.bumpBeforeSave; k != "" {
		m.bumpBeforeSave = ""
		s := m.rows[k]
		s.Version++
		m.rows[k] = s
	}
	if err := m.failNext; err != nil {
		m.failNext = nil
		return err
	}
	for _, s := range states {
		if m.rows[s.Key].Version != s.Version {
			return ErrConflict
		}
	}
	for _, s := range states {
		s.Version++
		m.rows[s.Key] = s
	}
	return nil
}

type memPub struct {
	mu       sync.Mutex
	events   []event.ListingEvent
	failNext error
}

func (p *memPub) Publish(_ context.Context, evs []event.ListingEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.failNext; err != nil {
		p.failNext = nil
		return err
	}
	p.events = append(p.events, evs...)
	return nil
}

func (p *memPub) uniqueTypes() map[event.Type]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := map[string]bool{}
	out := map[event.Type]int{}
	for _, e := range p.events {
		if !seen[e.ID] {
			seen[e.ID] = true
			out[e.Type]++
		}
	}
	return out
}

func newDetector(st Store, pub Publisher) *Detector {
	return &Detector{Store: st, Pub: pub, Metrics: NewMetrics(prometheus.NewRegistry()),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return t0 }}
}

func TestProcessPublishFailureSavesNothing(t *testing.T) {
	st, pub := &memStore{rows: map[string]State{}}, &memPub{}
	d := newDetector(st, pub)
	batch := changes("A", step{src: vow, status: "Active", price: 1})

	pub.failNext = errors.New("broker down")
	if err := d.Process(context.Background(), batch); err == nil {
		t.Fatal("want error")
	}
	if len(st.rows) != 0 {
		t.Fatal("state saved although publish failed")
	}
	if err := d.Process(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if got := pub.uniqueTypes(); got[event.NewListing] != 1 {
		t.Fatalf("events %v", got)
	}
}

func TestProcessSaveFailureRepublishesSameIDs(t *testing.T) {
	st, pub := &memStore{rows: map[string]State{}}, &memPub{}
	d := newDetector(st, pub)
	batch := changes("A", step{src: vow, status: "Active", price: 2}, step{src: vow, status: "Active", price: 1})

	st.failNext = errors.New("postgres down")
	if err := d.Process(context.Background(), batch); err == nil {
		t.Fatal("want error")
	}
	if err := d.Process(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(pub.events) != 4 {
		t.Fatalf("published %d", len(pub.events))
	}
	if got := pub.uniqueTypes(); got[event.NewListing] != 1 || got[event.PriceDrop] != 1 {
		t.Fatalf("after dedup %v", got)
	}
	// And the batch delivered again (offset not committed) is a no-op.
	if err := d.Process(context.Background(), batch); err != nil || len(pub.events) != 4 {
		t.Fatalf("redelivery published more: %d, err %v", len(pub.events), err)
	}
}

func TestProcessRecomputesOnConflict(t *testing.T) {
	st, pub := &memStore{rows: map[string]State{}}, &memPub{}
	d := newDetector(st, pub)
	if err := d.Process(context.Background(), changes("A", step{src: vow, status: "Active", price: 1})); err != nil {
		t.Fatal(err)
	}
	st.bumpBeforeSave = "A" // another worker wrote A meanwhile
	more := changes("A", step{src: vow, status: "Active", price: 9}, step{src: vow, status: "Active", price: 5})[1:]
	if err := d.Process(context.Background(), more); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(d.Metrics.Conflicts); got != 1 {
		t.Fatalf("conflicts %v", got)
	}
}
