package match

import (
	"context"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/geo"
	"github.com/mertmetin/listing-alerts/internal/listing"
	"github.com/mertmetin/listing-alerts/internal/search"
)

type memChanges struct {
	mu   sync.Mutex
	rows []*search.Search
}

func (m *memChanges) put(s *search.Search) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows = append(m.rows, s)
}

func (m *memChanges) Changes(_ context.Context, since time.Time, fn func(*search.Search) error) (time.Time, error) {
	m.mu.Lock()
	rows := append([]*search.Search(nil), m.rows...)
	m.mu.Unlock()
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].UpdatedAt.Before(rows[j].UpdatedAt) })
	last := since
	for _, r := range rows {
		if !r.UpdatedAt.Before(since) {
			if err := fn(r); err != nil {
				return last, err
			}
			last = r.UpdatedAt
		}
	}
	return last, nil
}

type memPub struct{ alerts []event.Alert }

func (p *memPub) Publish(_ context.Context, a []event.Alert) error {
	p.alerts = append(p.alerts, a...)
	return nil
}

var (
	t0      = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	orleans = geo.Point{Lat: 45.47, Lng: -75.51}
)

func shard(id, n int, ch Changes, pub Publisher) *Shard {
	return &Shard{ID: id, Ring: search.NewRing(n, 64), Store: ch, Pub: pub,
		Metrics: NewMetrics(prometheus.NewRegistry()), Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now: func() time.Time { return t0 }}
}

func ev(id string, t event.Type, public bool, price, close float64, showAddr bool) event.ListingEvent {
	lat, lng := orleans.Lat, orleans.Lng
	l := listing.Listing{Key: "X1", TransactionType: "For Sale", City: "Ottawa", StreetNumber: "12", StreetName: "Main",
		Latitude: &lat, Longitude: &lng, AddressDisplay: showAddr}
	e := event.ListingEvent{ID: id, Type: t, ListingKey: "X1", Public: public, Price: &price, Listing: l, ObservedAt: t0}
	if close > 0 {
		e.ClosePrice = &close
		l.ClosePrice = &close
	}
	return e
}

func TestAlertsRespectAudience(t *testing.T) {
	ch := &memChanges{}
	area := []geo.Polygon{geo.Circle(orleans, 3, 12)}
	ch.put(&search.Search{ID: "pub", Site: "mertmetin", UserID: "u1", Email: "a@example.com", Audience: search.Public, Transaction: "sale", Areas: area, UpdatedAt: t0})
	ch.put(&search.Search{ID: "vow", Site: "mertmetin", UserID: "u2", Email: "b@example.com", Audience: search.VOW, Transaction: "sale", Areas: area, UpdatedAt: t0})
	pub := &memPub{}
	s := shard(0, 1, ch, pub)
	if err := s.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Handle(context.Background(), []event.ListingEvent{ev("e1", event.Sold, true, 500000, 512000, false)}); err != nil {
		t.Fatal(err)
	}
	if len(pub.alerts) != 2 {
		t.Fatalf("alerts %d", len(pub.alerts))
	}
	for _, a := range pub.alerts {
		switch a.Audience {
		case "public":
			if a.ClosePrice != nil || a.Listing.ClosePrice != nil {
				t.Error("public alert carries the sold price")
			}
			if a.Listing.StreetName != "" || a.Listing.Latitude != nil {
				t.Error("public alert carries a withheld address")
			}
		case "vow":
			if a.ClosePrice == nil || *a.ClosePrice != 512000 || a.Listing.StreetName != "Main" {
				t.Errorf("vow alert lost data: %+v", a)
			}
		}
		if a.ID != event.AlertID("e1", a.SearchID) || a.Key() != "mertmetin:"+a.UserID {
			t.Errorf("alert id/key %+v", a)
		}
	}

	// now_public reaches public users as a new listing, and VOW users not at all.
	pub.alerts = nil
	_ = s.Handle(context.Background(), []event.ListingEvent{ev("e2", event.NowPublic, true, 500000, 0, true)})
	if len(pub.alerts) != 1 || pub.alerts[0].Audience != "public" || pub.alerts[0].Type != event.NewListing {
		t.Fatalf("now_public alerts %+v", pub.alerts)
	}
}

func TestOnlyOwnerShardAlerts(t *testing.T) {
	ch := &memChanges{}
	ch.put(&search.Search{ID: "s", Site: "hotrentals", UserID: "u", Email: "c@example.com", Audience: search.Public, Transaction: "sale",
		Areas: []geo.Polygon{geo.Circle(orleans, 3, 12)}, UpdatedAt: t0})
	const n = 4
	total := 0
	for id := range n {
		pub := &memPub{}
		s := shard(id, n, ch, pub)
		if err := s.Load(context.Background()); err != nil {
			t.Fatal(err)
		}
		_ = s.Handle(context.Background(), []event.ListingEvent{ev("e1", event.NewListing, true, 1, 0, true)})
		total += len(pub.alerts)
	}
	if total != 1 {
		t.Fatalf("%d shards produced %d alerts for one event, want exactly 1", n, total)
	}
}

func TestPollAppliesUpdatesAndDeletes(t *testing.T) {
	ch := &memChanges{}
	base := search.Search{ID: "s", Site: "mertmetin", UserID: "u", Email: "d@example.com", Audience: search.VOW, Transaction: "sale",
		Areas: []geo.Polygon{geo.Circle(orleans, 3, 12)}, MaxPrice: 400000, UpdatedAt: t0}
	ch.put(&base)
	pub := &memPub{}
	s := shard(0, 1, ch, pub)
	_ = s.Load(context.Background())
	e := ev("e1", event.NewListing, true, 450000, 0, true)
	_ = s.Handle(context.Background(), []event.ListingEvent{e})
	if len(pub.alerts) != 0 {
		t.Fatal("over budget alerted")
	}

	raised := base
	raised.MaxPrice, raised.UpdatedAt = 500000, t0.Add(time.Second)
	ch.put(&raised)
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.ID = "e2"
	_ = s.Handle(context.Background(), []event.ListingEvent{e})
	if len(pub.alerts) != 1 {
		t.Fatalf("after raising the budget: %d alerts", len(pub.alerts))
	}

	gone := raised
	gone.Deleted, gone.UpdatedAt = true, t0.Add(2*time.Second)
	ch.put(&gone)
	_ = s.Poll(context.Background())
	e.ID = "e3"
	_ = s.Handle(context.Background(), []event.ListingEvent{e})
	if len(pub.alerts) != 1 || s.Index.Len() != 0 {
		t.Fatalf("deleted search still alerts (%d) or is held (%d)", len(pub.alerts), s.Index.Len())
	}
	// Polling again re-reads the lookback window without re-applying.
	before := testCount(s)
	_ = s.Poll(context.Background())
	if testCount(s) != before {
		t.Fatal("lookback re-read re-applied unchanged searches")
	}
}

func testCount(s *Shard) float64 { return testutil.ToFloat64(s.Metrics.SearchUpdates) }
