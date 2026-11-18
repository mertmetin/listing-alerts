// Package match runs one matcher shard: it keeps the saved searches it owns
// in a search.Index, follows changes to them, matches the listing events
// whose location it owns, and publishes alerts.
package match

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/search"
)

// Topics.
const (
	TopicEvents = "listing.events"
	TopicAlerts = "alerts.pending"
)

// Changes streams saved searches updated at or after since (search.Store).
type Changes interface {
	Changes(ctx context.Context, since time.Time, fn func(*search.Search) error) (time.Time, error)
}

// Publisher writes alerts and returns once they are acknowledged.
type Publisher interface {
	Publish(ctx context.Context, alerts []event.Alert) error
}

// Shard is one matcher shard.
type Shard struct {
	ID      int
	Ring    *search.Ring
	Index   *search.Index
	Store   Changes
	Pub     Publisher
	Metrics *Metrics
	Log     *slog.Logger
	Now     func() time.Time

	owns    search.Owns
	cursor  time.Time
	applied map[string]time.Time // search ID -> updated_at applied
}

// Lookback re-reads search changes this far back on every poll: a
// transaction that committed late can carry an older updated_at than rows
// already read. Re-applying an unchanged search is skipped.
const Lookback = 10 * time.Second

// Load reads every saved search and indexes the ones this shard owns.
// Coverings are computed on all cores.
func (s *Shard) Load(ctx context.Context) error {
	s.owns = s.Ring.OwnsFunc(s.ID)
	s.applied = map[string]time.Time{}
	if s.Index == nil {
		s.Index = search.NewIndex()
	}
	start := s.Now()

	in := make(chan *search.Search, 1024)
	out := make(chan search.Prepared, 1024)
	var wg sync.WaitGroup
	for range runtime.NumCPU() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sr := range in {
				out <- search.Prepare(sr)
			}
		}()
	}
	go func() { wg.Wait(); close(out) }()

	errc := make(chan error, 1)
	go func() {
		last, err := s.Store.Changes(ctx, time.Time{}, func(sr *search.Search) error {
			if sr.Deleted {
				return nil
			}
			select {
			case in <- sr:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		s.cursor = last
		close(in)
		errc <- err
	}()
	held := 0
	for p := range out {
		if s.Index.Upsert(p, s.owns) {
			held++
		}
		s.applied[p.S.ID] = p.S.UpdatedAt
	}
	if err := <-errc; err != nil {
		return err
	}
	s.Metrics.observeIndex(s.Index)
	s.Log.Info("searches loaded", "held", held, "took", time.Since(start).Round(time.Millisecond))
	return nil
}

// Follow applies search changes until ctx is cancelled.
func (s *Shard) Follow(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := s.Poll(ctx); err != nil && ctx.Err() == nil {
			s.Log.Error("search changes", "err", err)
		}
	}
}

// Poll applies changes since the last poll (minus Lookback).
func (s *Shard) Poll(ctx context.Context) error {
	n := 0
	last, err := s.Store.Changes(ctx, s.cursor.Add(-Lookback), func(sr *search.Search) error {
		// Only ever move a search forward: the lookback re-read must not
		// re-apply (or roll back to) a version already seen.
		if at, ok := s.applied[sr.ID]; ok && !sr.UpdatedAt.After(at) {
			return nil
		}
		s.Index.Upsert(search.Prepare(sr), s.owns)
		s.applied[sr.ID] = sr.UpdatedAt
		n++
		return nil
	})
	if last.After(s.cursor) {
		s.cursor = last
	}
	if n > 0 {
		s.Metrics.SearchUpdates.Add(float64(n))
		if s.Index.NeedsCompact() {
			s.Index.Compact(s.owns)
		}
		s.Metrics.observeIndex(s.Index)
	}
	return err
}

// Handle matches a batch of listing events and publishes the alerts.
func (s *Shard) Handle(ctx context.Context, events []event.ListingEvent) error {
	var (
		alerts []event.Alert
		buf    []*search.Search
	)
	for _, e := range events {
		q := search.QueryFor(e)
		if s.Ring.QueryOwner(q) != s.ID {
			s.Metrics.Events.WithLabelValues("other_shard").Inc()
			continue
		}
		s.Metrics.Events.WithLabelValues("owned").Inc()
		t := time.Now()
		buf = s.Index.Match(q, buf[:0])
		s.Metrics.MatchTime.Observe(time.Since(t).Seconds())
		now := s.Now().UTC()
		for _, sr := range buf {
			alerts = append(alerts, NewAlert(e, sr, now))
		}
	}
	if err := s.Pub.Publish(ctx, alerts); err != nil {
		return err
	}
	now := s.Now()
	for _, a := range alerts {
		s.Metrics.Alerts.WithLabelValues(string(a.Type), a.Audience).Inc()
		s.Metrics.Latency.Observe(now.Sub(a.ObservedAt).Seconds())
	}
	return nil
}

// NewAlert builds the alert for one (event, search) match, applying the
// audience's display rules: public users never get sold or leased prices,
// and never the street address of a listing whose brokerage withheld it.
func NewAlert(e event.ListingEvent, s *search.Search, now time.Time) event.Alert {
	a := event.Alert{
		V: event.Version, ID: event.AlertID(e.ID, s.ID),
		Site: s.Site, UserID: s.UserID, Email: s.Email, Audience: string(s.Audience),
		SearchID: s.ID, SearchName: s.Name,
		EventID: e.ID, Type: e.Type, ListingKey: e.ListingKey,
		Price: e.Price, PrevPrice: e.PrevPrice, ClosePrice: e.ClosePrice,
		GeoPrecision: e.GeoPrecision, Listing: e.Listing,
		ObservedAt: e.ObservedAt, MatchedAt: now,
	}
	if s.Audience == search.Public {
		if a.Type == event.NowPublic {
			a.Type = event.NewListing
		}
		a.ClosePrice, a.Listing.ClosePrice = nil, nil
		if !a.Listing.AddressDisplay {
			l := &a.Listing
			l.StreetNumber, l.StreetName, l.StreetSuffix, l.Unit, l.Address, l.PostalCode = "", "", "", "", "", ""
			l.Latitude, l.Longitude = nil, nil
		}
	}
	return a
}

// Run consumes listing.events until ctx is cancelled, committing offsets
// after each batch's alerts are published (at-least-once; alert IDs dedupe).
func (s *Shard) Run(ctx context.Context, cl *kgo.Client, maxBatch int) error {
	defer cl.AllowRebalance()
	for {
		fs := cl.PollRecords(ctx, maxBatch)
		if ctx.Err() != nil {
			return nil
		}
		fs.EachError(func(t string, p int32, err error) { s.Log.Error("fetch", "topic", t, "partition", p, "err", err) })
		recs := fs.Records()
		events := make([]event.ListingEvent, 0, len(recs))
		for _, r := range recs {
			e, err := event.DecodeListingEvent(r.Value)
			if err != nil {
				s.Log.Error("skipping undecodable event", "offset", r.Offset, "err", err)
				continue
			}
			events = append(events, e)
		}
		for {
			err := s.Handle(ctx, events)
			if err == nil || ctx.Err() != nil {
				break
			}
			s.Log.Error("handle batch, retrying", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
		}
		if len(recs) > 0 {
			if err := cl.CommitRecords(ctx, recs...); err != nil && ctx.Err() == nil {
				s.Log.Error("commit offsets", "err", err)
			}
		}
		cl.AllowRebalance()
	}
}

// KafkaPublisher writes alerts keyed by site:user.
type KafkaPublisher struct {
	Client *kgo.Client
	Topic  string
}

func (p KafkaPublisher) Publish(ctx context.Context, alerts []event.Alert) error {
	if len(alerts) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, 0, len(alerts))
	for _, a := range alerts {
		b, err := a.Encode()
		if err != nil {
			return err
		}
		recs = append(recs, &kgo.Record{Topic: p.Topic, Key: []byte(a.Key()), Value: b,
			Headers: []kgo.RecordHeader{{Key: "id", Value: []byte(a.ID)}, {Key: "type", Value: []byte(a.Type)}}})
	}
	var errs []error
	for _, r := range p.Client.ProduceSync(ctx, recs...) {
		if r.Err != nil {
			errs = append(errs, r.Err)
		}
	}
	return errors.Join(errs...)
}

// Metrics for a shard.
type Metrics struct {
	Events        *prometheus.CounterVec // listing events seen: owned / other_shard
	Alerts        *prometheus.CounterVec // alerts published, by type and audience
	SearchUpdates prometheus.Counter
	MatchTime     prometheus.Histogram
	Latency       prometheus.Histogram // poller observed -> alert published
	Searches      prometheus.Gauge
	Entries       prometheus.Gauge
}

// NewMetrics registers shard metrics.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	ns, sub := "listing_alerts", "match"
	m := &Metrics{
		Events:        prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "events_total", Help: "Listing events seen, by whether this shard owns them."}, []string{"owner"}),
		Alerts:        prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "alerts_total", Help: "Alerts published."}, []string{"type", "audience"}),
		SearchUpdates: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "search_updates_total", Help: "Saved-search changes applied."}),
		MatchTime:     prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "match_seconds", Help: "Time to match one event.", Buckets: prometheus.ExponentialBuckets(1e-6, 2, 18)}),
		Latency:       prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "pipeline_latency_seconds", Help: "Poller observed the change -> alert published.", Buckets: prometheus.ExponentialBuckets(0.005, 2, 14)}),
		Searches:      prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "searches", Help: "Saved searches held by this shard."}),
		Entries:       prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "index_entries", Help: "Cell entries in this shard's index."}),
	}
	reg.MustRegister(m.Events, m.Alerts, m.SearchUpdates, m.MatchTime, m.Latency, m.Searches, m.Entries)
	return m
}

func (m *Metrics) observeIndex(ix *search.Index) {
	st := ix.Stats()
	m.Searches.Set(float64(st.Searches))
	m.Entries.Set(float64(st.Entries))
}
