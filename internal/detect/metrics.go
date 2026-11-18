package detect

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mertmetin/listing-alerts/internal/event"
)

// Metrics for the detector.
type Metrics struct {
	Changes   *prometheus.CounterVec // changes consumed, by outcome
	Events    *prometheus.CounterVec // events published, by type
	Conflicts prometheus.Counter     // batches recomputed after a state conflict
	Invalid   prometheus.Counter     // records that could not be decoded
	BatchTime prometheus.Histogram
	// PipelineLatency: poller observed the change -> detector published the event.
	PipelineLatency prometheus.Histogram
	// FeedLatency: MLS modification timestamp -> event published. Includes
	// the poll interval, so it is the end-user view.
	FeedLatency prometheus.Histogram
}

// NewMetrics registers detector metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	ns, sub := "listing_alerts", "detect"
	m := &Metrics{
		Changes:   prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "changes_total", Help: "Changes consumed, by outcome."}, []string{"outcome"}),
		Events:    prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "events_total", Help: "Listing events published, by type."}, []string{"type"}),
		Conflicts: prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "state_conflicts_total", Help: "Batches recomputed after a concurrent state write."}),
		Invalid:   prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "invalid_records_total", Help: "Records that could not be decoded and were skipped."}),
		BatchTime: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "batch_seconds", Help: "Load-apply-publish-save time per batch.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 14)}),
		PipelineLatency: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "pipeline_latency_seconds",
			Help: "Poller observation to listing event published.", Buckets: prometheus.ExponentialBuckets(0.005, 2, 14)}),
		FeedLatency: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "feed_latency_seconds",
			Help: "MLS modification time to listing event published.", Buckets: prometheus.ExponentialBuckets(1, 2, 12)}),
	}
	reg.MustRegister(m.Changes, m.Events, m.Conflicts, m.Invalid, m.BatchTime, m.PipelineLatency, m.FeedLatency)
	return m
}

func (m *Metrics) observe(counts map[Outcome]int, events []event.ListingEvent, now time.Time) {
	for o, n := range counts {
		m.Changes.WithLabelValues(string(o)).Add(float64(n))
	}
	for _, e := range events {
		m.Events.WithLabelValues(string(e.Type)).Inc()
		m.PipelineLatency.Observe(now.Sub(e.ObservedAt).Seconds())
		m.FeedLatency.Observe(now.Sub(e.ModifiedAt).Seconds())
	}
}
