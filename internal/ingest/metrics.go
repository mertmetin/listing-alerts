package ingest

import "github.com/prometheus/client_golang/prometheus"

// Metrics for pollers. Labels: source (vow, idx, offmls).
type Metrics struct {
	Fetched   *prometheus.CounterVec   // records read from the feed
	Published *prometheus.CounterVec   // events published, by kind
	Skipped   *prometheus.CounterVec   // records with no event, by reason
	Errors    *prometheus.CounterVec   // failed steps
	Lag       *prometheus.GaugeVec     // now - cursor timestamp
	StepTime  *prometheus.HistogramVec // one fetch-diff-publish-commit step
	Backfill  *prometheus.GaugeVec     // 1 while the baseline runs
}

// NewMetrics registers poller metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	ns, sub := "listing_alerts", "ingest"
	m := &Metrics{
		Fetched:   prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "records_fetched_total", Help: "Feed records read."}, []string{"source"}),
		Published: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "events_published_total", Help: "Change events published to listing.changes."}, []string{"source", "kind"}),
		Skipped:   prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "records_skipped_total", Help: "Records that produced no event."}, []string{"source", "reason"}),
		Errors:    prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "step_errors_total", Help: "Poll steps that failed and will be retried."}, []string{"source"}),
		Lag:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "cursor_lag_seconds", Help: "How far the incremental cursor trails the wall clock."}, []string{"source"}),
		StepTime:  prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "step_seconds", Help: "Duration of one poll step.", Buckets: prometheus.ExponentialBuckets(0.01, 2, 14)}, []string{"source"}),
		Backfill:  prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "baseline_running", Help: "1 while the first-run baseline is in progress."}, []string{"source"}),
	}
	reg.MustRegister(m.Fetched, m.Published, m.Skipped, m.Errors, m.Lag, m.StepTime, m.Backfill)
	return m
}

func (m *Metrics) count(source string, changes int, removed int, s Skips) {
	if changes > 0 {
		m.Published.WithLabelValues(source, "observed").Add(float64(changes))
	}
	if removed > 0 {
		m.Published.WithLabelValues(source, "removed").Add(float64(removed))
	}
	m.Skipped.WithLabelValues(source, "unchanged").Add(float64(s.Unchanged))
	m.Skipped.WithLabelValues(source, "stale").Add(float64(s.Stale))
	m.Skipped.WithLabelValues(source, "invalid").Add(float64(s.Invalid))
}
