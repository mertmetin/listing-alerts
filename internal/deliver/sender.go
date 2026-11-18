package deliver

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mertmetin/listing-alerts/internal/event"
)

// Topics.
const (
	TopicAlerts = "alerts.pending"
	TopicDLQ    = "alerts.dlq"
	Group       = "delivery"
)

// DeadLetters records alerts that will never be delivered.
type DeadLetters interface {
	Publish(ctx context.Context, items []Item, cause string) error
}

// Sender delivers queued alerts.
type Sender struct {
	Store   Store
	Limiter Limiter
	Channel Channel
	Render  Renderer
	DLQ     DeadLetters
	Metrics *Metrics
	Log     *slog.Logger
	Now     func() time.Time

	// Per user: Burst emails right away, then one per Every. Alerts held
	// back arrive together in the next email (a digest).
	Burst int
	Every time.Duration
	// DailyLimit caps emails per channel per day (Gmail: 2,000 for Workspace).
	// 0 = no cap.
	DailyLimit  int
	MaxPerEmail int // listings in one email; the rest wait for the next
	MaxAttempts int
	Lease       time.Duration
	Batch       int
}

// backoff schedule for temporary failures.
var backoff = []time.Duration{time.Minute, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour, 6 * time.Hour}

// Backoff returns the wait before attempt n+1 (n >= 1), with ±10% jitter so
// a burst of failures does not retry in lockstep.
func Backoff(n int) time.Duration {
	d := backoff[min(max(n, 1), len(backoff))-1]
	return d + time.Duration((rand.Float64()*0.2-0.1)*float64(d))
}

// Step claims due alerts and sends them, one email per user. It returns how
// many emails it attempted.
func (s *Sender) Step(ctx context.Context) (int, error) {
	items, err := s.Store.Claim(ctx, s.Batch, s.Lease)
	if err != nil || len(items) == 0 {
		return 0, err
	}
	// Group by user, keeping claim order (oldest first).
	var order []string
	groups := map[string][]Item{}
	for _, it := range items {
		k := it.Alert.Key()
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], it)
	}
	sent := 0
	for _, k := range order {
		if err := s.sendUser(ctx, k, groups[k]); err != nil {
			// Bookkeeping failed (Postgres); the lease expires and the
			// reaper returns the items, so nothing is lost.
			s.Log.Error("delivery bookkeeping", "user", k, "err", err)
		}
		sent++
	}
	return sent, nil
}

func (s *Sender) sendUser(ctx context.Context, user string, items []Item) error {
	now := s.Now()
	if len(items) > s.MaxPerEmail {
		if err := s.Store.Defer(ctx, ids(items[s.MaxPerEmail:]), now.Add(s.Every)); err != nil {
			return err
		}
		items = items[:s.MaxPerEmail]
	}
	all := ids(items)

	ok, wait, err := s.Limiter.Take(ctx, "user:"+user, s.Burst, s.Every)
	if err != nil {
		// Limiter down: hold rather than risk flooding someone's inbox.
		s.Metrics.Deferred.WithLabelValues("limiter_error").Add(float64(len(items)))
		return s.Store.Defer(ctx, all, now.Add(30*time.Second))
	}
	if !ok {
		s.Metrics.Deferred.WithLabelValues("user_rate").Add(float64(len(items)))
		return s.Store.Defer(ctx, all, now.Add(wait))
	}
	if s.DailyLimit > 0 {
		ok, wait, err := s.Limiter.Take(ctx, "daily:"+s.Channel.Name(), s.DailyLimit, 24*time.Hour/time.Duration(s.DailyLimit))
		if err != nil || !ok {
			s.Metrics.Deferred.WithLabelValues("daily_cap").Add(float64(len(items)))
			if wait == 0 {
				wait = time.Minute
			}
			return s.Store.Defer(ctx, all, now.Add(wait))
		}
	}

	alerts := make([]event.Alert, len(items))
	attempts := 0
	for i, it := range items {
		alerts[i] = it.Alert
		attempts = max(attempts, it.Attempts)
	}
	msg, err := s.Render.Render(alerts)
	if err == nil {
		err = s.Channel.Send(ctx, msg)
	}
	if err != nil {
		// Nothing reached the user: give the token back.
		if rerr := s.Limiter.Refund(ctx, "user:"+user, s.Burst); rerr != nil {
			s.Log.Warn("rate-limit refund", "err", rerr)
		}
		if s.DailyLimit > 0 {
			_ = s.Limiter.Refund(ctx, "daily:"+s.Channel.Name(), s.DailyLimit)
		}
	}
	switch {
	case err == nil:
		kind := "single"
		if len(items) > 1 {
			kind = "digest"
		}
		s.Metrics.Emails.WithLabelValues(s.Channel.Name(), kind).Inc()
		done := s.Now()
		for _, a := range alerts {
			s.Metrics.Delivered.WithLabelValues(string(a.Type), a.Audience).Inc()
			s.Metrics.Latency.Observe(done.Sub(a.ObservedAt).Seconds())
		}
		return s.Store.MarkSent(ctx, all, msg.MessageID)
	case IsPermanent(err) || attempts+1 >= s.MaxAttempts:
		cause := err.Error()
		s.Metrics.Failed.WithLabelValues(map[bool]string{true: "permanent", false: "exhausted"}[IsPermanent(err)]).Add(float64(len(items)))
		s.Log.Warn("giving up on alerts", "user", user, "alerts", len(items), "attempts", attempts+1, "err", cause)
		if derr := s.DLQ.Publish(ctx, items, cause); derr != nil {
			// Keep them retryable rather than lose them.
			return errors.Join(derr, s.Store.Retry(ctx, all, now.Add(time.Minute), cause))
		}
		return s.Store.MarkFailed(ctx, all, cause)
	default:
		next := now.Add(Backoff(attempts + 1))
		s.Metrics.Retries.Add(float64(len(items)))
		s.Log.Info("send failed, will retry", "user", user, "attempt", attempts+1, "at", next.Format(time.RFC3339), "err", err)
		return s.Store.Retry(ctx, all, next, err.Error())
	}
}

func ids(items []Item) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.AlertID
	}
	return out
}

// Run sends until ctx is cancelled, polling every idle interval when there
// is nothing due.
func (s *Sender) Run(ctx context.Context, idle time.Duration) {
	for ctx.Err() == nil {
		n, err := s.Step(ctx)
		if err != nil && ctx.Err() == nil {
			s.Log.Error("claim", "err", err)
		}
		if n == 0 || err != nil {
			select {
			case <-ctx.Done():
			case <-time.After(idle):
			}
		}
	}
}

// Reaper returns expired leases to the queue every interval.
func Reaper(ctx context.Context, st Store, every time.Duration, m *Metrics, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if n, err := st.Reap(ctx); err != nil {
			log.Error("reap", "err", err)
		} else if n > 0 {
			m.Reaped.Add(float64(n))
			log.Warn("returned expired leases to the queue (possible resends)", "count", n)
		}
	}
}

// Intake consumes alerts.pending into the queue. Offsets are committed only
// after the batch is in Postgres, so an alert is never lost between the two.
func Intake(ctx context.Context, cl *kgo.Client, st Store, coalesce time.Duration, m *Metrics, log *slog.Logger) error {
	defer cl.AllowRebalance()
	for {
		fs := cl.PollRecords(ctx, 1000)
		if ctx.Err() != nil {
			return nil
		}
		fs.EachError(func(t string, p int32, err error) { log.Error("fetch", "topic", t, "partition", p, "err", err) })
		recs := fs.Records()
		alerts := make([]event.Alert, 0, len(recs))
		for _, r := range recs {
			a, err := event.DecodeAlert(r.Value)
			if err != nil || a.ID == "" || a.Email == "" {
				log.Error("skipping undecodable alert", "offset", r.Offset, "err", err)
				continue
			}
			alerts = append(alerts, a)
		}
		for {
			// Coalesce: hold each alert a few seconds so one listing that
			// matches several of a user's searches becomes one email.
			n, err := st.Enqueue(ctx, alerts, time.Now().Add(coalesce))
			if err == nil {
				m.Enqueued.WithLabelValues("new").Add(float64(n))
				m.Enqueued.WithLabelValues("duplicate").Add(float64(len(alerts) - n))
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			log.Error("enqueue, retrying", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
		}
		if len(recs) > 0 {
			if err := cl.CommitRecords(ctx, recs...); err != nil && ctx.Err() == nil {
				log.Error("commit offsets", "err", err)
			}
		}
		cl.AllowRebalance()
	}
}

// KafkaDLQ publishes dead letters to alerts.dlq (30-day retention).
type KafkaDLQ struct {
	Client *kgo.Client
	Topic  string
}

type deadLetter struct {
	Alert    event.Alert `json:"alert"`
	Cause    string      `json:"cause"`
	Attempts int         `json:"attempts"`
	FailedAt time.Time   `json:"failed_at"`
}

func (d KafkaDLQ) Publish(ctx context.Context, items []Item, cause string) error {
	recs := make([]*kgo.Record, 0, len(items))
	for _, it := range items {
		b, err := json.Marshal(deadLetter{Alert: it.Alert, Cause: cause, Attempts: it.Attempts + 1, FailedAt: time.Now().UTC()})
		if err != nil {
			return err
		}
		recs = append(recs, &kgo.Record{Topic: d.Topic, Key: []byte(it.Alert.Key()), Value: b})
	}
	return d.Client.ProduceSync(ctx, recs...).FirstErr()
}

// Metrics for delivery.
type Metrics struct {
	Enqueued  *prometheus.CounterVec // alerts into the queue: new / duplicate
	Emails    *prometheus.CounterVec // messages sent, by channel and single/digest
	Delivered *prometheus.CounterVec // alerts delivered, by type and audience
	Deferred  *prometheus.CounterVec // alerts held back, by reason
	Failed    *prometheus.CounterVec // alerts dead-lettered: permanent / exhausted
	Retries   prometheus.Counter
	Reaped    prometheus.Counter
	Latency   prometheus.Histogram // poller observed the change -> email accepted by the server
	Queue     *prometheus.GaugeVec // deliveries by status
}

// NewMetrics registers delivery metrics.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	ns, sub := "listing_alerts", "deliver"
	m := &Metrics{
		Enqueued:  prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "enqueued_total", Help: "Alerts taken into the delivery queue."}, []string{"result"}),
		Emails:    prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "messages_total", Help: "Messages sent."}, []string{"channel", "kind"}),
		Delivered: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "alerts_delivered_total", Help: "Alerts delivered."}, []string{"type", "audience"}),
		Deferred:  prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "deferred_total", Help: "Alerts held back for later."}, []string{"reason"}),
		Failed:    prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "dead_lettered_total", Help: "Alerts sent to alerts.dlq."}, []string{"reason"}),
		Retries:   prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "retries_total", Help: "Alerts scheduled for another attempt."}),
		Reaped:    prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "reaped_total", Help: "Expired leases returned to the queue."}),
		Latency: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "pipeline_latency_seconds",
			Help: "Poller observed the change -> message accepted by the mail server.", Buckets: prometheus.ExponentialBuckets(0.05, 2, 16)}),
		Queue: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "queue", Help: "Deliveries by status (sent: last 24 h)."}, []string{"status"}),
	}
	reg.MustRegister(m.Enqueued, m.Emails, m.Delivered, m.Deferred, m.Failed, m.Retries, m.Reaped, m.Latency, m.Queue)
	return m
}
