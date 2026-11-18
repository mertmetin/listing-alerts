package detect

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mertmetin/listing-alerts/internal/event"
)

// ErrConflict means another writer updated a listing since it was loaded.
var ErrConflict = errors.New("detect: listing state changed concurrently")

// Store holds listing states.
type Store interface {
	// Load returns the stored states for keys; missing keys are absent.
	Load(ctx context.Context, keys []string) (map[string]State, error)
	// Save writes states atomically, or returns ErrConflict and writes
	// nothing if any state's Version is no longer current.
	Save(ctx context.Context, states []State) error
}

// Publisher writes listing events and returns once they are acknowledged.
type Publisher interface {
	Publish(ctx context.Context, events []event.ListingEvent) error
}

// Detector applies batches of changes.
type Detector struct {
	Store   Store
	Pub     Publisher
	Metrics *Metrics
	Log     *slog.Logger
	Now     func() time.Time
}

// Process applies a batch (in topic order) and publishes the events it
// produces before saving the new states.
//
// Publish-then-save: a crash after publishing reprocesses the batch from
// the old state and re-publishes the same events with the same IDs (they
// derive from the change IDs), which downstream dedup drops. Save-then-
// publish would instead lose events on a crash.
//
// On ErrConflict (a rebalance let another worker write first) the batch is
// recomputed from fresh state; changes that worker already applied become
// duplicates and produce nothing.
func (d *Detector) Process(ctx context.Context, batch []event.Change) error {
	if len(batch) == 0 {
		return nil
	}
	var err error
	for attempt := 1; attempt <= 5; attempt++ {
		if err = d.once(ctx, batch); !errors.Is(err, ErrConflict) {
			return err
		}
		d.Metrics.Conflicts.Inc()
		d.Log.Warn("state conflict, recomputing batch", "attempt", attempt, "size", len(batch))
	}
	return err
}

func (d *Detector) once(ctx context.Context, batch []event.Change) error {
	start := d.Now()
	keys := make([]string, 0, len(batch))
	seen := map[string]bool{}
	for _, c := range batch {
		if !seen[c.ListingKey] {
			seen[c.ListingKey] = true
			keys = append(keys, c.ListingKey)
		}
	}
	states, err := d.Store.Load(ctx, keys)
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}

	var (
		events  []event.ListingEvent
		touched = map[string]bool{}
		counts  = map[Outcome]int{}
	)
	for _, c := range batch {
		s, ok := states[c.ListingKey]
		if !ok {
			s = State{Key: c.ListingKey}
		}
		next, evs, outcome := Apply(s, c, start)
		counts[outcome]++
		if outcome == Applied || outcome == Backfill {
			states[c.ListingKey] = next
			touched[c.ListingKey] = true
		}
		events = append(events, evs...)
	}

	if err := d.Pub.Publish(ctx, events); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	save := make([]State, 0, len(touched))
	for _, k := range keys {
		if touched[k] {
			save = append(save, states[k])
		}
	}
	if err := d.Store.Save(ctx, save); err != nil {
		if errors.Is(err, ErrConflict) {
			return err
		}
		return fmt.Errorf("save state: %w", err)
	}

	d.Metrics.observe(counts, events, d.Now())
	d.Metrics.BatchTime.Observe(time.Since(start).Seconds())
	if len(events) > 0 {
		d.Log.Info("batch", "changes", len(batch), "events", len(events),
			"applied", counts[Applied], "backfill", counts[Backfill], "duplicate", counts[Duplicate], "stale", counts[Stale])
	}
	return nil
}
