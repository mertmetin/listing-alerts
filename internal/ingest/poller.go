package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/mertmetin/listing-alerts/internal/listing"
	"github.com/mertmetin/listing-alerts/internal/reso"
)

// FeedSource is what a Poller needs from a RESO feed (reso.Source in production).
type FeedSource interface {
	Fetch(ctx context.Context, cur reso.Cursor, now time.Time) (reso.Page, error)
	Recent(ctx context.Context, since time.Time) ([]map[string]any, error)
	Keys(ctx context.Context, filter string) ([]string, error)
}

// Poller follows one RESO feed (VOW or IDX) into listing.changes.
type Poller struct {
	Source   string // event.SourceVOW or event.SourceIDX
	Feed     FeedSource
	Store    Store
	Pub      Publisher
	Rec      Recorder
	Log      *slog.Logger
	Metrics  *Metrics
	Now      func() time.Time
	Interval time.Duration // wait between polls once caught up
	// Lookback is re-read each caught-up poll to catch late-visible records.
	Lookback time.Duration
	// ReconcileEvery lists every key in the feed and emits Removed for
	// listings that disappeared. 0 disables (the VOW feed keeps closed
	// listings, so it does not need it; IDX drops them).
	ReconcileEvery time.Duration
	// MinKeepFraction: reconcile refuses to run if the feed lists fewer than
	// this fraction of the listings we hold, so a broken feed cannot "remove"
	// everything.
	MinKeepFraction float64

	lastReconcile time.Time
}

// Step runs one fetch-diff-publish-commit cycle. more reports whether the
// next step should run immediately.
func (p *Poller) Step(ctx context.Context) (more bool, err error) {
	start := p.Now()
	defer func() { p.Metrics.StepTime.WithLabelValues(p.Source).Observe(time.Since(start).Seconds()) }()

	cur, err := p.Store.Cursor(ctx, p.Source)
	if err != nil {
		return false, fmt.Errorf("load cursor: %w", err)
	}
	page, err := p.Feed.Fetch(ctx, cur, start)
	if err != nil {
		return false, fmt.Errorf("fetch: %w", err)
	}
	rows := page.Records
	caughtUp := !page.More && !page.Backfill && cur.Phase == reso.Incremental
	if caughtUp && p.Lookback > 0 {
		recent, err := p.Feed.Recent(ctx, start.Add(-p.Lookback))
		if err != nil {
			return false, fmt.Errorf("lookback: %w", err)
		}
		rows = append(rows, recent...)
	}
	p.Metrics.Fetched.WithLabelValues(p.Source).Add(float64(len(rows)))
	if err := p.Rec.Record(p.Source, start, rows); err != nil {
		p.Log.Warn("recording failed", "err", err)
	}

	fresh, invalid := p.parse(rows)
	keys := make([]string, len(fresh))
	for i, l := range fresh {
		keys[i] = l.Key
	}
	prev, err := p.Store.Versions(ctx, p.Source, keys)
	if err != nil {
		return false, fmt.Errorf("load versions: %w", err)
	}
	changes, snaps, skips := Diff(p.Source, fresh, prev, start, page.Backfill)
	skips.Invalid = invalid

	if err := p.Pub.Publish(ctx, changes); err != nil {
		return false, fmt.Errorf("publish: %w", err)
	}
	if err := p.Store.Commit(ctx, p.Source, &page.Next, snaps); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}

	p.Metrics.count(p.Source, len(changes), 0, skips)
	p.Metrics.Backfill.WithLabelValues(p.Source).Set(b2f(page.Next.Phase == reso.Baseline))
	if page.Next.Phase == reso.Incremental && !page.Next.TS.IsZero() {
		p.Metrics.Lag.WithLabelValues(p.Source).Set(max(start.Sub(page.Next.TS).Seconds(), 0))
	}
	if len(changes) > 0 || page.Backfill {
		p.Log.Info("step", "phase", page.Next.Phase, "read", len(rows), "published", len(changes),
			"unchanged", skips.Unchanged, "stale", skips.Stale, "invalid", skips.Invalid, "backfill", page.Backfill)
	}
	return page.More || page.Next.Phase == reso.Baseline, nil
}

func (p *Poller) parse(rows []map[string]any) ([]listing.Listing, int) {
	out := make([]listing.Listing, 0, len(rows))
	invalid := 0
	for _, r := range rows {
		l, err := listing.FromRESO(r)
		if err != nil {
			invalid++
			p.Log.Warn("skipping record", "err", err)
			continue
		}
		out = append(out, l)
	}
	return out, invalid
}

// Reconcile emits Removed for listings we hold that the feed no longer has.
func (p *Poller) Reconcile(ctx context.Context) (int, error) {
	now := p.Now()
	keys, err := p.Feed.Keys(ctx, "")
	if err != nil {
		return 0, fmt.Errorf("list keys: %w", err)
	}
	present, err := p.Store.PresentKeys(ctx, p.Source)
	if err != nil {
		return 0, err
	}
	if n := len(present); n > 0 && float64(len(keys)) < p.MinKeepFraction*float64(n) {
		return 0, fmt.Errorf("reconcile refused: feed lists %d keys but we hold %d present listings", len(keys), n)
	}
	keep := make(map[string]bool, len(keys))
	for _, k := range keys {
		keep[k] = true
	}
	changes, snaps := Removals(p.Source, present, keep, now)
	if err := p.Pub.Publish(ctx, changes); err != nil {
		return 0, fmt.Errorf("publish removals: %w", err)
	}
	if err := p.Store.Commit(ctx, p.Source, nil, snaps); err != nil {
		return 0, fmt.Errorf("commit removals: %w", err)
	}
	p.Metrics.count(p.Source, 0, len(changes), Skips{})
	p.Log.Info("reconciled", "feed_keys", len(keys), "held", len(present), "removed", len(changes))
	return len(changes), nil
}

// Run polls until ctx is cancelled. Failed steps back off exponentially up
// to the poll interval and retry from the last committed cursor.
func (p *Poller) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		more, err := p.Step(ctx)
		if err == nil && !more && p.ReconcileEvery > 0 && p.Now().Sub(p.lastReconcile) >= p.ReconcileEvery {
			// Runs between steps, never alongside one, so a listing cannot be
			// observed and removed at the same time.
			if _, rerr := p.Reconcile(ctx); rerr != nil {
				err = rerr
			} else {
				p.lastReconcile = p.Now()
			}
		}
		wait := time.Duration(0)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			p.Metrics.Errors.WithLabelValues(p.Source).Inc()
			p.Log.Error("poll step failed", "err", err, "retry_in", backoff)
			wait, backoff = backoff, min(backoff*2, max(p.Interval, time.Second))
		case more:
			backoff = time.Second
		default:
			backoff = time.Second
			wait = p.Interval
		}
		if wait > 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(wait):
			}
		}
	}
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
