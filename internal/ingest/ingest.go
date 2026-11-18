// Package ingest turns listing feeds into events on listing.changes.
//
// The contract with the rest of the pipeline:
//
//   - Every event is published (acked by all in-sync replicas) before the
//     cursor and listing versions that produced it are committed. A crash in
//     between re-publishes the same events with the same deterministic IDs on
//     restart, so delivery is at-least-once and consumers dedupe by ID.
//   - Records whose alert-relevant content did not change (timestamp bumps,
//     photo re-uploads, re-reads of the overlap window) produce no event.
//   - A record older than the stored version of the same listing is stale and
//     is dropped, so re-reads can never move a listing backwards.
package ingest

import (
	"context"
	"time"

	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/listing"
	"github.com/mertmetin/listing-alerts/internal/reso"
)

// Version is what the store remembers about a listing per source.
type Version struct {
	Hash       string
	ModifiedAt time.Time
	Present    bool
}

// Snapshot is a version to persist, with the listing itself (nil for removals).
type Snapshot struct {
	Key string
	Version
	Listing *listing.Listing
}

// Store persists sync cursors and the last version of each listing.
type Store interface {
	Cursor(ctx context.Context, source string) (reso.Cursor, error) // zero Cursor when none
	Versions(ctx context.Context, source string, keys []string) (map[string]Version, error)
	PresentKeys(ctx context.Context, source string) (map[string]Version, error)
	// Commit atomically saves snapshots and, when cur is non-nil, the cursor.
	Commit(ctx context.Context, source string, cur *reso.Cursor, snaps []Snapshot) error
}

// Publisher writes changes to the log. It returns only after every change is
// durably acknowledged.
type Publisher interface {
	Publish(ctx context.Context, changes []event.Change) error
}

// Recorder keeps raw feed pages for replay. Errors are logged, not fatal:
// losing a recording must not stop alerts.
type Recorder interface {
	Record(source string, fetchedAt time.Time, rows []map[string]any) error
}

// NopRecorder records nothing.
type NopRecorder struct{}

func (NopRecorder) Record(string, time.Time, []map[string]any) error { return nil }

// Diff compares fresh listings with stored versions and returns the events to
// publish and the snapshots to commit. prev is updated in place, so a listing
// that appears twice in one batch is handled in order.
func Diff(source string, fresh []listing.Listing, prev map[string]Version, now time.Time, backfill bool) ([]event.Change, []Snapshot, Skips) {
	var (
		changes []event.Change
		snaps   []Snapshot
		skips   Skips
	)
	for i := range fresh {
		l := fresh[i]
		old, seen := prev[l.Key]
		hash := l.ContentHash()
		switch {
		case seen && l.ModifiedAt.Before(old.ModifiedAt):
			skips.Stale++
			continue
		case seen && old.Present && old.Hash == hash:
			skips.Unchanged++
			continue
		}
		v := Version{Hash: hash, ModifiedAt: l.ModifiedAt, Present: true}
		prev[l.Key] = v
		c := event.NewObserved(source, l, now, backfill)
		if seen && !old.Present {
			c = c.Reappeared()
		}
		changes = append(changes, c)
		snaps = append(snaps, Snapshot{Key: l.Key, Version: v, Listing: &l})
	}
	return changes, snaps, skips
}

// Removals returns Removed events and snapshots for present listings missing
// from keep.
func Removals(source string, present map[string]Version, keep map[string]bool, now time.Time) ([]event.Change, []Snapshot) {
	var (
		changes []event.Change
		snaps   []Snapshot
	)
	for key, v := range present {
		if keep[key] {
			continue
		}
		changes = append(changes, event.NewRemoved(source, key, v.Hash, v.ModifiedAt, now))
		v.Present = false
		snaps = append(snaps, Snapshot{Key: key, Version: v})
	}
	return changes, snaps
}

// Skips counts records that produced no event, by reason.
type Skips struct {
	Unchanged int
	Stale     int
	Invalid   int
}
