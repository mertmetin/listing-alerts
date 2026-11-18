package reso

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Phase of a sync cursor.
type Phase string

const (
	// Baseline: first run. Pages through every listing in BaselineFilter by
	// ListingKey to learn the current state. Events are marked backfill, so the
	// pipeline records them without alerting.
	Baseline Phase = "baseline"
	// Incremental: follows ModificationTimestamp from where the last page ended.
	Incremental Phase = "incremental"
)

// Cursor is a resumable sync position. In the incremental phase (TS, Key) is
// the last record read: the next page starts strictly after it.
type Cursor struct {
	Phase             Phase     `json:"phase"`
	TS                time.Time `json:"ts"`
	Key               string    `json:"key"`
	BaselineStartedAt time.Time `json:"baseline_started_at"`
}

// Page is one fetch.
type Page struct {
	Records  []map[string]any
	Next     Cursor
	Backfill bool
	// More is true when the page was full, so the caller should fetch again
	// right away instead of waiting for the poll interval.
	More bool
}

// Source pages through a Property feed with an exact keyset cursor.
type Source struct {
	Client *Client
	// Scope limits the feed, e.g. "(PropertyType eq 'Residential Freehold' or ...)".
	Scope string
	// BaselineFilter selects the listings whose current state matters on day
	// one (active and under contract). Closed history is not needed.
	BaselineFilter string
	Select         []string
	PageSize       int
	// Overlap re-reads this much before the baseline start, so changes made
	// while the baseline ran are not missed.
	Overlap time.Duration
}

// Fetch returns the page after cur. now is the wall clock (injected for tests).
func (s *Source) Fetch(ctx context.Context, cur Cursor, now time.Time) (Page, error) {
	if cur.Phase == "" {
		cur = Cursor{Phase: Baseline, BaselineStartedAt: now.UTC()}
	}
	switch cur.Phase {
	case Baseline:
		return s.baseline(ctx, cur)
	case Incremental:
		return s.incremental(ctx, cur)
	}
	return Page{}, fmt.Errorf("reso: unknown cursor phase %q", cur.Phase)
}

func (s *Source) baseline(ctx context.Context, cur Cursor) (Page, error) {
	conds := []string{s.Scope, s.BaselineFilter}
	if cur.Key != "" {
		conds = append(conds, "ListingKey gt "+Quote(cur.Key))
	}
	rows, err := s.Client.Get(ctx, "Property", Query{Filter: and(conds...), Select: s.Select, OrderBy: "ListingKey", Top: s.PageSize})
	if err != nil {
		return Page{}, err
	}
	p := Page{Records: rows, Backfill: true, More: true, Next: cur}
	if len(rows) > 0 {
		p.Next.Key, _ = rows[len(rows)-1]["ListingKey"].(string)
	}
	if len(rows) < s.PageSize {
		// Baseline complete: switch to following changes from just before it began.
		p.Next = Cursor{Phase: Incremental, TS: cur.BaselineStartedAt.Add(-s.Overlap), BaselineStartedAt: cur.BaselineStartedAt}
	}
	return p, nil
}

func (s *Source) incremental(ctx context.Context, cur Cursor) (Page, error) {
	ts := Timestamp(cur.TS)
	after := "ModificationTimestamp gt " + ts
	if cur.Key != "" {
		after = "(" + after + " or (ModificationTimestamp eq " + ts + " and ListingKey gt " + Quote(cur.Key) + "))"
	} else {
		after = "ModificationTimestamp ge " + ts
	}
	rows, err := s.Client.Get(ctx, "Property", Query{
		Filter: and(s.Scope, after), Select: s.Select, OrderBy: "ModificationTimestamp,ListingKey", Top: s.PageSize,
	})
	if err != nil {
		return Page{}, err
	}
	p := Page{Records: rows, Next: cur, More: len(rows) == s.PageSize}
	if n := len(rows); n > 0 {
		last := rows[n-1]
		t, err := time.Parse(time.RFC3339Nano, fmt.Sprint(last["ModificationTimestamp"]))
		if err != nil {
			return Page{}, fmt.Errorf("reso: last record has bad ModificationTimestamp: %w", err)
		}
		key, _ := last["ListingKey"].(string)
		p.Next = Cursor{Phase: Incremental, TS: t.UTC(), Key: key, BaselineStartedAt: cur.BaselineStartedAt}
	}
	return p, nil
}

// Recent returns records modified at or after since. Pollers call it when
// caught up, to pick up records that became visible after the cursor passed
// their timestamp (replication lag on the MLS side). Store-side dedup makes
// the re-read free of side effects.
func (s *Source) Recent(ctx context.Context, since time.Time) ([]map[string]any, error) {
	return s.Client.Get(ctx, "Property", Query{
		Filter: and(s.Scope, "ModificationTimestamp ge "+Timestamp(since)),
		Select: s.Select, OrderBy: "ModificationTimestamp,ListingKey", Top: s.PageSize,
	})
}

// Keys lists every ListingKey in scope and filter, for reconciliation.
func (s *Source) Keys(ctx context.Context, filter string) ([]string, error) {
	var keys []string
	last := ""
	for {
		conds := []string{s.Scope, filter}
		if last != "" {
			conds = append(conds, "ListingKey gt "+Quote(last))
		}
		rows, err := s.Client.Get(ctx, "Property", Query{Filter: and(conds...), Select: []string{"ListingKey"}, OrderBy: "ListingKey", Top: 5000})
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			if k, _ := r["ListingKey"].(string); k != "" {
				keys = append(keys, k)
				last = k
			}
		}
		if len(rows) < 5000 {
			return keys, nil
		}
	}
}

// and joins non-empty conditions, each parenthesised. Call it once per
// filter: nesting it would double-wrap.
func and(parts ...string) string {
	var out []string
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, "("+p+")")
		}
	}
	return strings.Join(out, " and ")
}
