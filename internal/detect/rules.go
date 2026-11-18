// Package detect turns per-source observations (listing.changes) into typed
// listing events (listing.events): new listing, price drop, sold, leased,
// back on market, and a few more the matcher may use.
//
// The rules are a pure function, Apply(state, change) -> (state, events),
// so they are tested without Kafka or Postgres, and running them on 1 or N
// workers gives the same result as long as each listing's changes are
// applied in order (they are: listing.changes is keyed by ListingKey).
package detect

import (
	"time"

	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/listing"
)

// SourceVersion is the last observation of a listing from one source.
type SourceVersion struct {
	Hash       string    `json:"hash"`
	ModifiedAt time.Time `json:"modified_at"`
	Present    bool      `json:"present"`
	// RemovedAt is when the source dropped the listing (poller clock). An
	// observation made before it is a replay and must not resurrect it.
	RemovedAt time.Time        `json:"removed_at,omitzero"`
	Listing   *listing.Listing `json:"listing,omitempty"`
}

// State is everything the detector remembers about one listing.
type State struct {
	Key string `json:"-"`
	// Version is for optimistic concurrency in the store: 0 = not stored yet.
	Version int64                    `json:"-"`
	Sources map[string]SourceVersion `json:"sources"`
	Phase   event.Phase              `json:"phase"`
	Public  bool                     `json:"public"`
	// PublicSeen: public at some point since the listing last came on the
	// market. A sale is announced to public users only then, even when IDX
	// dropped the listing before VOW reported the sale.
	PublicSeen      bool     `json:"public_seen"`
	Price           *float64 `json:"price,omitempty"`
	TransactionType string   `json:"transaction_type,omitempty"`
	// Geo caches the geocoding result for the listing's current address.
	Geo *Geo `json:"geo,omitempty"`
}

// Geo is a cached geocoding result. Precision "" records a miss, so a
// listing that cannot be placed is not looked up again until it moves.
type Geo struct {
	Addr      string  `json:"addr"`
	Lat       float64 `json:"lat,omitempty"`
	Lng       float64 `json:"lng,omitempty"`
	Precision string  `json:"precision,omitempty"`
}

// Outcome says what Apply did with a change.
type Outcome string

const (
	Applied   Outcome = "applied"   // state changed (events may or may not follow)
	Duplicate Outcome = "duplicate" // already applied: a redelivery or replay
	Stale     Outcome = "stale"     // older than what we hold for that source
	Backfill  Outcome = "backfill"  // applied silently (first-run baseline)
)

// PhaseOf classifies a listing's status.
func PhaseOf(l listing.Listing) event.Phase {
	switch l.StandardStatus {
	case "Active":
		return event.PhaseActive
	case "Active Under Contract", "Pending":
		return event.PhasePending
	case "Closed":
		if l.TransactionType == "For Sale" {
			return event.PhaseSold
		}
		return event.PhaseLeased
	default: // Expired, Cancelled, Withdrawn, Hold, Delete, ...
		return event.PhaseOff
	}
}

// sourcePriority breaks ties between sources with the same ModifiedAt: VOW
// carries the most (close prices), then IDX, then off-MLS.
var sourcePriority = map[string]int{event.SourceVOW: 3, event.SourceIDX: 2, event.SourceOffMLS: 1}

// Apply folds one change into s. It never mutates s; it returns the new state
// and the events to publish. now stamps DetectedAt only (not part of any ID).
func Apply(s State, c event.Change, now time.Time) (State, []event.ListingEvent, Outcome) {
	sv, seen := s.Sources[c.Source]
	switch c.Kind {
	case event.Observed:
		if seen && c.ModifiedAt.Before(sv.ModifiedAt) {
			return s, nil, Stale
		}
		if seen && sv.Hash == c.ContentHash && c.ModifiedAt.Equal(sv.ModifiedAt) &&
			(sv.Present || !c.ObservedAt.After(sv.RemovedAt)) {
			return s, nil, Duplicate
		}
		sv = SourceVersion{Hash: c.ContentHash, ModifiedAt: c.ModifiedAt, Present: true, Listing: c.Listing}
	case event.Removed:
		if !seen || !sv.Present {
			return s, nil, Duplicate
		}
		sv.Present = false
		sv.RemovedAt = c.ObservedAt
	default:
		return s, nil, Stale
	}

	next := s
	next.Sources = make(map[string]SourceVersion, len(s.Sources)+1)
	for k, v := range s.Sources {
		next.Sources[k] = v
	}
	next.Sources[c.Source] = sv

	cur, any := merged(next.Sources)
	next.Public = next.Sources[event.SourceIDX].Present || next.Sources[event.SourceOffMLS].Present
	if any {
		next.Phase = PhaseOf(cur)
	} else {
		next.Phase = event.PhaseOff // gone from every feed
	}
	if cur.TransactionType != "" {
		next.TransactionType = cur.TransactionType
	}
	onMarket := next.Phase == event.PhaseActive || next.Phase == event.PhasePending
	if onMarket {
		next.Price = cur.ListPrice
	}

	// A new market period resets PublicSeen; within one it only ever turns on.
	newPeriod := next.Phase == event.PhaseActive && s.Phase != event.PhaseActive && s.Phase != event.PhasePending
	if newPeriod {
		next.PublicSeen = next.Public
	} else {
		next.PublicSeen = s.PublicSeen || next.Public
	}

	if c.Backfill {
		return next, nil, Backfill
	}

	var types []event.Type
	switch prev, ph := s.Phase, next.Phase; {
	case prev == event.PhaseNone && ph == event.PhaseActive:
		types = append(types, event.NewListing)
	case prev != event.PhaseNone && prev != event.PhaseActive && ph == event.PhaseActive:
		types = append(types, event.BackOnMarket)
	case prev == event.PhaseActive && ph == event.PhasePending:
		types = append(types, event.Pending)
	case (prev == event.PhaseActive || prev == event.PhasePending) && ph == event.PhaseSold:
		types = append(types, event.Sold)
	case (prev == event.PhaseActive || prev == event.PhasePending) && ph == event.PhaseLeased:
		types = append(types, event.Leased)
	case (prev == event.PhaseActive || prev == event.PhasePending) && ph == event.PhaseOff:
		types = append(types, event.OffMarket)
	case prev == event.PhaseActive && ph == event.PhaseActive:
		if lower(next.Price, s.Price) && next.TransactionType == s.TransactionType {
			types = append(types, event.PriceDrop)
		}
		if next.Public && !s.Public && !s.PublicSeen {
			types = append(types, event.NowPublic)
		}
	}

	events := make([]event.ListingEvent, 0, len(types))
	for _, t := range types {
		e := event.ListingEvent{
			V: event.Version, ID: event.ListingEventID(c.ID, t), Type: t, ListingKey: c.ListingKey,
			ChangeID: c.ID, Source: c.Source, PrevPhase: s.Phase, Phase: next.Phase,
			Price: next.Price, Public: next.Public,
			ModifiedAt: c.ModifiedAt, ObservedAt: c.ObservedAt, DetectedAt: now.UTC(),
			Listing: cur,
		}
		switch t {
		case event.PriceDrop:
			e.PrevPrice = s.Price
		case event.Sold, event.Leased:
			e.Price = s.Price // last asking price
			e.ClosePrice = cur.ClosePrice
			e.Public = next.PublicSeen
		case event.OffMarket, event.Pending:
			e.Public = next.PublicSeen
		}
		events = append(events, e)
	}
	return next, events, Applied
}

// merged picks the listing content to act on: the present source with the
// newest ModifiedAt, VOW first on ties. When no source has it any more, the
// last known content is returned with any = false.
func merged(srcs map[string]SourceVersion) (l listing.Listing, any bool) {
	var best, last *SourceVersion
	var bestName, lastName string
	for name, v := range srcs {
		v := v
		if v.Listing == nil {
			continue
		}
		if last == nil || newer(v, name, *last, lastName) {
			last, lastName = &v, name
		}
		if v.Present && (best == nil || newer(v, name, *best, bestName)) {
			best, bestName = &v, name
		}
	}
	switch {
	case best != nil:
		return *best.Listing, true
	case last != nil:
		return *last.Listing, false
	}
	return listing.Listing{}, false
}

func newer(a SourceVersion, an string, b SourceVersion, bn string) bool {
	if !a.ModifiedAt.Equal(b.ModifiedAt) {
		return a.ModifiedAt.After(b.ModifiedAt)
	}
	return sourcePriority[an] > sourcePriority[bn]
}

func lower(now, before *float64) bool {
	return now != nil && before != nil && *now < *before
}
