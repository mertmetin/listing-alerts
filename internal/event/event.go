// Package event defines the messages on the listing.changes topic.
//
// A Change says "source S now shows listing K like this" (or "no longer shows
// it"). It does not say what changed: the change detector (M2) compares it
// with what it saw before and emits typed events (new, price drop, sold, ...).
// Keeping the poller dumb keeps it restartable and cheap to scale.
package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/mertmetin/listing-alerts/internal/listing"
)

// Kind of observation.
type Kind string

const (
	// Observed: the source shows the listing with this content.
	Observed Kind = "observed"
	// Removed: the source no longer has the listing (it fell out of the feed
	// with no status change, e.g. IDX drops sold homes or a brokerage opts out).
	Removed Kind = "removed"
)

// Source names.
const (
	SourceVOW    = "vow"    // PropTx VOW: full lifecycle, sold and leased prices
	SourceIDX    = "idx"    // PropTx IDX: what may be shown publicly
	SourceOffMLS = "offmls" // hotrentals private and landlord-submitted rentals
)

// Version is the envelope schema version, bumped on incompatible changes.
const Version = 1

// Change is one message on listing.changes, keyed by ListingKey so every
// observation of a listing, from every source, lands on one partition in order.
type Change struct {
	V           int              `json:"v"`
	ID          string           `json:"id"`
	Kind        Kind             `json:"kind"`
	Source      string           `json:"source"`
	ListingKey  string           `json:"listing_key"`
	ContentHash string           `json:"content_hash"`
	ModifiedAt  time.Time        `json:"modified_at"` // feed timestamp of the content
	ObservedAt  time.Time        `json:"observed_at"` // when the poller saw it: latency starts here
	Backfill    bool             `json:"backfill,omitempty"`
	Listing     *listing.Listing `json:"listing,omitempty"`
}

// ID is deterministic: the same observation re-published after a crash or
// retry gets the same ID, so consumers can drop duplicates. ModifiedAt is part
// of it so a listing that changes A -> B -> A still yields three IDs.
func ID(kind Kind, source, key, contentHash string, modifiedAt time.Time) string {
	h := sha256.New()
	for _, s := range []string{string(kind), source, key, contentHash, modifiedAt.UTC().Format(time.RFC3339Nano)} {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// NewObserved builds an Observed change for l.
func NewObserved(source string, l listing.Listing, observedAt time.Time, backfill bool) Change {
	hash := l.ContentHash()
	return Change{
		V: Version, Kind: Observed, Source: source, ListingKey: l.Key,
		ID:          ID(Observed, source, l.Key, hash, l.ModifiedAt),
		ContentHash: hash, ModifiedAt: l.ModifiedAt, ObservedAt: observedAt.UTC(),
		Backfill: backfill, Listing: &l,
	}
}

// NewRemoved builds a Removed change for a listing last seen with lastHash at lastModified.
func NewRemoved(source, key, lastHash string, lastModified, observedAt time.Time) Change {
	return Change{
		V: Version, Kind: Removed, Source: source, ListingKey: key,
		ID:          ID(Removed, source, key, lastHash, lastModified),
		ContentHash: lastHash, ModifiedAt: lastModified.UTC(), ObservedAt: observedAt.UTC(),
	}
}

// Encode returns the wire form (JSON; see docs/design/ingestion.md for why).
func (c Change) Encode() ([]byte, error) { return json.Marshal(c) }

// Decode parses the wire form.
func Decode(b []byte) (Change, error) {
	var c Change
	err := json.Unmarshal(b, &c)
	return c, err
}
