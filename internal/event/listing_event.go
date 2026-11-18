package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/mertmetin/listing-alerts/internal/listing"
)

// Type of a listing event: what happened, as an alert would describe it.
type Type string

const (
	NewListing   Type = "new_listing"    // first seen on the market
	PriceDrop    Type = "price_drop"     // asking price went down
	Sold         Type = "sold"           // closed sale (close price when VOW has it)
	Leased       Type = "leased"         // closed lease (close price when VOW has it)
	BackOnMarket Type = "back_on_market" // active again after being off the market or conditional
	Pending      Type = "pending"        // sold or leased conditionally
	OffMarket    Type = "off_market"     // expired, terminated, suspended or gone from every feed
	NowPublic    Type = "now_public"     // already on the market, newly allowed on public (IDX) sites
)

// Phase is where a listing is in its lifecycle.
type Phase string

const (
	PhaseNone    Phase = ""        // never seen on the market
	PhaseActive  Phase = "active"  // StandardStatus Active
	PhasePending Phase = "pending" // Active Under Contract (sold/leased conditional)
	PhaseSold    Phase = "sold"
	PhaseLeased  Phase = "leased"
	PhaseOff     Phase = "off" // expired, cancelled, withdrawn, removed
)

// ListingEvent is one message on listing.events, keyed by ListingKey.
type ListingEvent struct {
	V          int      `json:"v"`
	ID         string   `json:"id"`
	Type       Type     `json:"type"`
	ListingKey string   `json:"listing_key"`
	ChangeID   string   `json:"change_id"` // the listing.changes event that triggered it
	Source     string   `json:"source"`    // source of that change
	PrevPhase  Phase    `json:"prev_phase,omitempty"`
	Phase      Phase    `json:"phase"`
	PrevPrice  *float64 `json:"prev_price,omitempty"`
	Price      *float64 `json:"price,omitempty"`
	ClosePrice *float64 `json:"close_price,omitempty"` // VOW data: only for registered, logged-in users
	// Public: may be shown on public (IDX) sites. False means VOW users only.
	Public     bool            `json:"public"`
	ModifiedAt time.Time       `json:"modified_at"` // feed timestamp of the change
	ObservedAt time.Time       `json:"observed_at"` // poller saw it: start of pipeline latency
	DetectedAt time.Time       `json:"detected_at"`
	Listing    listing.Listing `json:"listing"`
}

// ListingEventID is deterministic in the triggering change, so reprocessing a
// change after a crash yields the same IDs and downstream dedup holds.
func ListingEventID(changeID string, t Type) string {
	sum := sha256.Sum256([]byte(changeID + "\x00" + string(t)))
	return hex.EncodeToString(sum[:16])
}

// Encode returns the wire form.
func (e ListingEvent) Encode() ([]byte, error) { return json.Marshal(e) }

// DecodeListingEvent parses the wire form.
func DecodeListingEvent(b []byte) (ListingEvent, error) {
	var e ListingEvent
	err := json.Unmarshal(b, &e)
	return e, err
}
