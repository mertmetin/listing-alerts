package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/mertmetin/listing-alerts/internal/listing"
)

// Alert is one message on alerts.pending: a listing event that matched a
// saved search, ready for delivery. Keyed by "site:user_id" so one user's
// alerts are ordered and land on one delivery worker (per-user rate limits).
type Alert struct {
	V          int    `json:"v"`
	ID         string `json:"id"`
	Site       string `json:"site"`
	UserID     string `json:"user_id"`
	Email      string `json:"email"`
	Audience   string `json:"audience"` // public or vow
	SearchID   string `json:"search_id"`
	SearchName string `json:"search_name,omitempty"`

	EventID    string `json:"event_id"`
	Type       Type   `json:"type"` // as the user sees it (now_public is a new listing to public users)
	ListingKey string `json:"listing_key"`

	Price      *float64 `json:"price,omitempty"`
	PrevPrice  *float64 `json:"prev_price,omitempty"`
	ClosePrice *float64 `json:"close_price,omitempty"` // only ever set for the vow audience

	GeoPrecision string          `json:"geo_precision,omitempty"`
	Listing      listing.Listing `json:"listing"`

	ObservedAt time.Time `json:"observed_at"` // poller saw the change
	MatchedAt  time.Time `json:"matched_at"`
}

// AlertID is deterministic in (event, search): redelivered events produce
// the same alert IDs, so delivery sends each at most once per search.
func AlertID(eventID, searchID string) string {
	sum := sha256.Sum256([]byte(eventID + "\x00" + searchID))
	return hex.EncodeToString(sum[:16])
}

// Key is the partition key for alerts.pending.
func (a Alert) Key() string { return a.Site + ":" + a.UserID }

// Encode returns the wire form.
func (a Alert) Encode() ([]byte, error) { return json.Marshal(a) }

// DecodeAlert parses the wire form.
func DecodeAlert(b []byte) (Alert, error) {
	var a Alert
	err := json.Unmarshal(b, &a)
	return a, err
}
