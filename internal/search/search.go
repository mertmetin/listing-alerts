// Package search holds saved searches and the reverse index that matches a
// listing event against all of them at once.
package search

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/geo"
)

// Audience decides which events and fields a search may see.
type Audience string

const (
	// Public: signed-up users of a public (IDX) site. Only listings IDX may
	// show, and never sold or leased prices.
	Public Audience = "public"
	// VOW: registered, email-confirmed, logged-in users (VOW rules). All
	// listings, with sold and leased prices.
	VOW Audience = "vow"
)

// Search is one saved search.
type Search struct {
	ID       string   `json:"id"`
	Site     string   `json:"site"` // mertmetin, hotrentals
	UserID   string   `json:"user_id"`
	Email    string   `json:"email"`
	Name     string   `json:"name,omitempty"`
	Audience Audience `json:"audience"`

	Transaction string        `json:"transaction"`         // "sale" or "lease"
	Areas       []geo.Polygon `json:"areas,omitempty"`     // drawn on a map
	Cities      []string      `json:"cities,omitempty"`    // or picked by name
	MinPrice    float64       `json:"min_price,omitempty"` // 0 = no minimum
	MaxPrice    float64       `json:"max_price,omitempty"` // 0 = no maximum
	MinBeds     int           `json:"min_beds,omitempty"`  //
	MinBaths    int           `json:"min_baths,omitempty"` //
	Types       []string      `json:"types,omitempty"`     // PropertySubType, e.g. Detached; empty = any
	Events      []event.Type  `json:"events,omitempty"`    // empty = DefaultEvents
	UpdatedAt   time.Time     `json:"updated_at"`
	Deleted     bool          `json:"deleted,omitempty"`
}

// DefaultEvents are the alerts a search gets unless it picks others.
var DefaultEvents = []event.Type{event.NewListing, event.PriceDrop, event.Sold, event.Leased, event.BackOnMarket}

// Limits keep one search from costing the index too much.
const (
	MaxAreas    = 10
	MaxVertices = 200
	MaxCities   = 50
	MaxTypes    = 20
)

// Validate checks a search from the API and normalises it in place.
func (s *Search) Validate() error {
	var errs []error
	if s.UserID == "" || len(s.UserID) > 128 {
		errs = append(errs, errors.New("user_id is required (max 128 chars)"))
	}
	if !strings.Contains(s.Email, "@") || len(s.Email) > 254 {
		errs = append(errs, errors.New("email is invalid"))
	}
	if s.Audience != Public && s.Audience != VOW {
		errs = append(errs, errors.New(`audience must be "public" or "vow"`))
	}
	if s.Transaction != "sale" && s.Transaction != "lease" {
		errs = append(errs, errors.New(`transaction must be "sale" or "lease"`))
	}
	if len(s.Areas) == 0 && len(s.Cities) == 0 {
		errs = append(errs, errors.New("give at least one area or city"))
	}
	if len(s.Areas) > MaxAreas {
		errs = append(errs, fmt.Errorf("at most %d areas", MaxAreas))
	}
	for i, a := range s.Areas {
		if len(a) < 3 || len(a) > MaxVertices {
			errs = append(errs, fmt.Errorf("area %d: 3 to %d points", i+1, MaxVertices))
			continue
		}
		for _, p := range a {
			// Canada, roughly; catches swapped lat/lng.
			if p.Lat < 41 || p.Lat > 84 || p.Lng < -142 || p.Lng > -52 {
				errs = append(errs, fmt.Errorf("area %d: point %v is outside Canada (lat, lng swapped?)", i+1, p))
				break
			}
		}
	}
	if len(s.Cities) > MaxCities || len(s.Types) > MaxTypes {
		errs = append(errs, fmt.Errorf("at most %d cities and %d types", MaxCities, MaxTypes))
	}
	if s.MinPrice < 0 || s.MaxPrice < 0 || (s.MaxPrice > 0 && s.MaxPrice < s.MinPrice) {
		errs = append(errs, errors.New("price range is invalid"))
	}
	if s.MinBeds < 0 || s.MinBeds > 20 || s.MinBaths < 0 || s.MinBaths > 20 {
		errs = append(errs, errors.New("min_beds and min_baths must be 0 to 20"))
	}
	for _, e := range s.Events {
		if eventBit(e) == 0 {
			errs = append(errs, fmt.Errorf("unknown event type %q", e))
		}
	}
	for i := range s.Cities {
		s.Cities[i] = NormCity(s.Cities[i])
	}
	return errors.Join(errs...)
}

// NormCity makes city names comparable: "Ottawa ", "OTTAWA" -> "ottawa".
func NormCity(c string) string { return strings.ToLower(strings.Join(strings.Fields(c), " ")) }

func eventBit(t event.Type) uint16 {
	switch t {
	case event.NewListing:
		return 1 << 0
	case event.PriceDrop:
		return 1 << 1
	case event.Sold:
		return 1 << 2
	case event.Leased:
		return 1 << 3
	case event.BackOnMarket:
		return 1 << 4
	case event.Pending:
		return 1 << 5
	case event.OffMarket:
		return 1 << 6
	}
	return 0
}
