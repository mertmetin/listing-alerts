package sold

import (
	"testing"
	"time"
)

func TestOffMarketDate(t *testing.T) {
	mod := time.Date(2026, 11, 20, 15, 4, 5, 0, time.UTC)
	cases := []struct {
		name string
		rec  map[string]any
		want string
	}{
		{"expired: its expiry date", map[string]any{"StandardStatus": "Expired", "ExpirationDate": "2026-10-31", "ListingContractDate": "2026-05-01", "DaysOnMarket": 150.0}, "2026-10-31"},
		{"terminated: list date + DOM", map[string]any{"StandardStatus": "Cancelled", "ListingContractDate": "2026-05-01", "DaysOnMarket": 30.0}, "2026-05-31"},
		{"no DOM: day of last change", map[string]any{"StandardStatus": "Cancelled", "ListingContractDate": "2026-05-01"}, "2026-11-20"},
		{"expired without expiry: list date + DOM", map[string]any{"StandardStatus": "Expired", "ListingContractDate": "2026-05-01", "DaysOnMarket": 10.0}, "2026-05-11"},
	}
	for _, c := range cases {
		got := OffMarketDate(c.rec, mod)
		if got == nil || got.Format("2006-01-02") != c.want {
			t.Errorf("%s: got %v, want %s", c.name, got, c.want)
		}
	}
	if OffMarketScope("PropertyType eq 'X'", "CountyOrParish eq 'Ottawa'") !=
		"(PropertyType eq 'X') and ("+EndedUnsold+") and (CountyOrParish eq 'Ottawa')" {
		t.Error("scope")
	}
}
