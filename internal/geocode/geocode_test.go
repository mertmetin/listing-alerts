package geocode

import (
	"os"
	"testing"

	"github.com/mertmetin/listing-alerts/internal/listing"
)

func TestStreetCore(t *testing.T) {
	cases := map[string]string{
		"St. Laurent Blvd": "st laurent",
		"ST LAURENT":       "st laurent",
		"Bay St Street":    "bay",
		"Montée Paiement":  "montee paiement",
		"Park":             "park", // a lone word is kept even if it is a street type
		"Main St W":        "main",
	}
	for in, want := range cases {
		if got := StreetCore(in); got != want {
			t.Errorf("StreetCore(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestLocateReal runs against the real address points when present
// (data/geo is gitignored; `make geodata` copies it from hotrentals).
func TestLocateReal(t *testing.T) {
	path := "../../data/geo/address-points.db"
	if _, err := os.Stat(path); err != nil {
		t.Skip("no address points database")
	}
	g, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	// 110 Laurier Ave W, Ottawa (City Hall).
	f, err := g.Locate(listing.Listing{StreetNumber: "110", StreetName: "Laurier", StreetSuffix: "Avenue", County: "Ottawa", PostalCode: "K1P 1J1"})
	if err != nil || f == nil || f.Precision != Address {
		t.Fatalf("city hall: %+v, %v", f, err)
	}
	if d := (f.Point.Lat-45.4207)*(f.Point.Lat-45.4207) + (f.Point.Lng+75.6905)*(f.Point.Lng+75.6905); d > 0.0001 {
		t.Errorf("city hall at %+v, want about 45.4207,-75.6905", f.Point)
	}
	// An address that does not exist falls back to its postal area, if known.
	f, _ = g.Locate(listing.Listing{StreetNumber: "99999", StreetName: "Nowhere", County: "Ottawa", PostalCode: "K1P 1J1"})
	if f != nil && f.Precision != FSA {
		t.Errorf("missing address: %+v", f)
	}
	t.Logf("%d postal areas with a fallback centre", g.FSAs())
}
