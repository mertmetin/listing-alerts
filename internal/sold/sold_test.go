package sold

import (
	"strings"
	"testing"
	"time"

	"github.com/mertmetin/listing-alerts/internal/geo"
	"github.com/mertmetin/listing-alerts/internal/geocode"
	"github.com/mertmetin/listing-alerts/internal/listing"
)

func TestScope(t *testing.T) {
	got := Scope("PropertyType eq 'Residential Freehold'", "CountyOrParish eq 'Ottawa'")
	want := "(PropertyType eq 'Residential Freehold') and (" + ClosedForSale + ") and (CountyOrParish eq 'Ottawa')"
	if got != want {
		t.Fatalf("Scope =\n%s\nwant\n%s", got, want)
	}
	if got := Scope("", ""); got != "("+ClosedForSale+")" {
		t.Fatalf("empty parts not dropped: %s", got)
	}
}

func TestSince(t *testing.T) {
	if got := Since(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)); got != "CloseDate ge 2021-01-01" {
		t.Fatal(got)
	}
}

func TestFieldsIncludeSaleAndHubFields(t *testing.T) {
	joined := "," + strings.Join(Fields, ",") + ","
	for _, f := range []string{"ClosePrice", "CloseDate", "PurchaseContractDate", "LivingAreaRange", "TaxAnnualAmount", "AssociationFee", "ApproximateAge"} {
		if !strings.Contains(joined, ","+f+",") {
			t.Errorf("Fields lacks %s", f)
		}
	}
}

type fakeGeo struct{ calls int }

func (g *fakeGeo) Locate(l listing.Listing) (*geocode.Fix, error) {
	g.calls++
	if l.StreetName == "Nowhere" {
		return nil, nil
	}
	return &geocode.Fix{Point: geo.Point{Lat: 45.4, Lng: -75.7}, Precision: geocode.Address}, nil
}

func TestPrepare(t *testing.T) {
	g := &fakeGeo{}
	a := &Archive{Geo: g}
	rows := a.Prepare([]map[string]any{
		{"ListingKey": "X1", "ModificationTimestamp": "2026-11-20T10:00:00Z", "MlsStatus": "Sold",
			"CountyOrParish": "Ottawa", "PropertySubType": "Semi-Detached ", "ClosePrice": 650000.0,
			"CloseDate": "2026-12-15", "PurchaseContractDate": "2026-11-19", "StreetNumber": "12", "StreetName": "Main"},
		{"ListingKey": "X2", "ModificationTimestamp": "2026-11-20T10:00:00Z", "StreetName": "Nowhere"},
		{"ListingKey": "X3", "ModificationTimestamp": "2026-11-20T10:00:00Z", "Latitude": 45.1, "Longitude": -75.9},
		{"ModificationTimestamp": "2026-11-20T10:00:00Z"}, // no key: skipped
	})
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
	r := rows[0]
	if r.SubType != "Semi-Detached" || *r.ClosePrice != 650000 || r.ContractDate.Format("2006-01-02") != "2026-11-19" ||
		r.CloseDate.Format("2006-01-02") != "2026-12-15" || r.Precision != "address" || *r.Lat != 45.4 {
		t.Fatalf("bad row: %+v", r)
	}
	if rows[1].Lat != nil || rows[1].Precision != "" {
		t.Fatalf("unlocated row has a point: %+v", rows[1])
	}
	if rows[2].Precision != "feed" || *rows[2].Lat != 45.1 {
		t.Fatalf("feed coordinates not kept: %+v", rows[2])
	}
	if g.calls != 2 {
		t.Fatalf("geocoder called %d times, want 2 (not for rows with feed coordinates)", g.calls)
	}
}

func TestDay(t *testing.T) {
	if d := day("2025-03-04T00:00:00Z"); d == nil || d.Format("2006-01-02") != "2025-03-04" {
		t.Fatal(d)
	}
	for _, v := range []any{nil, "", "garbage", 12} {
		if day(v) != nil {
			t.Fatalf("day(%v) not nil", v)
		}
	}
}
