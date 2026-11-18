package listing

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

const sold = `{
  "ListingKey": "X12345678", "TransactionType": "For Sale", "StandardStatus": "Closed", "MlsStatus": "Sold",
  "PropertyType": "Residential Freehold", "PropertySubType": "Detached",
  "ListPrice": 799900.0, "ClosePrice": 812000.0, "CloseDate": "2026-11-14",
  "BedroomsTotal": 3, "BathroomsTotalInteger": 2, "LivingAreaRange": "1500-2000",
  "City": "Ottawa", "CityRegion": "2005 - Convent Glen North", "CountyOrParish": "Ottawa", "StateOrProvince": "ON",
  "PostalCode": "K1C 1A1", "InternetAddressDisplayYN": true, "InternetEntireListingDisplayYN": true,
  "Latitude": null, "ModificationTimestamp": "2026-11-17T05:15:29Z", "PhotosChangeTimestamp": "2026-11-01T10:00:00Z"
}`

func decode(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestFromRESO(t *testing.T) {
	l, err := FromRESO(decode(t, sold))
	if err != nil {
		t.Fatal(err)
	}
	if l.Key != "X12345678" || l.MlsStatus != "Sold" || *l.ClosePrice != 812000 || *l.CloseDate != "2026-11-14" {
		t.Errorf("mapped %+v", l)
	}
	if *l.Bedrooms != 3 || l.Latitude != nil || !l.ListingDisplay {
		t.Errorf("beds/lat/display wrong: %+v", l)
	}
	if want := time.Date(2026, 11, 17, 5, 15, 29, 0, time.UTC); !l.ModifiedAt.Equal(want) {
		t.Errorf("ModifiedAt = %v", l.ModifiedAt)
	}
}

func TestFromRESORejectsUnidentifiable(t *testing.T) {
	if _, err := FromRESO(map[string]any{"ModificationTimestamp": "2026-11-17T05:15:29Z"}); err == nil {
		t.Error("want error for missing ListingKey")
	}
	if _, err := FromRESO(map[string]any{"ListingKey": "X1"}); err == nil || !strings.Contains(err.Error(), "X1") {
		t.Errorf("want error naming the listing, got %v", err)
	}
}

func TestContentHashIgnoresNoise(t *testing.T) {
	a, _ := FromRESO(decode(t, sold))
	b := a
	later := a.ModifiedAt.Add(time.Hour)
	b.ModifiedAt = later
	b.PhotosChangedAt = &later
	if a.ContentHash() != b.ContentHash() {
		t.Error("timestamp-only change altered the hash")
	}

	price := 789900.0
	b.ListPrice = &price
	if a.ContentHash() == b.ContentHash() {
		t.Error("price change did not alter the hash")
	}
}
