// Package listing defines the canonical listing record that flows through the
// pipeline, the mapping from RESO Data Dictionary fields, and the content hash
// used to tell real changes from feed noise.
package listing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Listing is the subset of a RESO Property record the alert engine needs.
// Pointers mark fields the feed may omit; nil means "not provided".
type Listing struct {
	Key             string `json:"key"`
	TransactionType string `json:"transaction_type"` // For Sale, For Lease, For Sub-Lease
	StandardStatus  string `json:"standard_status"`  // Active, Active Under Contract, Closed, Expired, ...
	MlsStatus       string `json:"mls_status"`       // New, Price Change, Sold, Leased, Terminated, ...
	PropertyType    string `json:"property_type,omitempty"`
	PropertySubType string `json:"property_sub_type,omitempty"`

	ListPrice         *float64   `json:"list_price,omitempty"`
	OriginalListPrice *float64   `json:"original_list_price,omitempty"`
	PreviousListPrice *float64   `json:"previous_list_price,omitempty"`
	ClosePrice        *float64   `json:"close_price,omitempty"` // VOW only
	CloseDate         *string    `json:"close_date,omitempty"`  // YYYY-MM-DD
	PriceChangedAt    *time.Time `json:"price_changed_at,omitempty"`

	Bedrooms  *int     `json:"bedrooms,omitempty"`
	Bathrooms *int     `json:"bathrooms,omitempty"`
	AreaRange string   `json:"area_range,omitempty"` // e.g. "1500-2000" (sq ft)
	Parking   *float64 `json:"parking,omitempty"`

	StreetNumber string `json:"street_number,omitempty"`
	StreetName   string `json:"street_name,omitempty"`
	StreetSuffix string `json:"street_suffix,omitempty"`
	Unit         string `json:"unit,omitempty"`
	Address      string `json:"address,omitempty"`
	City         string `json:"city,omitempty"`
	Community    string `json:"community,omitempty"` // CityRegion
	County       string `json:"county,omitempty"`
	Province     string `json:"province,omitempty"`
	PostalCode   string `json:"postal_code,omitempty"`

	// Latitude/Longitude are empty in PropTx feeds; a geocoding stage fills them.
	Latitude  *float64 `json:"lat,omitempty"`
	Longitude *float64 `json:"lng,omitempty"`

	AddressDisplay bool   `json:"address_display"`
	ListingDisplay bool   `json:"listing_display"`
	ListOffice     string `json:"list_office,omitempty"`

	ListedAt   *time.Time `json:"listed_at,omitempty"`
	ModifiedAt time.Time  `json:"modified_at"`
	// PhotosChangedAt is carried for the UI but left out of the content hash.
	PhotosChangedAt *time.Time `json:"photos_changed_at,omitempty"`
}

// RESOFields is the $select list for Property requests, in the RESO names.
var RESOFields = []string{
	"ListingKey", "TransactionType", "StandardStatus", "MlsStatus", "PropertyType", "PropertySubType",
	"ListPrice", "OriginalListPrice", "PreviousListPrice", "ClosePrice", "CloseDate", "PriceChangeTimestamp",
	"BedroomsTotal", "BathroomsTotalInteger", "LivingAreaRange", "ParkingTotal",
	"StreetNumber", "StreetName", "StreetSuffix", "UnitNumber", "UnparsedAddress",
	"City", "CityRegion", "CountyOrParish", "StateOrProvince", "PostalCode",
	"Latitude", "Longitude",
	"InternetAddressDisplayYN", "InternetEntireListingDisplayYN", "ListOfficeName",
	"OriginalEntryTimestamp", "ModificationTimestamp", "PhotosChangeTimestamp",
}

// FromRESO maps one decoded RESO Property record. It fails only when the
// record cannot be identified or ordered (no ListingKey or timestamp).
func FromRESO(r map[string]any) (Listing, error) {
	l := Listing{
		Key:             str(r, "ListingKey"),
		TransactionType: str(r, "TransactionType"),
		StandardStatus:  str(r, "StandardStatus"),
		MlsStatus:       str(r, "MlsStatus"),
		PropertyType:    str(r, "PropertyType"),
		PropertySubType: str(r, "PropertySubType"),

		ListPrice:         num(r, "ListPrice"),
		OriginalListPrice: num(r, "OriginalListPrice"),
		PreviousListPrice: num(r, "PreviousListPrice"),
		ClosePrice:        num(r, "ClosePrice"),
		PriceChangedAt:    ts(r, "PriceChangeTimestamp"),

		Bedrooms:  integer(r, "BedroomsTotal"),
		Bathrooms: integer(r, "BathroomsTotalInteger"),
		AreaRange: str(r, "LivingAreaRange"),
		Parking:   num(r, "ParkingTotal"),

		StreetNumber: str(r, "StreetNumber"),
		StreetName:   str(r, "StreetName"),
		StreetSuffix: str(r, "StreetSuffix"),
		Unit:         str(r, "UnitNumber"),
		Address:      str(r, "UnparsedAddress"),
		City:         str(r, "City"),
		Community:    str(r, "CityRegion"),
		County:       str(r, "CountyOrParish"),
		Province:     str(r, "StateOrProvince"),
		PostalCode:   str(r, "PostalCode"),
		Latitude:     num(r, "Latitude"),
		Longitude:    num(r, "Longitude"),

		AddressDisplay: boolean(r, "InternetAddressDisplayYN"),
		ListingDisplay: boolean(r, "InternetEntireListingDisplayYN"),
		ListOffice:     str(r, "ListOfficeName"),

		ListedAt:        ts(r, "OriginalEntryTimestamp"),
		PhotosChangedAt: ts(r, "PhotosChangeTimestamp"),
	}
	if d := str(r, "CloseDate"); d != "" {
		l.CloseDate = &d
	}
	if l.Key == "" {
		return l, fmt.Errorf("record has no ListingKey")
	}
	m := ts(r, "ModificationTimestamp")
	if m == nil {
		return l, fmt.Errorf("listing %s has no ModificationTimestamp", l.Key)
	}
	l.ModifiedAt = *m
	return l, nil
}

// ContentHash identifies the alert-relevant content of a listing. Timestamps
// that change without the listing changing (ModificationTimestamp bumps,
// photo re-uploads) are excluded, so they do not produce events.
func (l Listing) ContentHash() string {
	c := l
	c.ModifiedAt = time.Time{}
	c.PhotosChangedAt = nil
	b, err := json.Marshal(c) // struct field order makes this deterministic
	if err != nil {
		panic(err) // only plain values: cannot happen
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16])
}

func str(r map[string]any, k string) string {
	s, _ := r[k].(string)
	return s
}

func num(r map[string]any, k string) *float64 {
	switch v := r[k].(type) {
	case float64:
		return &v
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return &f
		}
	}
	return nil
}

func integer(r map[string]any, k string) *int {
	if f := num(r, k); f != nil {
		i := int(*f)
		return &i
	}
	return nil
}

func boolean(r map[string]any, k string) bool {
	b, _ := r[k].(bool)
	return b
}

func ts(r map[string]any, k string) *time.Time {
	s := str(r, k)
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil
	}
	t = t.UTC()
	return &t
}
