package event

import (
	"testing"
	"time"

	"github.com/mertmetin/listing-alerts/internal/listing"
)

func TestIDIsDeterministicAndDistinct(t *testing.T) {
	t0 := time.Date(2026, 11, 17, 5, 15, 29, 0, time.UTC)
	a := ID(Observed, SourceVOW, "X1", "h1", t0)
	if a != ID(Observed, SourceVOW, "X1", "h1", t0.In(time.FixedZone("EST", -5*3600))) {
		t.Error("same instant in another zone gave another ID")
	}
	others := []string{
		ID(Removed, SourceVOW, "X1", "h1", t0),
		ID(Observed, SourceIDX, "X1", "h1", t0),
		ID(Observed, SourceVOW, "X2", "h1", t0),
		ID(Observed, SourceVOW, "X1", "h2", t0),
		ID(Observed, SourceVOW, "X1", "h1", t0.Add(time.Second)), // A -> B -> A
		ID(Observed, SourceVOW, "X", "1h1", t0),                  // no ambiguity from concatenation
	}
	for i, o := range others {
		if o == a {
			t.Errorf("variant %d collides with base ID", i)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	price := 2450.0
	l := listing.Listing{Key: "E9", TransactionType: "For Lease", ListPrice: &price, ModifiedAt: time.Now().UTC()}
	c := NewObserved(SourceIDX, l, time.Now(), true)
	b, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != c.ID || got.Listing == nil || *got.Listing.ListPrice != price || !got.Backfill || got.V != Version {
		t.Errorf("round trip: %+v", got)
	}
}
