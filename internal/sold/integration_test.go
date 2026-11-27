//go:build integration

package sold

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mertmetin/listing-alerts/internal/db"
	"github.com/mertmetin/listing-alerts/internal/reso"
)

func isolated(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("itest_sold_%d", time.Now().UnixNano())
	admin, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, _ := pgxpool.ParseConfig(os.Getenv("POSTGRES_URL"))
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	return pool
}

// pages is a feed that returns canned pages in order.
type pages struct{ p []reso.Page }

func (f *pages) Fetch(context.Context, reso.Cursor, time.Time) (reso.Page, error) {
	if len(f.p) == 0 {
		return reso.Page{}, nil
	}
	p := f.p[0]
	f.p = f.p[1:]
	return p, nil
}

func sale(key, mod string, price float64) map[string]any {
	return map[string]any{"ListingKey": key, "ModificationTimestamp": mod, "MlsStatus": "Sold",
		"CountyOrParish": "Ottawa", "ClosePrice": price, "CloseDate": "2025-06-30", "PurchaseContractDate": "2025-05-01"}
}

func TestStepStoresAndNeverGoesBackwards(t *testing.T) {
	ctx := context.Background()
	pool := isolated(t)
	cur1 := reso.Cursor{Phase: reso.Baseline, Key: "X2"}
	cur2 := reso.Cursor{Phase: reso.Incremental, TS: time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)}
	a := &Archive{Pool: pool, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: time.Now, Feed: &pages{p: []reso.Page{
		{Records: []map[string]any{sale("X1", "2026-11-01T00:00:00Z", 500000), sale("X2", "2026-11-01T00:00:00Z", 700000)}, Next: cur1, More: true},
		// A correction for X1, then a stale re-read of X2 that must not win.
		{Records: []map[string]any{sale("X1", "2026-11-02T00:00:00Z", 510000), sale("X2", "2026-10-01T00:00:00Z", 1)}, Next: cur2},
	}}}
	for i := 0; i < 2; i++ {
		if _, err := a.Step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	prices := map[string]float64{}
	rows, err := pool.Query(ctx, `SELECT listing_key, close_price::float8 FROM sold_listings`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var k string
		var p float64
		rows.Scan(&k, &p)
		prices[k] = p
	}
	if prices["X1"] != 510000 || prices["X2"] != 700000 || len(prices) != 2 {
		t.Fatalf("prices = %v", prices)
	}
	got, err := a.Cursor(ctx)
	if err != nil || got.Phase != reso.Incremental || !got.TS.Equal(cur2.TS) {
		t.Fatalf("cursor = %+v, %v", got, err)
	}
	var contract string
	pool.QueryRow(ctx, `SELECT contract_date::text FROM sold_listings WHERE listing_key = 'X1'`).Scan(&contract)
	if contract != "2025-05-01" {
		t.Fatalf("contract_date = %q", contract)
	}
}

func TestLookup(t *testing.T) {
	ctx := context.Background()
	pool := isolated(t)
	ins := func(key, county, num, street, unit, date string, price float64, extra map[string]any) {
		rec := map[string]any{"StreetNumber": num, "StreetName": street, "UnitNumber": unit, "PropertySubType": "Detached",
			"BedroomsAboveGrade": 3.0, "ClosePrice": price, "ListPrice": price}
		for k, v := range extra {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		if _, err := pool.Exec(ctx, `INSERT INTO sold_listings (listing_key, county, close_price, contract_date, modified_at, record)
			VALUES ($1, $2, $3, $4::date, now(), $5)`, key, county, price, date, b); err != nil {
			t.Fatal(err)
		}
	}
	ins("A1", "Ottawa", "12", "Maple Ave", "", "2025-01-10", 600000, map[string]any{"FoundationDetails": []any{"Poured Concrete"}})
	ins("A2", "Ottawa", "12", "MAPLE", "", "2026-05-02", 690000, map[string]any{"BedroomsAboveGrade": 4.0})
	ins("B1", "Ottawa", "300", "Queen St", "1204", "2025-06-01", 450000, nil)
	ins("B2", "Ottawa", "300", "Queen St", "805", "2025-08-01", 410000, nil)
	ins("C1", "Toronto", "12", "Maple Ave", "", "2026-06-01", 999000, nil)

	h, err := Lookup(ctx, pool, "Ottawa", Address{Number: "12", Street: "Maple Avenue"})
	if err != nil || h == nil {
		t.Fatalf("lookup: %v %v", h, err)
	}
	if h.Key != "A2" || h.Date != "2026-05" || h.Facts["BedroomsAboveGrade"] != 4.0 {
		t.Fatalf("not the newest Ottawa record: %+v", h)
	}
	for _, banned := range []string{"ClosePrice", "ListPrice"} {
		if _, ok := h.Facts[banned]; ok {
			t.Fatalf("lookup leaked %s", banned)
		}
	}
	if h, _ := Lookup(ctx, pool, "Ottawa", Address{Number: "300", Street: "Queen Street"}); h != nil {
		t.Fatalf("guessed a unit in a multi-unit building: %+v", h)
	}
	if h, _ := Lookup(ctx, pool, "Ottawa", Address{Number: "300", Street: "Queen", Unit: "#805"}); h == nil || h.Key != "B2" {
		t.Fatalf("unit match: %+v", h)
	}
	if h, _ := Lookup(ctx, pool, "Ottawa", Address{Number: "13", Street: "Maple Ave"}); h != nil {
		t.Fatalf("wrong number matched: %+v", h)
	}
}

func TestStreetMix(t *testing.T) {
	ctx := context.Background()
	pool := isolated(t)
	add := func(key, num, street, typ string) {
		rec := map[string]any{"StreetNumber": num, "StreetName": street, "PropertySubType": typ}
		if num == "14" || num == "16" || num == "900" {
			rec["ApproximateAge"] = "16-30"
		}
		if num == "20" {
			rec["ApproximateAge"] = "100+" // the address itself: never counted
		}
		b, _ := json.Marshal(rec)
		if _, err := pool.Exec(ctx, `INSERT INTO sold_listings (listing_key, county, modified_at, record) VALUES ($1, 'Ottawa', now(), $2)`, key, b); err != nil {
			t.Fatal(err)
		}
	}
	add("S0", "20", "Elm St", "Condo Apartment") // the visitor's own address: never counted
	for i, n := range []string{"14", "16", "18", "22", "24", "26"} {
		add(fmt.Sprintf("S%d", i+1), n, "ELM", "Detached")
	}
	add("S7", "28", "Elm Street", "Semi-Detached ")
	add("S8", "900", "Elm St", "Att/Row/Townhouse")
	add("S9", "21", "Elmwood Ave", "Condo Apartment") // other street, same first word
	h := Homes{Pool: pool, County: "Ottawa"}
	mix, err := h.StreetMix(ctx, Address{Number: "20", Street: "Elm Street"})
	if err != nil {
		t.Fatal(err)
	}
	if mix.N != 8 || mix.Types["Detached"] != 6 || mix.Types["Semi-Detached"] != 1 || mix.Types["Condo Apartment"] != 0 {
		t.Fatalf("mix = %+v", mix)
	}
	// Ages look along the whole street (900 is far, but has an age), never the address itself.
	if mix.AgeN != 3 || mix.Ages["16-30"] != 3 || mix.Ages["100+"] != 0 {
		t.Fatalf("ages = %+v", mix)
	}
}

func TestNeighbours(t *testing.T) {
	ctx := context.Background()
	pool := isolated(t)
	add := func(key, num, unit, street string, price float64) {
		b, _ := json.Marshal(map[string]any{"StreetNumber": num, "UnitNumber": unit, "StreetName": street, "PropertySubType": "Detached",
			"LivingAreaRange": "1500-2000", "ClosePrice": price, "ListPrice": price, "PublicRemarks": "Lovely"})
		if _, err := pool.Exec(ctx, `INSERT INTO sold_listings (listing_key, county, close_price, modified_at, record) VALUES ($1, 'Ottawa', $2, now(), $3)`, key, price, b); err != nil {
			t.Fatal(err)
		}
	}
	add("N0", "20", "", "Elm St", 1)
	add("N1", "26", "", "Elm St", 600000)
	add("N2", "18", "", "ELM", 610000)
	add("N3", "40", "", "Elm Street", 620000)
	add("N4", "22", "", "Elmwood Ave", 630000)
	homes, err := Homes{Pool: pool, County: "Ottawa"}.Neighbours(ctx, Address{Number: "20", Street: "Elm Street"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(homes) != 2 || homes[0].Gap != 2 || homes[1].Gap != 6 {
		t.Fatalf("not the two nearest, nearest first: %+v", homes)
	}
	for _, h := range homes {
		for _, banned := range []string{"ClosePrice", "ListPrice", "StreetNumber", "UnitNumber", "StreetName", "PublicRemarks"} {
			if _, ok := h.Facts[banned]; ok {
				t.Fatalf("neighbour lookup leaked %s", banned)
			}
		}
		if h.Facts["LivingAreaRange"] != "1500-2000" {
			t.Fatalf("facts missing: %+v", h)
		}
	}
}

func TestNeighboursSameBuilding(t *testing.T) {
	ctx := context.Background()
	pool := isolated(t)
	add := func(key, num, unit string) {
		b, _ := json.Marshal(map[string]any{"StreetNumber": num, "UnitNumber": unit, "StreetName": "Queen St", "PropertySubType": "Condo Apartment"})
		if _, err := pool.Exec(ctx, `INSERT INTO sold_listings (listing_key, county, modified_at, record) VALUES ($1, 'Ottawa', now(), $2)`, key, b); err != nil {
			t.Fatal(err)
		}
	}
	add("U1", "300", "1204") // the visitor's own unit
	add("U2", "300", "805")
	add("U3", "300", "1501")
	add("U4", "310", "2")
	h := Homes{Pool: pool, County: "Ottawa"}
	homes, err := h.Neighbours(ctx, Address{Number: "300", Street: "Queen Street", Unit: "#1204"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(homes) != 3 || homes[0].Gap != 0 || homes[1].Gap != 0 || homes[2].Gap != 10 {
		t.Fatalf("want the building's other units first, own unit left out: %+v", homes)
	}
	// Without a unit the whole building is "the address" and is left out.
	if homes, _ := h.Neighbours(ctx, Address{Number: "300", Street: "Queen Street"}, 10); len(homes) != 1 {
		t.Fatalf("no unit: %+v", homes)
	}
}

func TestOffMarketArchive(t *testing.T) {
	ctx := context.Background()
	pool := isolated(t)
	rec := func(key, status, mls, mod string) map[string]any {
		return map[string]any{"ListingKey": key, "ModificationTimestamp": mod, "StandardStatus": status, "MlsStatus": mls,
			"TransactionType": "For Sale", "CountyOrParish": "Ottawa", "PropertySubType": "Detached", "ListPrice": 650000.0,
			"OriginalListPrice": 699000.0, "ListingContractDate": "2026-05-01", "DaysOnMarket": 45.0, "ExpirationDate": "2026-08-31"}
	}
	a := &Archive{Kind: KindOffMarket, Pool: pool, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: time.Now, Feed: &pages{p: []reso.Page{
		{Records: []map[string]any{rec("T1", "Cancelled", "Terminated", "2026-06-15T00:00:00Z"), rec("E1", "Expired", "Expired", "2026-09-01T00:00:00Z")},
			Next: reso.Cursor{Phase: reso.Incremental, TS: time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)}},
	}}}
	if _, err := a.Step(ctx); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	rows, err := pool.Query(ctx, `SELECT listing_key, mls_status || ' ' || off_market_date::text || ' ' || days_on_market || ' ' || original_list_price::int FROM offmarket_listings`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var k, v string
		rows.Scan(&k, &v)
		got[k] = v
	}
	if got["T1"] != "Terminated 2026-06-15 45 699000" || got["E1"] != "Expired 2026-08-31 45 699000" || len(got) != 2 {
		t.Fatalf("offmarket rows = %v", got)
	}
	// Its own cursor: the sold archive's position is untouched.
	var n int
	pool.QueryRow(ctx, `SELECT count(*) FROM sold_state`).Scan(&n)
	if n != 0 {
		t.Fatal("offmarket archive wrote the sold cursor")
	}
	if c, err := a.Cursor(ctx); err != nil || c.Phase != reso.Incremental {
		t.Fatalf("cursor %+v %v", c, err)
	}
}

func TestLookupUnsold(t *testing.T) {
	ctx := context.Background()
	pool := isolated(t)
	sold := func(key, num, date string) {
		b, _ := json.Marshal(map[string]any{"StreetNumber": num, "StreetName": "Prominence", "PropertySubType": "Att/Row/Townhouse",
			"ClosePrice": 600000.0})
		if _, err := pool.Exec(ctx, `INSERT INTO sold_listings (listing_key, county, close_price, contract_date, modified_at, record)
			VALUES ($1, 'Ottawa', 600000, $2::date, now(), $3)`, key, date, b); err != nil {
			t.Fatal(err)
		}
	}
	unsold := func(key, num, status string, price float64, off time.Time) {
		b, _ := json.Marshal(map[string]any{"StreetNumber": num, "StreetName": "Prominence Way", "PropertySubType": "Att/Row/Townhouse",
			"MlsStatus": status, "ListPrice": price, "BedroomsAboveGrade": 3.0})
		if _, err := pool.Exec(ctx, `INSERT INTO offmarket_listings (listing_key, county, standard_status, mls_status, list_price,
			off_market_date, modified_at, record) VALUES ($1, 'Ottawa', 'Expired', $2, $3, $4::date, now(), $5)`, key, status, price, off, b); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	// 560: expired four months ago, never sold since -> cap.
	unsold("A1", "560", "Expired", 749000, now.AddDate(0, -4, 0))
	unsold("A0", "560", "Terminated", 779000, now.AddDate(0, -9, 0))
	// 562: terminated, then sold -> no cap.
	unsold("B1", "562", "Terminated", 699000, now.AddDate(0, -6, 0))
	sold("B2", "562", now.AddDate(0, -2, 0).Format("2006-01-02"))
	// 564: expired 20 months ago -> too old.
	unsold("C1", "564", "Expired", 650000, now.AddDate(0, -20, 0))
	h, err := Lookup(ctx, pool, "Ottawa", Address{Number: "560", Street: "Prominence Way"})
	if err != nil || h == nil || h.Unsold == nil {
		t.Fatalf("560: %+v %v", h, err)
	}
	if h.Unsold.ListPrice != 749000 || h.Unsold.Status != "Expired" || h.Facts["BedroomsAboveGrade"] != 3.0 {
		t.Fatalf("560 unsold: %+v", h.Unsold)
	}
	if h, _ := Lookup(ctx, pool, "Ottawa", Address{Number: "562", Street: "Prominence"}); h == nil || h.Unsold != nil || h.Source != "sold" {
		t.Fatalf("562 sold after: %+v", h)
	}
	if h, _ := Lookup(ctx, pool, "Ottawa", Address{Number: "564", Street: "Prominence"}); h == nil || h.Unsold != nil {
		t.Fatalf("564 too old: %+v", h)
	}
	if _, ok := h.Facts["ListPrice"]; ok {
		t.Fatal("lookup facts leaked a price")
	}
}
