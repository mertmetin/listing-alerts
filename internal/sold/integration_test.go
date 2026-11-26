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
