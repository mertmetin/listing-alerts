//go:build integration

package hub

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mertmetin/listing-alerts/internal/db"
)

// isolated migrates a throwaway schema so tests never touch real hub rows.
func isolated(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("itest_hub_%d", time.Now().UnixNano())
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

type countingMedia struct {
	mu    sync.Mutex
	calls [][]string
}

func (m *countingMedia) Photos(_ context.Context, keys []string) (map[string][]Photo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, append([]string(nil), keys...))
	out := map[string][]Photo{}
	for _, k := range keys {
		out[k] = []Photo{{Large: "https://img/" + k + "-l.jpg"}}
	}
	return out, nil
}

func (m *countingMedia) Rooms(_ context.Context, keys []string) (map[string][]Room, error) {
	out := map[string][]Room{}
	for _, k := range keys {
		out[k] = []Room{{Type: "Kitchen"}}
	}
	return out, nil
}

func rec(key, county, tx string, price float64, mod, photos string) map[string]any {
	return map[string]any{"ListingKey": key, "CountyOrParish": county, "TransactionType": tx, "ListPrice": price,
		"ModificationTimestamp": mod, "PhotosChangeTimestamp": photos, "PublicRemarks": "Bright and clean."}
}

func TestApplyRemoveAndFeed(t *testing.T) {
	ctx := context.Background()
	media := &countingMedia{}
	h := &Hub{Pool: isolated(t), Media: media, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	n, err := h.Apply(ctx, []map[string]any{
		rec("A", "Ottawa", "For Sale", 600000, "2026-11-24T01:00:00Z", "2026-11-20T00:00:00Z"),
		rec("B", "Ottawa", "For Lease", 2400, "2026-11-24T01:00:01Z", "2026-11-20T00:00:00Z"),
		rec("C", "Toronto", "For Sale", 900000, "2026-11-24T01:00:02Z", "2026-11-20T00:00:00Z"),
	})
	if err != nil || n != 3 || len(media.calls) != 1 {
		t.Fatalf("first apply: %d changed, err %v, media calls %d", n, err, len(media.calls))
	}

	// A timestamp-only bump: no change, no photo fetch.
	if n, _ = h.Apply(ctx, []map[string]any{rec("A", "Ottawa", "For Sale", 600000, "2026-11-24T02:00:00Z", "2026-11-20T00:00:00Z")}); n != 0 {
		t.Fatalf("timestamp-only bump changed %d", n)
	}
	// Price drop: a change, but photos unchanged so no photo fetch, and photos kept.
	if n, _ = h.Apply(ctx, []map[string]any{rec("A", "Ottawa", "For Sale", 579000, "2026-11-24T03:00:00Z", "2026-11-20T00:00:00Z")}); n != 1 || len(media.calls) != 1 {
		t.Fatalf("price drop: %d changed, media calls %d", n, len(media.calls))
	}
	// New photos: refetched.
	if n, _ = h.Apply(ctx, []map[string]any{rec("B", "Ottawa", "For Lease", 2400, "2026-11-24T04:00:00Z", "2026-11-24T04:00:00Z")}); n != 1 || len(media.calls) != 2 || media.calls[1][0] != "B" {
		t.Fatalf("new photos: %d changed, media calls %v", n, media.calls)
	}
	// Stale re-read: ignored.
	if n, _ = h.Apply(ctx, []map[string]any{rec("A", "Ottawa", "For Sale", 1, "2026-11-23T00:00:00Z", "2026-11-20T00:00:00Z")}); n != 0 {
		t.Fatal("stale record applied")
	}
	if n, _ = h.Remove(ctx, []string{"C", "C", "nope"}); n != 1 {
		t.Fatalf("removed %d", n)
	}

	// Feed: everything in seq order; latest version of each listing once.
	all, err := h.Changes(ctx, 0, 100, Filter{})
	if err != nil {
		t.Fatal(err)
	}
	order := ""
	for _, l := range all {
		order += l.Key
	}
	if order != "ABC" {
		t.Fatalf("feed order %q, want ABC (A price drop, B photos, C removal)", order)
	}
	a := all[0]
	if a.Record["ListPrice"].(float64) != 579000 || len(a.Photos) != 1 || a.Photos[0].Large != "https://img/A-l.jpg" || len(a.Rooms) != 1 {
		t.Fatalf("A in feed %+v", a)
	}
	if all[2].Present || all[2].Record != nil {
		t.Fatalf("removed C should come without record: %+v", all[2])
	}

	// Paging by seq and filtering by county and transaction.
	page, _ := h.Changes(ctx, all[0].Seq, 100, Filter{County: "Ottawa"})
	if len(page) != 1 || page[0].Key != "B" {
		t.Fatalf("after A, Ottawa only: %+v", page)
	}
	leases, _ := h.Changes(ctx, 0, 100, Filter{Transaction: "For Lease"})
	if len(leases) != 1 || leases[0].Key != "B" {
		t.Fatalf("leases %+v", leases)
	}
	if none, _ := h.Changes(ctx, all[2].Seq, 100, Filter{}); len(none) != 0 {
		t.Fatalf("nothing after the last seq, got %d", len(none))
	}

	// C comes back: present again, photos refetched.
	if n, _ = h.Apply(ctx, []map[string]any{rec("C", "Toronto", "For Sale", 899000, "2026-11-24T05:00:00Z", "2026-11-20T00:00:00Z")}); n != 1 || len(media.calls) != 3 {
		t.Fatalf("relisted C: %d changed, media calls %d", n, len(media.calls))
	}
}

func TestBackfillResumes(t *testing.T) {
	ctx := context.Background()
	var rows []map[string]any
	for i := range 7 {
		rows = append(rows, rec(fmt.Sprintf("K%02d", i), "Ottawa", "For Sale", 500000, "2026-11-24T01:00:00Z", "2026-11-20T00:00:00Z"))
	}
	// Fake RESO server that pages by ListingKey gt.
	c := fakePagedAPI(t, rows)
	h := &Hub{Pool: isolated(t), Media: &countingMedia{}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := h.Backfill(ctx, c, "", "", 3); err != nil {
		t.Fatal(err)
	}
	present, _, _, _ := h.Stats(ctx)
	if present != 7 {
		t.Fatalf("backfilled %d, want 7", present)
	}
	if done, _ := h.State(ctx, "backfill_done"); done == "" {
		t.Fatal("backfill not marked done")
	}
	// A second run is a no-op.
	if err := h.Backfill(ctx, c, "", "", 3); err != nil {
		t.Fatal(err)
	}
}
