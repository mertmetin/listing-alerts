//go:build integration

package search

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mertmetin/listing-alerts/internal/db"
	"github.com/mertmetin/listing-alerts/internal/geo"
)

func TestStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	st := Store{Pool: pool}
	user := fmt.Sprintf("itest-%d", time.Now().UnixNano())
	t.Cleanup(func() { pool.Exec(ctx, `DELETE FROM saved_searches WHERE user_id = $1`, user) })

	start := time.Now().Add(-time.Second)
	s := &Search{Site: "itest", UserID: user, Email: "a@example.com", Audience: VOW, Transaction: "sale",
		Areas: []geo.Polygon{geo.Circle(geo.Point{Lat: 45.4, Lng: -75.7}, 2, 8)}}
	if err := st.Create(ctx, s); err != nil {
		t.Fatal(err)
	}
	got, err := st.Get(ctx, "itest", s.ID)
	if err != nil || got.UserID != user || len(got.Areas) != 1 {
		t.Fatalf("get %+v %v", got, err)
	}
	if _, err := st.Get(ctx, "other-site", s.ID); err != ErrNotFound {
		t.Fatalf("other site can read: %v", err)
	}
	if err := st.Delete(ctx, "itest", s.ID); err != nil {
		t.Fatal(err)
	}
	var sawDelete bool
	if _, err := st.Changes(ctx, start, func(c *Search) error {
		if c.ID == s.ID && c.Deleted {
			sawDelete = true
		}
		return nil
	}); err != nil || !sawDelete {
		t.Fatalf("change feed missed the delete (err %v)", err)
	}
	key, err := st.NewAPIKey(ctx, "itest", "test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Exec(ctx, `DELETE FROM api_keys WHERE key_hash = $1`, HashKey(key)) })
	if site, _ := st.SiteForKey(ctx, key); site != "itest" {
		t.Fatalf("key resolves to %q", site)
	}
}
