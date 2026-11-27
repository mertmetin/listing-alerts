//go:build integration

// Integration tests against the Docker Compose stack: `make itest`.
package ingest

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mertmetin/listing-alerts/internal/db"
	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/listing"
	"github.com/mertmetin/listing-alerts/internal/reso"
)

func pg(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), os.Getenv("POSTGRES_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

func TestPGStore(t *testing.T) {
	ctx := context.Background()
	pool := pg(t)
	s := PGStore{Pool: pool}
	src := fmt.Sprintf("itest-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM source_listings WHERE source = $1`, src)
		pool.Exec(ctx, `DELETE FROM ingest_cursors WHERE source = $1`, src)
	})

	if c, err := s.Cursor(ctx, src); err != nil || c.Phase != "" {
		t.Fatalf("fresh cursor %+v, err %v", c, err)
	}
	now := time.Date(2026, 11, 17, 12, 0, 0, 123000000, time.UTC)
	price := 1.5e6
	l := listing.Listing{Key: "A", ListPrice: &price, ModifiedAt: now}
	cur := reso.Cursor{Phase: reso.Incremental, TS: now, Key: "A"}
	err := s.Commit(ctx, src, &cur, []Snapshot{
		{Key: "A", Version: Version{Hash: "h1", ModifiedAt: now, Present: true}, Listing: &l},
		{Key: "B", Version: Version{Hash: "h2", ModifiedAt: now, Present: true}, Listing: &l},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Cursor(ctx, src); got != cur {
		t.Fatalf("cursor %+v, want %+v", got, cur)
	}

	// Remove B: the stored listing body must survive.
	if err := s.Commit(ctx, src, nil, []Snapshot{{Key: "B", Version: Version{Hash: "h2", ModifiedAt: now, Present: false}}}); err != nil {
		t.Fatal(err)
	}
	vs, err := s.Versions(ctx, src, []string{"A", "B", "C"})
	if err != nil || len(vs) != 2 || vs["B"].Present || !vs["A"].ModifiedAt.Equal(now) {
		t.Fatalf("versions %+v, err %v", vs, err)
	}
	present, _ := s.PresentKeys(ctx, src)
	if len(present) != 1 || present["A"].Hash != "h1" {
		t.Fatalf("present %+v", present)
	}
	var body *string
	pool.QueryRow(ctx, `SELECT listing::text FROM source_listings WHERE source=$1 AND listing_key='B'`, src).Scan(&body)
	if body == nil || !strings.Contains(*body, `"list_price": 1500000`) {
		t.Fatalf("removed listing body lost: %v", body)
	}
}

func TestKafkaPublisherKeysByListing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	topic := fmt.Sprintf("itest-changes-%d", time.Now().UnixNano())

	cl, err := kgo.NewClient(append(ProducerOpts(), kgo.SeedBrokers(brokers...))...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopic(ctx, 6, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { adm.DeleteTopics(context.Background(), topic) })

	var changes []event.Change
	for i := range 30 {
		l := listing.Listing{Key: fmt.Sprintf("L%d", i%5), ModifiedAt: time.Unix(int64(i), 0)}
		changes = append(changes, event.NewObserved("vow", l, time.Now(), false))
	}
	if err := (KafkaPublisher{Client: cl, Topic: topic}).Publish(ctx, changes); err != nil {
		t.Fatal(err)
	}

	cons, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	partitionOf := map[string]int32{}
	lastTS := map[string]time.Time{}
	got := 0
	for got < len(changes) {
		fs := cons.PollFetches(ctx)
		if err := fs.Err(); err != nil {
			t.Fatal(err)
		}
		fs.EachRecord(func(r *kgo.Record) {
			got++
			c, err := event.Decode(r.Value)
			if err != nil {
				t.Fatal(err)
			}
			if string(r.Key) != c.ListingKey {
				t.Errorf("record key %q, listing %q", r.Key, c.ListingKey)
			}
			if p, ok := partitionOf[c.ListingKey]; ok && p != r.Partition {
				t.Errorf("listing %s on partitions %d and %d", c.ListingKey, p, r.Partition)
			}
			partitionOf[c.ListingKey] = r.Partition
			if c.ModifiedAt.Before(lastTS[c.ListingKey]) {
				t.Errorf("listing %s out of order", c.ListingKey)
			}
			lastTS[c.ListingKey] = c.ModifiedAt
		})
	}
}

func TestListingHistory(t *testing.T) {
	ctx := context.Background()
	pool := pg(t)
	s := PGStore{Pool: pool}
	key := fmt.Sprintf("H%d", time.Now().UnixNano())
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM source_listings WHERE source = 'vow' AND listing_key = $1`, key)
		pool.Exec(ctx, `DELETE FROM listing_history WHERE listing_key = $1`, key)
	})
	put := func(at time.Time, status, mls string, price float64, hash string) {
		t.Helper()
		l := listing.Listing{Key: key, StandardStatus: status, MlsStatus: mls, ListPrice: &price, ModifiedAt: at}
		if err := s.Commit(ctx, HistorySource, nil, []Snapshot{{Key: key, Version: Version{Hash: hash, ModifiedAt: at, Present: true}, Listing: &l}}); err != nil {
			t.Fatal(err)
		}
	}
	t0 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	put(t0, "Active", "New", 699000, "a")
	put(t0.Add(24*time.Hour), "Active", "New", 699000, "b")          // photos changed: no history row
	put(t0.Add(48*time.Hour), "Active", "Price Change", 679000, "c") // price drop
	put(t0.Add(72*time.Hour), "Active Under Contract", "Sold Conditional", 679000, "d")
	// Removal without a body never adds a row.
	if err := s.Commit(ctx, HistorySource, nil, []Snapshot{{Key: key, Version: Version{Hash: "d", ModifiedAt: t0.Add(96 * time.Hour), Present: false}}}); err != nil {
		t.Fatal(err)
	}
	rows, err := pool.Query(ctx, `SELECT mls_status, list_price::int FROM listing_history WHERE listing_key = $1 ORDER BY at`, key)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var m string
		var p int
		rows.Scan(&m, &p)
		got = append(got, fmt.Sprintf("%s %d", m, p))
	}
	want := []string{"New 699000", "Price Change 679000", "Sold Conditional 679000"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("history = %v, want %v", got, want)
	}
	// Other sources (IDX) don't write history.
	other := fmt.Sprintf("itest-%d", time.Now().UnixNano())
	t.Cleanup(func() { pool.Exec(ctx, `DELETE FROM source_listings WHERE source = $1`, other) })
	p := 1.0
	if err := s.Commit(ctx, other, nil, []Snapshot{{Key: key + "x", Version: Version{Hash: "z", ModifiedAt: t0, Present: true},
		Listing: &listing.Listing{Key: key + "x", ListPrice: &p, ModifiedAt: t0}}}); err != nil {
		t.Fatal(err)
	}
	var n int
	pool.QueryRow(ctx, `SELECT count(*) FROM listing_history WHERE listing_key = $1`, key+"x").Scan(&n)
	if n != 0 {
		t.Fatalf("non-VOW source wrote %d history rows", n)
	}
}
