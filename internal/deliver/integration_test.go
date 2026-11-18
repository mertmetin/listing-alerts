//go:build integration

// Delivery against real Postgres (in a throwaway schema, so the running
// delivery service never sees these rows), Redis and Mailpit, including
// SMTP failures injected with Mailpit's chaos mode.
package deliver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/mertmetin/listing-alerts/internal/db"
	"github.com/mertmetin/listing-alerts/internal/event"
)

// isolatedStore migrates a fresh schema and returns a store bound to it.
func isolatedStore(t *testing.T) (PGStore, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("itest_%d", time.Now().UnixNano())
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
	return PGStore{Pool: pool}, pool
}

func TestStoreQueueSemantics(t *testing.T) {
	ctx := context.Background()
	st, pool := isolatedStore(t)
	a := alert("a1", "u1", event.NewListing, "vow")
	b := alert("a2", "u1", event.NewListing, "vow") // same search, listing, type, price: a duplicate by meaning
	c := alert("a3", "u2", event.PriceDrop, "vow")
	c.SearchID = "ss_2"
	n, err := st.Enqueue(ctx, []event.Alert{a, a, b, c}, time.Now().Add(-time.Second))
	if err != nil || n != 2 {
		t.Fatalf("enqueue: %d new (want 2: a and c), err %v", n, err)
	}

	// Two senders claiming at once never get the same row.
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			items, err := st.Claim(ctx, 10, time.Minute)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			for _, it := range items {
				seen[it.AlertID]++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(seen) != 2 || seen["a1"] != 1 || seen["a3"] != 1 {
		t.Fatalf("claims %v", seen)
	}

	// A user's backlog is never split across concurrent claimers.
	var backlog []event.Alert
	for i := range 40 {
		x := alert(fmt.Sprintf("bk%d", i), fmt.Sprintf("bkuser%d", i%4), event.NewListing, "vow")
		x.SearchID = fmt.Sprintf("bk-search-%d", i)
		backlog = append(backlog, x)
	}
	_, _ = st.Enqueue(ctx, backlog, time.Now().Add(-time.Second))
	owner := map[string]int{}
	var wg2 sync.WaitGroup
	for w := range 4 {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			items, _ := st.Claim(ctx, 100, time.Minute)
			mu.Lock()
			defer mu.Unlock()
			for _, it := range items {
				if o, ok := owner[it.Alert.UserID]; ok && o != w {
					t.Errorf("user %s claimed by senders %d and %d", it.Alert.UserID, o, w)
				}
				owner[it.Alert.UserID] = w
			}
		}()
	}
	wg2.Wait()
	pool.Exec(ctx, `UPDATE deliveries SET status='sent' WHERE alert_id LIKE 'bk%'`)

	if err := st.Retry(ctx, []string{"a1"}, time.Now().Add(-time.Second), "smtp down"); err != nil {
		t.Fatal(err)
	}
	items, _ := st.Claim(ctx, 10, time.Minute)
	if len(items) != 1 || items[0].AlertID != "a1" || items[0].Attempts != 1 {
		t.Fatalf("after retry %+v", items)
	}
	_ = st.MarkSent(ctx, []string{"a1"}, "<m@x>")

	// a3's sender "died": force its lease to expire and reap it.
	pool.Exec(ctx, `UPDATE deliveries SET lease_until = now() - interval '1 second' WHERE alert_id = 'a3'`)
	if n, err := st.Reap(ctx); err != nil || n != 1 {
		t.Fatalf("reap %d %v", n, err)
	}
	if items, _ := st.Claim(ctx, 10, time.Minute); len(items) != 1 || items[0].AlertID != "a3" {
		t.Fatalf("reaped item not claimable: %+v", items)
	}
	counts, _ := st.Counts(ctx)
	if counts["sent"] != 1 || counts["sending"] != 1 {
		t.Fatalf("counts %v", counts)
	}
}

func TestRedisTokenBucket(t *testing.T) {
	ctx := context.Background()
	cl := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")})
	defer cl.Close()
	now := time.Now()
	l := RedisLimiter{Client: cl, Now: func() time.Time { return now }}
	key := fmt.Sprintf("itest:%d", now.UnixNano())
	defer cl.Del(ctx, "rl:"+key)
	for i := range 3 {
		if ok, _, err := l.Take(ctx, key, 3, time.Minute); err != nil || !ok {
			t.Fatalf("take %d: ok=%v err=%v", i, ok, err)
		}
	}
	ok, wait, _ := l.Take(ctx, key, 3, time.Minute)
	if ok || wait <= 0 || wait > time.Minute {
		t.Fatalf("4th take: ok=%v wait=%v", ok, wait)
	}
	now = now.Add(61 * time.Second)
	if ok, _, _ := l.Take(ctx, key, 3, time.Minute); !ok {
		t.Fatal("no token after the refill interval")
	}
	if ok, _, _ := l.Take(ctx, key, 3, time.Minute); ok {
		t.Fatal("refilled more than one token")
	}
	_ = l.Refund(ctx, key, 3) // a send failed
	if ok, _, _ := l.Take(ctx, key, 3, time.Minute); !ok {
		t.Fatal("refunded token not available")
	}
}

// ---- end to end with Mailpit ----

const mailpit = "http://localhost:8025"

func chaos(t *testing.T, code, probability int) {
	t.Helper()
	body := fmt.Sprintf(`{"Recipient":{"ErrorCode":%d,"Probability":%d}}`, code, probability)
	req, _ := http.NewRequest(http.MethodPut, mailpit+"/api/v1/chaos", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("set chaos: %v %v", resp, err)
	}
	resp.Body.Close()
}

func inbox(t *testing.T, to string) []map[string]any {
	t.Helper()
	resp, err := http.Get(mailpit + "/api/v1/search?query=" + url.QueryEscape("to:"+to))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Messages []map[string]any `json:"messages"`
	}
	b, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(b, &out)
	return out.Messages
}

func TestEndToEndWithFailures(t *testing.T) {
	ctx := context.Background()
	st, pool := isolatedStore(t)
	t.Cleanup(func() { chaos(t, 451, 0) })
	rc := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR")})
	defer rc.Close()

	run := time.Now().UnixNano()
	user := func(n int) string { return fmt.Sprintf("itest-%d-%d", run, n) }
	dlq := &fakeDLQ{}
	s := &Sender{Store: st, Limiter: RedisLimiter{Client: rc, Now: time.Now}, Channel: SMTP{Host: "localhost", Port: 1025, From: "Alerts <alerts@example.com>"},
		Render: renderer(), DLQ: dlq, Metrics: NewMetrics(prometheus.NewRegistry()), Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now: time.Now, Burst: 2, Every: time.Hour, MaxPerEmail: 25, MaxAttempts: 3, Lease: time.Minute, Batch: 100}

	// 1. Three alerts for one user arrive together: one digest email.
	var batch []event.Alert
	for i := range 3 {
		a := alert(fmt.Sprintf("%d-a%d", run, i), user(1), event.NewListing, "vow")
		a.ListingKey = fmt.Sprintf("X%d", i)
		batch = append(batch, a)
	}
	_, _ = st.Enqueue(ctx, batch, time.Now().Add(-time.Second))
	if _, err := s.Step(ctx); err != nil {
		t.Fatal(err)
	}
	msgs := inbox(t, user(1)+"@example.com")
	if len(msgs) != 1 || msgs[0]["Subject"] != "3 updates for your saved searches" {
		t.Fatalf("digest: %v", msgs)
	}

	// 2. Rate limit: burst 2 per hour. A second email goes; a third is held.
	for i := 3; i < 5; i++ {
		a := alert(fmt.Sprintf("%d-a%d", run, i), user(1), event.PriceDrop, "vow")
		a.ListingKey = fmt.Sprintf("X%d", i)
		_, _ = st.Enqueue(ctx, []event.Alert{a}, time.Now().Add(-time.Second))
		_, _ = s.Step(ctx)
	}
	if got := len(inbox(t, user(1)+"@example.com")); got != 2 {
		t.Fatalf("rate limit: %d emails, want 2", got)
	}
	var held int
	pool.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE status='pending' AND attempts=0 AND next_at > now() + interval '30 minutes'`).Scan(&held)
	if held != 1 {
		t.Fatalf("held alerts %d, want 1 (deferred, no attempt counted)", held)
	}

	// 3. Temporary SMTP failure (451): retried later, then delivered.
	chaos(t, 451, 100)
	a := alert(fmt.Sprintf("%d-t", run), user(2), event.Sold, "vow")
	a.SearchID = "ss_temp"
	_, _ = st.Enqueue(ctx, []event.Alert{a}, time.Now().Add(-time.Second))
	_, _ = s.Step(ctx)
	var status string
	var attempts int
	pool.QueryRow(ctx, `SELECT status, attempts FROM deliveries WHERE alert_id=$1`, a.ID).Scan(&status, &attempts)
	if status != "pending" || attempts != 1 || len(inbox(t, user(2)+"@example.com")) != 0 {
		t.Fatalf("after 451: status %s attempts %d", status, attempts)
	}
	chaos(t, 451, 0)
	pool.Exec(ctx, `UPDATE deliveries SET next_at = now() WHERE alert_id=$1`, a.ID) // skip the backoff wait
	_, _ = s.Step(ctx)
	if len(inbox(t, user(2)+"@example.com")) != 1 {
		t.Fatal("not delivered after the server recovered")
	}

	// 4. Permanent failure (550): dead-lettered at once, never retried.
	chaos(t, 550, 100)
	p := alert(fmt.Sprintf("%d-p", run), user(3), event.NewListing, "vow")
	p.SearchID = "ss_perm"
	_, _ = st.Enqueue(ctx, []event.Alert{p}, time.Now().Add(-time.Second))
	_, _ = s.Step(ctx)
	chaos(t, 550, 0)
	pool.QueryRow(ctx, `SELECT status FROM deliveries WHERE alert_id=$1`, p.ID).Scan(&status)
	if status != "failed" || len(dlq.items) != 1 || dlq.items[0].AlertID != p.ID {
		t.Fatalf("550: status %s, dlq %d", status, len(dlq.items))
	}

	// 5. SMTP server unreachable (wrong port): retried, then dead-lettered
	// once attempts run out.
	down := *s
	down.Channel = SMTP{Host: "localhost", Port: 1, From: "a@example.com", Timeout: time.Second}
	d := alert(fmt.Sprintf("%d-d", run), user(4), event.NewListing, "vow")
	d.SearchID = "ss_down" // a search belongs to one user; distinct from p's
	_, _ = st.Enqueue(ctx, []event.Alert{d}, time.Now().Add(-time.Second))
	for range 3 {
		pool.Exec(ctx, `UPDATE deliveries SET next_at = now() WHERE alert_id=$1 AND status='pending'`, d.ID)
		_, _ = down.Step(ctx)
	}
	var lastErr string
	pool.QueryRow(ctx, `SELECT status, attempts, coalesce(last_error,'') FROM deliveries WHERE alert_id=$1`, d.ID).Scan(&status, &attempts, &lastErr)
	if status != "failed" || attempts != 3 || len(dlq.items) != 2 {
		t.Fatalf("unreachable: status %s attempts %d dlq %d (last error %q)", status, attempts, len(dlq.items), lastErr)
	}
}
