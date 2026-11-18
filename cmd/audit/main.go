// Command audit proves a load or chaos run lost nothing.
//
// It replays the run's change log (written by loadgen) through the same pure
// functions the services use, detect.Apply and the search index, to work
// out every listing event and alert that must exist, by ID (IDs are
// deterministic). Then it reads what the system actually produced on
// listing.events, alerts.pending and in the deliveries table, and reports
// missing, unexpected and duplicate items, plus latency and throughput.
//
//	audit -run 1118063012 -log data/load/1118063012.changes.gz
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mertmetin/listing-alerts/internal/deliver"
	"github.com/mertmetin/listing-alerts/internal/detect"
	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/load"
	"github.com/mertmetin/listing-alerts/internal/match"
	"github.com/mertmetin/listing-alerts/internal/search"
)

func main() {
	run := flag.String("run", "", "run ID")
	logPath := flag.String("log", "", "change log from loadgen")
	brokers := flag.String("brokers", "localhost:29092", "Kafka API seed brokers")
	pgURL := flag.String("pg", os.Getenv("POSTGRES_URL"), "Postgres URL")
	groups := flag.String("groups", "detector,matcher-0-of-3,matcher-1-of-3,matcher-2-of-3,delivery", "consumer groups that must drain first")
	wait := flag.Duration("wait", 10*time.Minute, "how long to wait for the pipeline to drain")
	flag.Parse()
	if *run == "" || *logPath == "" {
		fmt.Fprintln(os.Stderr, "usage: audit -run ID -log FILE")
		os.Exit(2)
	}
	ctx := context.Background()
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...))
	must(err)
	defer cl.Close()

	// 1. Wait until every stage has consumed everything.
	t := time.Now()
	must(waitDrained(ctx, kadm.NewClient(cl), strings.Split(*groups, ","), *wait))
	fmt.Printf("pipeline drained (waited %v)\n", time.Since(t).Round(time.Second))
	pg, err := pgxpool.New(ctx, *pgURL)
	must(err)
	defer pg.Close()
	waitQueue(ctx, pg, *run)

	// 2. Expected, by replaying the log through the real rules.
	var changes []event.Change
	must(load.ReadLog(*logPath, func(c event.Change) error { changes = append(changes, c); return nil }))
	keys := map[string]bool{}
	states := map[string]detect.State{}
	expEvents := map[string]event.ListingEvent{}
	var firstObserved, lastObserved time.Time
	for _, c := range changes {
		keys[c.ListingKey] = true
		if firstObserved.IsZero() || c.ObservedAt.Before(firstObserved) {
			firstObserved = c.ObservedAt
		}
		lastObserved = maxT(lastObserved, c.ObservedAt)
		s, ok := states[c.ListingKey]
		if !ok {
			s = detect.State{Key: c.ListingKey}
		}
		next, evs, out := detect.Apply(s, c, time.Now())
		if out == detect.Applied || out == detect.Backfill {
			states[c.ListingKey] = next
		}
		for _, e := range evs {
			expEvents[e.ID] = e
		}
	}
	ix := search.NewIndex()
	nSearch := 0
	prefix := "lt-" + *run + "-"
	_, err = search.Store{Pool: pg}.Changes(ctx, time.Time{}, func(s *search.Search) error {
		if strings.HasPrefix(s.ID, prefix) && !s.Deleted {
			ix.Upsert(search.Prepare(s), nil)
			nSearch++
		}
		return nil
	})
	must(err)
	expAlerts := map[string]bool{}
	// Delivery also drops an alert whose meaning repeats an earlier one
	// (same search, listing, type and price): one queued row per key.
	expKeys := map[string]string{} // dedup key -> first alert ID
	var buf []*search.Search
	for _, e := range expEvents {
		buf = ix.Match(search.QueryFor(e), buf[:0])
		for _, s := range buf {
			a := match.NewAlert(e, s, time.Now())
			expAlerts[a.ID] = true
			if _, ok := expKeys[deliver.DedupKey(a)]; !ok {
				expKeys[deliver.DedupKey(a)] = a.ID
			}
		}
	}
	fmt.Printf("expected: %d changes -> %d listing events -> %d alerts (%d saved searches)\n",
		len(changes), len(expEvents), len(expAlerts), nSearch)

	// 3. Actual.
	gotEvents, evDup, evLat, lastDetected := readEvents(ctx, cl, keys)
	gotAlerts, alDup, alLat, lastMatched := readAlerts(ctx, cl, prefix)
	status, keyStatus, sentLat := readDeliveries(ctx, pg, prefix)

	// 4. Compare.
	missEv, extraEv := diff(keysOf(expEvents), gotEvents)
	missAl, extraAl := diff(expAlerts, gotAlerts)
	var notQueued int
	for key := range expKeys {
		if _, ok := keyStatus[key]; !ok {
			notQueued++
		}
	}
	counts := map[string]int{}
	for _, st := range status {
		counts[st]++
	}

	fmt.Println()
	row := func(stage string, exp, got, missing, extra, dup int) {
		fmt.Printf("  %-16s expected %8d  found %8d  missing %6d  unexpected %6d  duplicates %6d\n", stage, exp, got, missing, extra, dup)
	}
	row("listing.events", len(expEvents), len(gotEvents), missEv, extraEv, evDup)
	row("alerts.pending", len(expAlerts), len(gotAlerts), missAl, extraAl, alDup)
	fmt.Printf("  %-16s expected %8d  found %8d  missing %6d  by status %v\n", "deliveries", len(expKeys), len(status), notQueued, counts)
	fmt.Printf("  %-16s (%d alerts repeat earlier news for the same search and are dropped by design; pending = held by per-user rate limits)\n", "", len(expAlerts)-len(expKeys))

	fmt.Println()
	span := lastDetected.Sub(firstObserved)
	fmt.Printf("throughput: changes produced over %v; detection finished %v after the first change (%.0f changes/s end to end)\n",
		lastObserved.Sub(firstObserved).Round(time.Millisecond), span.Round(time.Millisecond), float64(len(changes))/span.Seconds())
	if !lastMatched.IsZero() {
		fmt.Printf("            matching finished %v after the first change (%.0f alerts/s)\n",
			lastMatched.Sub(firstObserved).Round(time.Millisecond), float64(len(gotAlerts))/lastMatched.Sub(firstObserved).Seconds())
	}
	pct("latency: change observed -> listing event", evLat)
	pct("latency: change observed -> alert published", alLat)
	pct("latency: change observed -> email accepted", sentLat)

	if missEv+extraEv+missAl+extraAl+notQueued+counts["failed"] > 0 {
		fmt.Println("\nFAIL")
		os.Exit(1)
	}
	fmt.Println("\nPASS: every expected event and alert was produced exactly as the rules say, and every alert is queued or delivered")
}

func waitDrained(ctx context.Context, adm *kadm.Client, groups []string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	stable := 0
	for time.Now().Before(deadline) {
		lags, err := adm.Lag(ctx, groups...)
		if err == nil {
			total := int64(0)
			for _, g := range groups {
				l, ok := lags[g]
				if !ok || l.Error() != nil {
					total = -1
					break
				}
				total += l.Lag.Total()
			}
			if total == 0 {
				if stable++; stable >= 3 {
					return nil
				}
			} else {
				stable = 0
			}
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("pipeline did not drain within %v", limit)
}

// waitQueue waits until the delivery intake has stopped adding this run's rows.
func waitQueue(ctx context.Context, pg *pgxpool.Pool, run string) {
	prev, same := -1, 0
	for i := 0; i < 120 && same < 3; i++ {
		var n int
		_ = pg.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE alert->>'search_id' LIKE $1`, "lt-"+run+"-%").Scan(&n)
		if n == prev {
			same++
		} else {
			same = 0
		}
		prev = n
		time.Sleep(time.Second)
	}
}

// consume reads a topic from the start to its current end.
func consume(ctx context.Context, cl *kgo.Client, topic string, fn func(*kgo.Record)) {
	adm := kadm.NewClient(cl)
	ends, err := adm.ListEndOffsets(ctx, topic)
	must(err)
	want := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			want[o.Partition] = o.Offset
		}
	})
	offsets := map[string]map[int32]kgo.Offset{topic: {}}
	for p := range want {
		offsets[topic][p] = kgo.NewOffset().AtStart()
	}
	c, err := kgo.NewClient(kgo.SeedBrokers(cl.OptValue(kgo.SeedBrokers).([]string)...), kgo.ConsumePartitions(offsets), kgo.FetchMaxBytes(64<<20))
	must(err)
	defer c.Close()
	for len(want) > 0 {
		fs := c.PollFetches(ctx)
		must(fs.Err())
		fs.EachRecord(func(r *kgo.Record) {
			if r.Offset < want[r.Partition] {
				fn(r)
			}
			if r.Offset >= want[r.Partition]-1 {
				delete(want, r.Partition)
			}
		})
	}
}

func readEvents(ctx context.Context, cl *kgo.Client, keys map[string]bool) (map[string]bool, int, []time.Duration, time.Time) {
	got := map[string]bool{}
	dup := 0
	var lat []time.Duration
	var last time.Time
	consume(ctx, cl, "listing.events", func(r *kgo.Record) {
		if !keys[string(r.Key)] {
			return
		}
		e, err := event.DecodeListingEvent(r.Value)
		must(err)
		if got[e.ID] {
			dup++
			return
		}
		got[e.ID] = true
		lat = append(lat, e.DetectedAt.Sub(e.ObservedAt))
		last = maxT(last, e.DetectedAt)
	})
	return got, dup, lat, last
}

func readAlerts(ctx context.Context, cl *kgo.Client, prefix string) (map[string]bool, int, []time.Duration, time.Time) {
	got := map[string]bool{}
	dup := 0
	var lat []time.Duration
	var last time.Time
	consume(ctx, cl, "alerts.pending", func(r *kgo.Record) {
		if !strings.HasPrefix(string(r.Key), load.Site+":lt-") {
			return
		}
		a, err := event.DecodeAlert(r.Value)
		must(err)
		if !strings.HasPrefix(a.SearchID, prefix) {
			return
		}
		if got[a.ID] {
			dup++
			return
		}
		got[a.ID] = true
		lat = append(lat, a.MatchedAt.Sub(a.ObservedAt))
		last = maxT(last, a.MatchedAt)
	})
	return got, dup, lat, last
}

func readDeliveries(ctx context.Context, pg *pgxpool.Pool, prefix string) (map[string]string, map[string]string, []time.Duration) {
	rows, err := pg.Query(ctx, `SELECT alert_id, dedup_key, status, coalesce(extract(epoch from sent_at - (alert->>'observed_at')::timestamptz), -1)
		FROM deliveries WHERE alert->>'search_id' LIKE $1`, prefix+"%")
	must(err)
	defer rows.Close()
	out, byKey := map[string]string{}, map[string]string{}
	var lat []time.Duration
	for rows.Next() {
		var id, key, st string
		var secs float64
		must(rows.Scan(&id, &key, &st, &secs))
		out[id] = st
		byKey[key] = st
		if secs >= 0 {
			lat = append(lat, time.Duration(secs*float64(time.Second)))
		}
	}
	return out, byKey, lat
}

func diff(want, got map[string]bool) (missing, extra int) {
	for k := range want {
		if !got[k] {
			missing++
		}
	}
	for k := range got {
		if !want[k] {
			extra++
		}
	}
	return
}

func keysOf(m map[string]event.ListingEvent) map[string]bool {
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}

func pct(name string, d []time.Duration) {
	if len(d) == 0 {
		fmt.Printf("%-46s (none)\n", name)
		return
	}
	slices.Sort(d)
	q := func(f float64) time.Duration { return d[min(len(d)-1, int(f*float64(len(d))))].Round(time.Millisecond) }
	fmt.Printf("%-46s p50 %-9v p99 %-9v max %v  (n=%d)\n", name, q(.5), q(.99), d[len(d)-1].Round(time.Millisecond), len(d))
}

func maxT(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "audit:", err)
		os.Exit(1)
	}
}
