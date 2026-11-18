// Command loadgen drives the pipeline for load and chaos tests by producing
// listing changes straight to listing.changes (as the pollers would).
//
//	loadgen -mode steady -rate 2000 -duration 2m     synthetic, paced
//	loadgen -mode burst  -changes 250000             synthetic, as fast as possible
//	loadgen -mode replay -replay data/recordings -speed 1000
//
// Every produced change is written to -log for cmd/audit. With -searches N
// it first seeds N synthetic saved searches (site "loadtest").
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/ingest"
	"github.com/mertmetin/listing-alerts/internal/listing"
	"github.com/mertmetin/listing-alerts/internal/load"
	"github.com/mertmetin/listing-alerts/internal/recorder"
)

func main() {
	mode := flag.String("mode", "steady", "steady, burst or replay")
	brokers := flag.String("brokers", "localhost:29092", "Kafka API seed brokers")
	pgURL := flag.String("pg", os.Getenv("POSTGRES_URL"), "Postgres URL (for -searches)")
	run := flag.String("run", time.Now().UTC().Format("0102150405"), "run ID (prefixes keys and search IDs)")
	logPath := flag.String("log", "", "where to write produced changes (default data/load/<run>.changes.gz)")
	nSearch := flag.Int("searches", 0, "seed this many synthetic saved searches first")
	pool := flag.Int("pool", 50000, "listings active at a time (steady/burst)")
	rate := flag.Int("rate", 1000, "changes per second (steady)")
	duration := flag.Duration("duration", time.Minute, "how long to run (steady)")
	total := flag.Int("changes", 100000, "how many changes (burst)")
	replayDir := flag.String("replay", "data/recordings", "recordings directory (replay)")
	speed := flag.Float64("speed", 1000, "replay speed-up")
	backfillFor := flag.Duration("replay-baseline", 10*time.Minute, "recorded pages this soon after the first are the baseline (backfill)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *logPath == "" {
		*logPath = filepath.Join("data", "load", *run+".changes.gz")
	}
	must(os.MkdirAll(filepath.Dir(*logPath), 0o700))

	if *nSearch > 0 {
		pg, err := pgxpool.New(ctx, *pgURL)
		must(err)
		t := time.Now()
		_, err = load.SeedSearches(ctx, pg, *run, 1, *nSearch)
		must(err)
		pg.Close()
		fmt.Printf("seeded %d saved searches in %v; waiting 5s for matcher shards to pick them up\n", *nSearch, time.Since(t).Round(time.Millisecond))
		time.Sleep(5 * time.Second)
	}

	cl, err := kgo.NewClient(append(ingest.ProducerOpts(),
		kgo.SeedBrokers(strings.Split(*brokers, ",")...), kgo.MaxBufferedRecords(200_000),
		kgo.ProducerLinger(2*time.Millisecond))...)
	must(err)
	defer cl.Close()
	lg, err := load.CreateLog(*logPath)
	must(err)

	var produced, failed atomic.Int64
	send := func(cs []event.Change) {
		must(lg.Write(cs))
		for _, c := range cs {
			b, err := c.Encode()
			must(err)
			cl.Produce(ctx, &kgo.Record{Topic: ingest.TopicChanges, Key: []byte(c.ListingKey), Value: b}, func(_ *kgo.Record, err error) {
				if err != nil {
					failed.Add(1)
					fmt.Fprintln(os.Stderr, "produce:", err)
				} else {
					produced.Add(1)
				}
			})
		}
	}

	start := time.Now()
	fmt.Printf("run %s, mode %s, log %s\n", *run, *mode, *logPath)
	go func() { // progress
		for t := time.NewTicker(5 * time.Second); ctx.Err() == nil; <-t.C {
			if el := time.Since(start).Seconds(); el > 1 {
				fmt.Printf("  %.0fs: %d produced (%.0f/s)\n", el, produced.Load(), float64(produced.Load())/el)
			}
		}
	}()

	n := 0
	switch *mode {
	case "steady":
		g := load.NewGen(*run, 7, *pool)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		end := start.Add(*duration)
		for now := range tick.C {
			if now.After(end) || ctx.Err() != nil {
				break
			}
			want := int(now.Sub(start).Seconds() * float64(*rate))
			for n < want {
				cs := g.Next(time.Now())
				send(cs)
				n += len(cs)
			}
		}
	case "burst":
		g := load.NewGen(*run, 7, *pool)
		for n < *total && ctx.Err() == nil {
			cs := g.Next(time.Now())
			send(cs)
			n += len(cs)
		}
	case "replay":
		n = replay(ctx, *replayDir, *speed, *backfillFor, send)
	default:
		fmt.Fprintln(os.Stderr, "unknown -mode")
		os.Exit(2)
	}
	must(cl.Flush(context.Background()))
	must(lg.Close())
	el := time.Since(start)
	fmt.Printf("done: %d changes produced in %v (%.0f/s), %d failed\n", produced.Load(), el.Round(time.Millisecond), float64(produced.Load())/el.Seconds(), failed.Load())
	fmt.Printf("audit with: go run ./cmd/audit -run %s -log %s\n", *run, *logPath)
	if failed.Load() > 0 {
		os.Exit(1)
	}
}

// replay re-produces recorded pages in fetch order, compressing the time
// between them by speed. Pages in the first baselineFor of the recording
// are the first-run baseline and are marked backfill, as they were live.
func replay(ctx context.Context, dir string, speed float64, baselineFor time.Duration, send func([]event.Change)) int {
	type page struct {
		source string
		p      recorder.Page
	}
	var pages []page
	for _, src := range []string{event.SourceVOW, event.SourceIDX} {
		files, _ := filepath.Glob(filepath.Join(dir, src, "*.jsonl.gz"))
		sort.Strings(files)
		for _, f := range files {
			must(recorder.Read(f, func(p recorder.Page) error {
				pages = append(pages, page{src, p})
				return nil
			}))
		}
	}
	if len(pages) == 0 {
		fmt.Fprintln(os.Stderr, "no recordings in", dir)
		os.Exit(1)
	}
	sort.SliceStable(pages, func(i, j int) bool { return pages[i].p.FetchedAt.Before(pages[j].p.FetchedAt) })
	first := pages[0].p.FetchedAt
	fmt.Printf("replaying %d pages recorded %s to %s at %.0fx\n", len(pages), first.Format(time.RFC3339),
		pages[len(pages)-1].p.FetchedAt.Format(time.RFC3339), speed)
	t0 := time.Now()
	n := 0
	for _, pg := range pages {
		due := t0.Add(time.Duration(float64(pg.p.FetchedAt.Sub(first)) / speed))
		if d := time.Until(due); d > 0 {
			select {
			case <-ctx.Done():
				return n
			case <-time.After(d):
			}
		}
		backfill := pg.p.FetchedAt.Sub(first) < baselineFor
		now := time.Now()
		var cs []event.Change
		for _, row := range pg.p.Rows {
			l, err := listing.FromRESO(row)
			if err != nil {
				continue
			}
			cs = append(cs, event.NewObserved(pg.source, l, now, backfill))
		}
		send(cs)
		n += len(cs)
	}
	return n
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}
