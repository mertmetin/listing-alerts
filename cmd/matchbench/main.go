// Command matchbench measures the saved-search matcher at scale: build time,
// memory, per-event match latency (p50/p99/p999) and throughput, unsharded
// and split over a consistent-hash ring.
//
//	go run ./cmd/matchbench -searches 1000000 -events 200000 -shards 5
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/mertmetin/listing-alerts/internal/search"
)

func main() {
	nSearch := flag.Int("searches", 1_000_000, "saved searches")
	nEvents := flag.Int("events", 200_000, "listing events to match")
	shards := flag.Int("shards", 5, "shards for the sharded run")
	workers := flag.Int("workers", runtime.GOMAXPROCS(0), "goroutines for the throughput run")
	cpuprofile := flag.String("cpuprofile", "", "write a CPU profile of the latency run here")
	flag.Parse()

	fmt.Printf("matchbench: %d searches, %d events, %d shards, %d workers, %s/%s, %d CPUs\n\n",
		*nSearch, *nEvents, *shards, *workers, runtime.GOOS, runtime.GOARCH, runtime.NumCPU())

	syn := search.NewSynth(42)
	t0 := time.Now()
	prepared := make([]search.Prepared, *nSearch)
	var wg sync.WaitGroup
	// Searches must come from one generator to be reproducible; coverings
	// are the expensive part and are computed in parallel.
	ss := make([]*search.Search, *nSearch)
	for i := range ss {
		ss[i] = syn.Search(i)
	}
	for w := range runtime.NumCPU() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < *nSearch; i += runtime.NumCPU() {
				prepared[i] = search.Prepare(ss[i])
			}
		}()
	}
	wg.Wait()
	coverTime := time.Since(t0)

	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	t1 := time.Now()
	ix := search.NewIndex()
	for _, p := range prepared {
		ix.Upsert(p, nil)
	}
	buildTime := time.Since(t1)
	var after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&after)
	st := ix.Stats()
	fmt.Printf("Build: coverings %v (parallel), index %v, %d cells, %d entries (%.1f per search), heap +%d MB\n",
		coverTime.Round(time.Millisecond), buildTime.Round(time.Millisecond), st.Cells, st.Entries,
		float64(st.Entries)/float64(st.Searches), (after.HeapAlloc-before.HeapAlloc)>>20)

	events := make([]search.Query, *nEvents)
	for i := range events {
		events[i] = search.QueryFor(syn.Event(i))
	}

	// Latency: one goroutine, every event timed.
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
	}
	lat, matches := latency(ix, events)
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "\nrun\tp50\tp99\tp99.9\tmax\tmatches/event\tevents/s\t")
	row(tw, "1 index, 1 goroutine", lat, matches, len(events), 0)

	// Throughput: all cores matching against the one index.
	n, el := throughput(func(q search.Query, buf []*search.Search) []*search.Search { return ix.Match(q, buf) }, events, *workers)
	row(tw, fmt.Sprintf("1 index, %d goroutines", *workers), nil, 0, n, el)

	// Sharded: each event goes to its owner shard on the ring.
	ring := search.NewRing(*shards, 128)
	parts := make([]*search.Index, *shards)
	held := 0
	for s := range parts {
		parts[s] = search.NewIndex()
		owns := ring.OwnsFunc(s)
		for _, p := range prepared {
			if parts[s].Upsert(p, owns) {
				held++
			}
		}
	}
	load := make([]int, *shards)
	for _, q := range events {
		load[ring.QueryOwner(q)]++
	}
	slat := make([]time.Duration, 0, len(events))
	smatch := 0
	var buf []*search.Search
	for _, q := range events {
		ix := parts[ring.QueryOwner(q)]
		t := time.Now()
		buf = ix.Match(q, buf[:0])
		slat = append(slat, time.Since(t))
		smatch += len(buf)
	}
	row(tw, fmt.Sprintf("%d shards, 1 goroutine", *shards), slat, smatch, len(events), 0)
	tw.Flush()

	fmt.Printf("\nSharding: searches held %.2fx (city searches and areas crossing shard cells are on several shards)\n", float64(held)/float64(*nSearch))
	fmt.Printf("Event share per shard: ")
	for s, n := range load {
		fmt.Printf("%d:%.1f%% ", s, 100*float64(n)/float64(len(events)))
	}
	fmt.Println()
	if smatch != matches {
		fmt.Printf("\nMISMATCH: sharded run found %d matches, unsharded %d\n", smatch, matches)
		os.Exit(1)
	}
	fmt.Println("Sharded and unsharded runs found the same matches.")
}

func latency(ix *search.Index, events []search.Query) ([]time.Duration, int) {
	lat := make([]time.Duration, 0, len(events))
	matches := 0
	var buf []*search.Search
	for _, q := range events[:min(1000, len(events))] { // warm up
		buf = ix.Match(q, buf[:0])
	}
	for _, q := range events {
		t := time.Now()
		buf = ix.Match(q, buf[:0])
		lat = append(lat, time.Since(t))
		matches += len(buf)
	}
	return lat, matches
}

func throughput(match func(search.Query, []*search.Search) []*search.Search, events []search.Query, workers int) (int, time.Duration) {
	var next atomic.Int64
	var wg sync.WaitGroup
	t := time.Now()
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buf []*search.Search
			for {
				i := next.Add(1) - 1
				if i >= int64(len(events)) {
					return
				}
				buf = match(events[i], buf[:0])
			}
		}()
	}
	wg.Wait()
	return len(events), time.Since(t)
}

func row(tw *tabwriter.Writer, name string, lat []time.Duration, matches, n int, el time.Duration) {
	if lat == nil {
		fmt.Fprintf(tw, "%s\t\t\t\t\t\t%.0f\t\n", name, float64(n)/el.Seconds())
		return
	}
	slices.Sort(lat)
	q := func(f float64) time.Duration { return lat[min(len(lat)-1, int(f*float64(len(lat))))] }
	var total time.Duration
	for _, d := range lat {
		total += d
	}
	fmt.Fprintf(tw, "%s\t%v\t%v\t%v\t%v\t%.1f\t%.0f\t\n", name, q(.5).Round(time.Microsecond/10), q(.99).Round(time.Microsecond/10),
		q(.999).Round(time.Microsecond/10), lat[len(lat)-1].Round(time.Microsecond), float64(matches)/float64(n), float64(n)/total.Seconds())
}
