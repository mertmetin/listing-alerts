//go:build integration

package detect

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mertmetin/listing-alerts/internal/db"
	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/ingest"
)

func pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	p, err := pgxpool.New(context.Background(), os.Getenv("POSTGRES_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	if _, err := db.Migrate(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPGStoreOptimisticConcurrency(t *testing.T) {
	ctx := context.Background()
	p := pool(t)
	s := PGStore{Pool: p}
	key := fmt.Sprintf("IT-%d", time.Now().UnixNano())
	t.Cleanup(func() { p.Exec(ctx, `DELETE FROM listing_state WHERE listing_key LIKE $1`, key+"%") })

	if err := s.Save(ctx, []State{{Key: key, Phase: event.PhaseActive}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, []State{{Key: key, Phase: event.PhaseOff}}); err != ErrConflict {
		t.Fatalf("second insert of a new key: %v, want ErrConflict", err)
	}
	got, _ := s.Load(ctx, []string{key})
	st := got[key]
	if st.Version != 1 || st.Phase != event.PhaseActive {
		t.Fatalf("loaded %+v", st)
	}

	st.Phase = event.PhaseSold
	stale := st
	if err := s.Save(ctx, []State{st}); err != nil {
		t.Fatal(err)
	}
	// Writing from the old version fails, and fails as a whole batch.
	other := State{Key: key + "-b"}
	if err := s.Save(ctx, []State{other, stale}); err != ErrConflict {
		t.Fatalf("stale write: %v", err)
	}
	if got, _ := s.Load(ctx, []string{key + "-b"}); len(got) != 0 {
		t.Fatal("conflicting batch was partly written")
	}
}

// TestGroupSurvivesWorkerCrash runs two detectors in one consumer group,
// kills one midway, and checks that every expected event still comes out.
func TestGroupSurvivesWorkerCrash(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	p := pool(t)
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	run := time.Now().UnixNano()
	in, out, group := fmt.Sprintf("itest-in-%d", run), fmt.Sprintf("itest-out-%d", run), fmt.Sprintf("itest-detector-%d", run)
	prefix := fmt.Sprintf("IT%d-", run)
	t.Cleanup(func() {
		p.Exec(context.Background(), `DELETE FROM listing_state WHERE listing_key LIKE $1`, prefix+"%")
	})

	admCl, _ := kgo.NewClient(kgo.SeedBrokers(brokers...))
	defer admCl.Close()
	adm := kadm.NewClient(admCl)
	if _, err := adm.CreateTopics(ctx, 6, 1, nil, in, out); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		adm.DeleteTopics(context.Background(), in, out)
		adm.DeleteGroup(context.Background(), group)
	})

	// 300 listings, each: new, price drop, sold. 900 changes, 900 events expected.
	var cs []event.Change
	const n = 300
	for i := range n {
		cs = append(cs, changes(fmt.Sprintf("%s%04d", prefix, i),
			step{src: vow, status: "Active", price: 500000},
			step{src: vow, status: "Active", price: 480000},
			step{src: vow, status: "Closed", price: 480000, close: 490000})...)
	}
	prod, _ := kgo.NewClient(append(ingest.ProducerOpts(), kgo.SeedBrokers(brokers...))...)
	defer prod.Close()
	if err := (ingest.KafkaPublisher{Client: prod, Topic: in}).Publish(ctx, cs); err != nil {
		t.Fatal(err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	worker := func(wctx context.Context, batch int) (*kgo.Client, chan error) {
		cl, err := kgo.NewClient(append(ConsumerOpts(group, in), kgo.SeedBrokers(brokers...))...)
		if err != nil {
			t.Fatal(err)
		}
		d := &Detector{Store: PGStore{Pool: p}, Pub: KafkaPublisher{Client: cl, Topic: out},
			Metrics: NewMetrics(prometheus.NewRegistry()), Log: log, Now: time.Now}
		done := make(chan error, 1)
		go func() { done <- Run(wctx, cl, d, batch, log) }()
		return cl, done
	}

	// Count unique events on the output topic as they arrive.
	var mu sync.Mutex
	unique := map[string]event.Type{}
	total := 0
	reader, _ := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(out), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer reader.Close()
	go func() {
		for ctx.Err() == nil {
			reader.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
				e, _ := event.DecodeListingEvent(r.Value)
				mu.Lock()
				unique[e.ID] = e.Type
				total++
				mu.Unlock()
			})
		}
	}()
	count := func() int { mu.Lock(); defer mu.Unlock(); return len(unique) }

	aCtx, killA := context.WithCancel(ctx)
	aCl, aDone := worker(aCtx, 20) // small batches so A is mid-stream when killed
	_, bDone := worker(ctx, 20)

	for count() < 200 && ctx.Err() == nil {
		time.Sleep(20 * time.Millisecond)
	}
	killA() // worker A stops mid-stream: its in-flight batch is never committed
	<-aDone
	aCl.Close() // and it leaves the group; B takes over A's partitions

	for count() < 3*n && ctx.Err() == nil {
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(time.Second) // let any trailing duplicates land
	cancel()
	<-bDone

	mu.Lock()
	defer mu.Unlock()
	byType := map[event.Type]int{}
	for _, ty := range unique {
		byType[ty]++
	}
	if byType[event.NewListing] != n || byType[event.PriceDrop] != n || byType[event.Sold] != n {
		t.Fatalf("unique events by type %v (of %d published), want %d of each", byType, total, n)
	}
	t.Logf("%d unique events, %d published (%d duplicates from the crash, dropped by ID)", len(unique), total, total-len(unique))
}
