// Command matcher runs one shard of the saved-search matcher.
//
//	SHARD_INDEX=0 SHARD_COUNT=3 matcher
//
// Each shard owns part of the map (a consistent-hash ring over 5-character
// geohash cells), holds the saved searches that touch its part, reads every
// listing event, matches the ones located in its part, and publishes alerts
// to alerts.pending. Every shard uses its own consumer group, so each sees
// all events; the ring makes sure exactly one of them acts on each event.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mertmetin/listing-alerts/internal/config"
	"github.com/mertmetin/listing-alerts/internal/db"
	"github.com/mertmetin/listing-alerts/internal/health"
	"github.com/mertmetin/listing-alerts/internal/match"
	"github.com/mertmetin/listing-alerts/internal/platform"
	"github.com/mertmetin/listing-alerts/internal/search"
)

func main() {
	if err := run(); err != nil {
		os.Stderr.WriteString("matcher: " + err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	idx, err1 := strconv.Atoi(os.Getenv("SHARD_INDEX"))
	n, err2 := strconv.Atoi(os.Getenv("SHARD_COUNT"))
	if err1 != nil || err2 != nil || n < 1 || idx < 0 || idx >= n {
		return errors.New("set SHARD_INDEX (0..SHARD_COUNT-1) and SHARD_COUNT")
	}
	cfg, err := config.Load(fmt.Sprintf("matcher-%d", idx), os.Getenv)
	if err != nil {
		return err
	}
	log := platform.NewLogger(cfg).With("shard", idx, "shards", n)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	group := fmt.Sprintf("matcher-%d-of-%d", idx, n)
	deps, err := platform.Connect(ctx, cfg,
		kgo.ConsumerGroup(group), kgo.ConsumeTopics(match.TopicEvents),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()), // a new shard starts with new events
		kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(),
		kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerBatchCompression(kgo.ZstdCompression()))
	if err != nil {
		return err
	}
	defer deps.Close()
	for i := 0; ; i++ {
		if _, err = db.Migrate(ctx, deps.PG); err == nil || i == 30 || ctx.Err() != nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return err
	}

	reg := platform.NewRegistry(cfg)
	checks := health.New(2 * time.Second)
	deps.RegisterChecks(checks)
	sh := &match.Shard{
		ID: idx, Ring: search.NewRing(n, 128), Store: search.Store{Pool: deps.PG},
		Pub:     match.KafkaPublisher{Client: deps.Kafka, Topic: match.TopicAlerts},
		Metrics: match.NewMetrics(reg), Log: log, Now: time.Now,
	}

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: platform.OpsMux(checks, reg), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 2)
	go func() { errc <- platform.Serve(ctx, log, srv, cfg.ShutdownTimeout) }()

	// Searches first: matching before they are loaded would miss alerts.
	if err := sh.Load(ctx); err != nil {
		return fmt.Errorf("load searches: %w", err)
	}
	go sh.Follow(ctx, 2*time.Second)
	go func() { errc <- sh.Run(ctx, deps.Kafka, 500) }()
	log.Info("matching", "group", group)

	err = <-errc
	stop()
	deps.Kafka.LeaveGroup()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}
