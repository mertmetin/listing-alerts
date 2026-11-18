// Command detector turns listing.changes into typed listing.events (new
// listing, price drop, sold, leased, back on market, ...).
//
// It is stateless: listing state lives in Postgres, offsets in Redpanda.
// Run as many replicas as listing.changes has partitions (12); the consumer
// group spreads partitions across them and rebalances when one dies.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/mertmetin/listing-alerts/internal/config"
	"github.com/mertmetin/listing-alerts/internal/db"
	"github.com/mertmetin/listing-alerts/internal/detect"
	"github.com/mertmetin/listing-alerts/internal/geocode"
	"github.com/mertmetin/listing-alerts/internal/health"
	"github.com/mertmetin/listing-alerts/internal/platform"
)

func main() {
	if err := run(); err != nil {
		os.Stderr.WriteString("detector: " + err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("detector", os.Getenv)
	if err != nil {
		return err
	}
	log := platform.NewLogger(cfg)
	if h, _ := os.Hostname(); h != "" {
		log = log.With("instance", h)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	deps, err := platform.Connect(ctx, cfg, detect.ConsumerOpts(detect.Group, detect.TopicChanges)...)
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

	d := &detect.Detector{
		Store:   detect.PGStore{Pool: deps.PG},
		Pub:     detect.KafkaPublisher{Client: deps.Kafka, Topic: detect.TopicEvents},
		Metrics: detect.NewMetrics(reg),
		Log:     log,
		Now:     time.Now,
	}
	if path := os.Getenv("ADDRESS_POINTS_DB"); path != "" {
		g, err := geocode.Open(path)
		if err != nil {
			log.Warn("geocoding disabled", "err", err)
		} else {
			defer g.Close()
			d.Geo = g
			log.Info("geocoding on", "db", path, "fsa_fallbacks", g.FSAs())
		}
	}
	batch := 500
	if n, err := strconv.Atoi(os.Getenv("BATCH_SIZE")); err == nil && n > 0 {
		batch = n
	}

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: platform.OpsMux(checks, reg), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 2)
	go func() { errc <- platform.Serve(ctx, log, srv, cfg.ShutdownTimeout) }()
	go func() { errc <- detect.Run(ctx, deps.Kafka, d, batch, log) }()
	log.Info("consuming", "topic", detect.TopicChanges, "group", detect.Group, "batch", batch)

	err = <-errc
	stop()
	// Leave the group cleanly so partitions move to other replicas at once.
	deps.Kafka.LeaveGroup()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}
