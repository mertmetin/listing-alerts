// Command poller follows one listing source into the listing.changes topic.
//
//	SOURCE=vow     PropTx VOW (PROPTX_VOW_TOKEN): full lifecycle incl. sold and leased prices
//	SOURCE=idx     PropTx IDX (PROPTX_IDX_TOKEN): what may be shown publicly; reconciled hourly
//	SOURCE=offmls  hotrentals.ca off-MLS rental files (OFFMLS_FILES)
//
// Run one replica per source: a source's cursor has a single writer. Scaling
// happens downstream, where listing.changes partitions fan out to workers.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mertmetin/listing-alerts/internal/config"
	"github.com/mertmetin/listing-alerts/internal/db"
	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/health"
	"github.com/mertmetin/listing-alerts/internal/ingest"
	"github.com/mertmetin/listing-alerts/internal/listing"
	"github.com/mertmetin/listing-alerts/internal/platform"
	"github.com/mertmetin/listing-alerts/internal/recorder"
	"github.com/mertmetin/listing-alerts/internal/reso"
)

const (
	defaultScope    = "PropertyType eq 'Residential Freehold' or PropertyType eq 'Residential Condo & Other'"
	defaultBaseline = "StandardStatus eq 'Active' or StandardStatus eq 'Active Under Contract'"
)

func main() {
	if err := run(); err != nil {
		os.Stderr.WriteString("poller: " + err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	source := os.Getenv("SOURCE")
	cfg, err := config.Load("poller-"+source, os.Getenv)
	if err != nil {
		return err
	}
	log := platform.NewLogger(cfg).With("source", source)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	deps, err := platform.Connect(ctx, cfg, ingest.ProducerOpts()...)
	if err != nil {
		return err
	}
	defer deps.Close()

	applied, err := migrateWithRetry(ctx, deps)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if len(applied) > 0 {
		log.Info("applied migrations", "names", applied)
	}

	reg := platform.NewRegistry(cfg)
	metrics := ingest.NewMetrics(reg)
	checks := health.New(2 * time.Second)
	deps.RegisterChecks(checks)

	store := ingest.PGStore{Pool: deps.PG}
	pub := ingest.KafkaPublisher{Client: deps.Kafka, Topic: ingest.TopicChanges}
	interval := duration("POLL_INTERVAL", time.Minute)

	var loop func(context.Context) error
	switch source {
	case event.SourceVOW, event.SourceIDX:
		token := os.Getenv("PROPTX_" + strings.ToUpper(source) + "_TOKEN")
		if token == "" {
			return fmt.Errorf("PROPTX_%s_TOKEN is not set (run `make tokens`)", strings.ToUpper(source))
		}
		client := reso.New(env("RESO_URL", "https://query.ampre.ca/odata"), token)
		if ua := os.Getenv("RESO_USER_AGENT"); ua != "" {
			client.WithUserAgent(ua)
		}
		var rec ingest.Recorder = ingest.NopRecorder{}
		if dir := os.Getenv("RECORD_DIR"); dir != "" {
			rec = &recorder.Recorder{Dir: dir}
		}
		reconcile := time.Duration(0)
		if source == event.SourceIDX {
			reconcile = duration("RECONCILE_EVERY", time.Hour)
		}
		p := &ingest.Poller{
			Source: source,
			Feed: &reso.Source{
				Client:         client,
				Scope:          env("SCOPE_FILTER", defaultScope),
				BaselineFilter: env("BASELINE_FILTER", defaultBaseline),
				Select:         listing.RESOFields,
				PageSize:       intEnv("PAGE_SIZE", 1000),
				Overlap:        5 * time.Minute,
			},
			Store: store, Pub: pub, Rec: rec, Log: log, Metrics: metrics, Now: time.Now,
			Interval:        interval,
			Lookback:        duration("LOOKBACK", 3*time.Minute),
			ReconcileEvery:  reconcile,
			MinKeepFraction: 0.5,
		}
		loop = p.Run
	case event.SourceOffMLS:
		var paths []string
		for _, p := range strings.Split(os.Getenv("OFFMLS_FILES"), ",") {
			if p = strings.TrimSpace(p); p != "" {
				paths = append(paths, p)
			}
		}
		if len(paths) == 0 {
			return errors.New("OFFMLS_FILES is not set")
		}
		p := &ingest.SnapshotPoller{
			Source: source, Files: ingest.OffMLSFiles{Paths: paths},
			Store: store, Pub: pub, Log: log, Metrics: metrics, Now: time.Now, Interval: interval,
		}
		loop = p.Run
	default:
		return fmt.Errorf("SOURCE must be vow, idx or offmls, got %q", source)
	}

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: platform.OpsMux(checks, reg), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 2)
	go func() { errc <- platform.Serve(ctx, log, srv, cfg.ShutdownTimeout) }()
	go func() { errc <- loop(ctx) }()

	log.Info("polling", "interval", interval)
	err = <-errc
	stop()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}

// migrateWithRetry waits for Postgres on startup instead of crash-looping.
func migrateWithRetry(ctx context.Context, deps *platform.Deps) ([]string, error) {
	var err error
	for i := 0; i < 30; i++ {
		var applied []string
		if applied, err = db.Migrate(ctx, deps.PG); err == nil {
			return applied, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, err
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func duration(k string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(k)); err == nil && d >= 0 {
		return d
	}
	return def
}

func intEnv(k string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(k)); err == nil && n > 0 {
		return n
	}
	return def
}
