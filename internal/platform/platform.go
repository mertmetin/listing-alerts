// Package platform holds the plumbing every service shares: logging, the ops
// HTTP server (/healthz, /readyz, /metrics) and connections to Postgres, Redis
// and Redpanda with matching readiness checks.
package platform

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mertmetin/listing-alerts/internal/config"
	"github.com/mertmetin/listing-alerts/internal/health"
)

// Version is set at build time with -ldflags "-X .../platform.Version=...".
var Version = "dev"

// Topics every pipeline stage relies on. They are created by the
// redpanda-init container (deploy/compose) and checked in readiness.
var Topics = []string{"listing.changes", "listing.events", "alerts.pending", "alerts.dlq"}

// NewLogger returns a JSON logger tagged with the service name.
func NewLogger(cfg config.Config) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})
	return slog.New(h).With("service", cfg.Service, "version", Version)
}

// Deps are the shared backing services.
type Deps struct {
	PG    *pgxpool.Pool
	Redis *redis.Client
	Kafka *kgo.Client
}

// Connect opens clients for Postgres, Redis and Redpanda. The clients connect
// lazily, so a dependency that is down shows up in /readyz, not as a crash
// loop; the service recovers by itself when the dependency comes back.
func Connect(ctx context.Context, cfg config.Config, opts ...kgo.Opt) (*Deps, error) {
	pg, err := pgxpool.New(ctx, cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("postgres config: %w", err)
	}
	kc, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.ClientID("listing-alerts-" + cfg.Service),
	}, opts...)...)
	if err != nil {
		pg.Close()
		return nil, fmt.Errorf("kafka config: %w", err)
	}
	return &Deps{
		PG:    pg,
		Redis: redis.NewClient(&redis.Options{Addr: cfg.RedisAddr}),
		Kafka: kc,
	}, nil
}

// RegisterChecks adds one readiness check per dependency.
func (d *Deps) RegisterChecks(c *health.Checker) {
	c.Add("postgres", func(ctx context.Context) error { return d.PG.Ping(ctx) })
	c.Add("redis", func(ctx context.Context) error { return d.Redis.Ping(ctx).Err() })
	c.Add("redpanda", func(ctx context.Context) error {
		details, err := kadm.NewClient(d.Kafka).ListTopics(ctx, Topics...)
		if err != nil {
			return err
		}
		var missing []error
		for _, t := range Topics {
			if td, ok := details[t]; !ok || td.Err != nil {
				missing = append(missing, fmt.Errorf("topic %s missing", t))
			}
		}
		return errors.Join(missing...)
	})
}

// Close releases every client.
func (d *Deps) Close() {
	d.Kafka.Close()
	_ = d.Redis.Close()
	d.PG.Close()
}

// NewRegistry returns a Prometheus registry with Go runtime, process and
// build_info metrics. Services register their own metrics on it.
func NewRegistry(cfg config.Config) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	info := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "listing_alerts",
		Name:      "build_info",
		Help:      "Always 1; labels carry the service name and version.",
	}, []string{"service", "version"})
	info.WithLabelValues(cfg.Service, Version).Set(1)
	reg.MustRegister(info)
	return reg
}

// OpsMux serves /healthz (liveness), /readyz (readiness) and /metrics.
func OpsMux(checks *health.Checker, reg *prometheus.Registry) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("GET /readyz", checks.Handler())
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	return mux
}

// Serve runs srv until ctx is cancelled, then shuts it down gracefully.
func Serve(ctx context.Context, log *slog.Logger, srv *http.Server, timeout time.Duration) error {
	errc := make(chan error, 1)
	go func() {
		log.Info("http listening", "addr", srv.Addr)
		errc <- srv.ListenAndServe()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return srv.Shutdown(sctx)
}
