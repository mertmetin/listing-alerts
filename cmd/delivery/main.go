// Command delivery sends alerts: it takes alerts.pending into a durable
// Postgres queue and runs senders that email them, one message per user,
// within per-user and daily rate limits, retrying temporary failures and
// dead-lettering the rest to alerts.dlq.
//
// Email goes to SMTP_HOST: Mailpit in development (nothing leaves the
// machine), smtp.gmail.com:587 with an app password in production.
// Replicas can run side by side: senders claim rows with SKIP LOCKED.
package main

import (
	"context"
	"encoding/json"
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
	"github.com/mertmetin/listing-alerts/internal/deliver"
	"github.com/mertmetin/listing-alerts/internal/health"
	"github.com/mertmetin/listing-alerts/internal/platform"
)

func main() {
	if err := run(); err != nil {
		os.Stderr.WriteString("delivery: " + err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("delivery", os.Getenv)
	if err != nil {
		return err
	}
	log := platform.NewLogger(cfg)
	secret := os.Getenv("UNSUBSCRIBE_SECRET")
	if len(secret) < 16 {
		return errors.New("UNSUBSCRIBE_SECRET must be set (16+ chars; make env creates one)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	deps, err := platform.Connect(ctx, cfg,
		kgo.ConsumerGroup(deliver.Group), kgo.ConsumeTopics(deliver.TopicAlerts),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(),
		kgo.RequiredAcks(kgo.AllISRAcks()))
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

	sites := deliver.DefaultSites
	if raw := os.Getenv("SITES_JSON"); raw != "" {
		sites = map[string]deliver.Site{}
		if err := json.Unmarshal([]byte(raw), &sites); err != nil {
			return fmt.Errorf("SITES_JSON: %w", err)
		}
	}
	smtp := deliver.SMTP{
		Host: env("SMTP_HOST", "mailpit"), Port: intEnv("SMTP_PORT", 1025),
		Username: os.Getenv("SMTP_USER"), Password: os.Getenv("SMTP_PASSWORD"),
		From: env("EMAIL_FROM", "Mert Metin <alerts@mertmetin.ca>"),
	}
	reg := platform.NewRegistry(cfg)
	m := deliver.NewMetrics(reg)
	store := deliver.PGStore{Pool: deps.PG}
	checks := health.New(2 * time.Second)
	deps.RegisterChecks(checks)

	newSender := func() *deliver.Sender {
		return &deliver.Sender{
			Store: store, Limiter: deliver.RedisLimiter{Client: deps.Redis, Now: time.Now}, Channel: smtp,
			Render: deliver.Renderer{Sites: sites, UnsubURL: env("UNSUBSCRIBE_URL", "http://localhost:8081/v1/unsubscribe"),
				UnsubSecret: []byte(secret), Domain: env("MESSAGE_ID_DOMAIN", "mertmetin.ca")},
			DLQ:     deliver.KafkaDLQ{Client: deps.Kafka, Topic: deliver.TopicDLQ},
			Metrics: m, Log: log, Now: time.Now,
			Burst: intEnv("RATE_BURST", 5), Every: durEnv("RATE_EVERY", 10*time.Minute),
			DailyLimit: intEnv("DAILY_LIMIT", 1500), MaxPerEmail: 25, MaxAttempts: intEnv("MAX_ATTEMPTS", 6),
			Lease: 2 * time.Minute, Batch: 200,
		}
	}

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: platform.OpsMux(checks, reg), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 2)
	go func() { errc <- platform.Serve(ctx, log, srv, cfg.ShutdownTimeout) }()
	go func() { errc <- deliver.Intake(ctx, deps.Kafka, store, durEnv("COALESCE", 5*time.Second), m, log) }()
	senders := intEnv("SENDERS", 4)
	for range senders {
		go newSender().Run(ctx, time.Second)
	}
	go deliver.Reaper(ctx, store, 30*time.Second, m, log)
	go func() { // queue gauges
		for ctx.Err() == nil {
			if c, err := store.Counts(ctx); err == nil {
				for _, st := range []string{"pending", "sending", "sent", "failed"} {
					m.Queue.WithLabelValues(st).Set(float64(c[st]))
				}
			}
			select {
			case <-ctx.Done():
			case <-time.After(15 * time.Second):
			}
		}
	}()
	log.Info("delivering", "smtp", fmt.Sprintf("%s:%d", smtp.Host, smtp.Port), "auth", smtp.Username != "", "senders", senders)

	err = <-errc
	stop()
	deps.Kafka.LeaveGroup()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func intEnv(k string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(k)); err == nil && n >= 0 {
		return n
	}
	return def
}

func durEnv(k string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(k)); err == nil && d > 0 {
		return d
	}
	return def
}
