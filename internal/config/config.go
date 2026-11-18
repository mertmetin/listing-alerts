// Package config loads service settings from environment variables.
//
// Every service reads the same small set of variables, so a deployment only
// has to set them once (in .env for Docker Compose, a Secret in Kubernetes).
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Config is the shared configuration for all listing-alerts services.
type Config struct {
	// Service is the name used in logs and metrics (for example "api").
	Service string
	// HTTPAddr is where the service serves /healthz, /readyz and /metrics.
	HTTPAddr string
	// PostgresURL is a libpq-style connection string.
	PostgresURL string
	// RedisAddr is host:port of the Redis server.
	RedisAddr string
	// KafkaBrokers are the Redpanda (Kafka API) seed brokers.
	KafkaBrokers []string
	// LogLevel is debug, info, warn or error.
	LogLevel slog.Level
	// ShutdownTimeout bounds graceful shutdown.
	ShutdownTimeout time.Duration
}

// Load reads the configuration through getenv (os.Getenv in production, a map
// in tests). It returns every problem at once, not just the first.
func Load(service string, getenv func(string) string) (Config, error) {
	cfg := Config{
		Service:         service,
		HTTPAddr:        orDefault(getenv("HTTP_ADDR"), ":8080"),
		PostgresURL:     getenv("POSTGRES_URL"),
		RedisAddr:       getenv("REDIS_ADDR"),
		ShutdownTimeout: 10 * time.Second,
	}

	var errs []error
	if cfg.PostgresURL == "" {
		errs = append(errs, errors.New("POSTGRES_URL is required"))
	}
	if cfg.RedisAddr == "" {
		errs = append(errs, errors.New("REDIS_ADDR is required"))
	}

	for _, b := range strings.Split(getenv("KAFKA_BROKERS"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, b)
		}
	}
	if len(cfg.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required (comma-separated host:port)"))
	}

	if err := cfg.LogLevel.UnmarshalText([]byte(orDefault(getenv("LOG_LEVEL"), "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}

	if v := getenv("SHUTDOWN_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			errs = append(errs, fmt.Errorf("SHUTDOWN_TIMEOUT: want a positive duration like 10s, got %q", v))
		} else {
			cfg.ShutdownTimeout = d
		}
	}

	return cfg, errors.Join(errs...)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
