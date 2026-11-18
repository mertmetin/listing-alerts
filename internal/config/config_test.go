package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadValid(t *testing.T) {
	cfg, err := Load("api", env(map[string]string{
		"POSTGRES_URL":     "postgres://u:p@localhost:5432/db",
		"REDIS_ADDR":       "localhost:6379",
		"KAFKA_BROKERS":    " a:9092, b:9092 ,,",
		"LOG_LEVEL":        "debug",
		"SHUTDOWN_TIMEOUT": "3s",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want default :8080", cfg.HTTPAddr)
	}
	if got := strings.Join(cfg.KafkaBrokers, "|"); got != "a:9092|b:9092" {
		t.Errorf("KafkaBrokers = %q", got)
	}
	if cfg.LogLevel != slog.LevelDebug {
		t.Errorf("LogLevel = %v", cfg.LogLevel)
	}
	if cfg.ShutdownTimeout != 3*time.Second {
		t.Errorf("ShutdownTimeout = %v", cfg.ShutdownTimeout)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	_, err := Load("api", env(map[string]string{"LOG_LEVEL": "loud", "SHUTDOWN_TIMEOUT": "-1s"}))
	if err == nil {
		t.Fatal("want error")
	}
	for _, want := range []string{"POSTGRES_URL", "REDIS_ADDR", "KAFKA_BROKERS", "LOG_LEVEL", "SHUTDOWN_TIMEOUT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}
