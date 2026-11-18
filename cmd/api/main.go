// Command api is the saved-search API that mertmetin.ca and hotrentals.ca
// call. In milestone 0 it only proves the platform: it connects to every
// backing service and serves health and metrics. Endpoints arrive in M3.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mertmetin/listing-alerts/internal/config"
	"github.com/mertmetin/listing-alerts/internal/health"
	"github.com/mertmetin/listing-alerts/internal/platform"
)

func main() {
	if err := run(); err != nil {
		// The logger may not exist yet, so write plainly.
		os.Stderr.WriteString("api: " + err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("api", os.Getenv)
	if err != nil {
		return err
	}
	log := platform.NewLogger(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	deps, err := platform.Connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer deps.Close()

	checks := health.New(2 * time.Second)
	deps.RegisterChecks(checks)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           platform.OpsMux(checks, platform.NewRegistry(cfg)),
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := platform.Serve(ctx, log, srv, cfg.ShutdownTimeout); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
