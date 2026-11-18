// Command api is the saved-search API that mertmetin.ca and hotrentals.ca
// call (see internal/api), plus /healthz, /readyz and /metrics.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mertmetin/listing-alerts/internal/api"
	"github.com/mertmetin/listing-alerts/internal/config"
	"github.com/mertmetin/listing-alerts/internal/db"
	"github.com/mertmetin/listing-alerts/internal/health"
	"github.com/mertmetin/listing-alerts/internal/platform"
	"github.com/mertmetin/listing-alerts/internal/search"
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

	for i := 0; ; i++ {
		if _, err = db.Migrate(ctx, deps.PG); err == nil || i == 30 || ctx.Err() != nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return err
	}

	checks := health.New(2 * time.Second)
	deps.RegisterChecks(checks)
	reg := platform.NewRegistry(cfg)
	mux := platform.OpsMux(checks, reg)
	api.New(search.Store{Pool: deps.PG}, log, reg).Routes(mux)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
	}
	if err := platform.Serve(ctx, log, srv, cfg.ShutdownTimeout); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
