// Command api is the saved-search API that mertmetin.ca and hotrentals.ca
// call (see internal/api), plus /healthz, /readyz and /metrics.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mertmetin/listing-alerts/internal/api"
	"github.com/mertmetin/listing-alerts/internal/config"
	"github.com/mertmetin/listing-alerts/internal/db"
	"github.com/mertmetin/listing-alerts/internal/health"
	"github.com/mertmetin/listing-alerts/internal/hub"
	"github.com/mertmetin/listing-alerts/internal/platform"
	"github.com/mertmetin/listing-alerts/internal/search"
	"github.com/mertmetin/listing-alerts/internal/sold"
	"github.com/mertmetin/listing-alerts/internal/valuation"
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
	srvAPI := api.New(search.Store{Pool: deps.PG}, log, reg)
	srvAPI.UnsubSecret = []byte(os.Getenv("UNSUBSCRIBE_SECRET"))
	srvAPI.Hub = &hub.Hub{Pool: deps.PG, Log: log}
	srvAPI.Homes = sold.Homes{Pool: deps.PG, County: env("HOMES_COUNTY", "Ottawa")}
	if u := os.Getenv("VALUATION_URL"); u != "" {
		srvAPI.Estimator = valuation.New(strings.TrimRight(u, "/"), os.Getenv("VALUATION_API_KEY"))
	}
	srvAPI.VOWSites = map[string]bool{}
	for _, site := range strings.Split(env("VOW_SITES", "mertmetin"), ",") {
		if site = strings.TrimSpace(site); site != "" {
			srvAPI.VOWSites[site] = true
		}
	}
	if len(srvAPI.UnsubSecret) == 0 {
		log.Warn("UNSUBSCRIBE_SECRET not set: /v1/unsubscribe disabled")
	}
	srvAPI.Routes(mux)

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

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
