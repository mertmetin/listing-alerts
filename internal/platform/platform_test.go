package platform

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mertmetin/listing-alerts/internal/config"
	"github.com/mertmetin/listing-alerts/internal/health"
)

func TestOpsMux(t *testing.T) {
	cfg := config.Config{Service: "test"}
	checks := health.New(1e9)
	checks.Add("dep", func(context.Context) error { return errors.New("down") })
	srv := httptest.NewServer(OpsMux(checks, NewRegistry(cfg)))
	defer srv.Close()

	cases := []struct {
		path     string
		status   int
		contains string
	}{
		{"/healthz", 200, "ok"},
		{"/readyz", 503, `"error":"down"`},
		{"/metrics", 200, `listing_alerts_build_info{service="test",version="dev"} 1`},
	}
	for _, tc := range cases {
		resp, err := http.Get(srv.URL + tc.path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != tc.status || !strings.Contains(string(body), tc.contains) {
			t.Errorf("%s: status %d, body %q; want %d containing %q", tc.path, resp.StatusCode, body, tc.status, tc.contains)
		}
	}
}
