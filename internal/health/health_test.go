package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAllPassing(t *testing.T) {
	c := New(time.Second)
	c.Add("b", func(context.Context) error { return nil })
	c.Add("a", func(context.Context) error { return nil })

	rep := c.Run(context.Background())
	if !rep.OK || len(rep.Checks) != 2 || rep.Checks[0].Name != "a" {
		t.Fatalf("report = %+v", rep)
	}
}

func TestFailureAndTimeout(t *testing.T) {
	c := New(50 * time.Millisecond)
	c.Add("ok", func(context.Context) error { return nil })
	c.Add("down", func(context.Context) error { return errors.New("connection refused") })
	// Ignores its context on purpose: Run must still return on time.
	c.Add("stuck", func(context.Context) error { time.Sleep(2 * time.Second); return nil })

	start := time.Now()
	rep := c.Run(context.Background())
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Run took %v; a stuck check must not block it", elapsed)
	}
	if rep.OK {
		t.Fatal("report OK with failing checks")
	}
	got := map[string]Result{}
	for _, r := range rep.Checks {
		got[r.Name] = r
	}
	if !got["ok"].OK || got["down"].Error != "connection refused" || got["stuck"].OK {
		t.Fatalf("results = %+v", got)
	}
}

func TestHandlerStatusCodes(t *testing.T) {
	c := New(time.Second)
	c.Add("db", func(context.Context) error { return nil })

	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	c.Add("db", func(context.Context) error { return errors.New("nope") })
	rec = httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var rep Report
	if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil || rep.OK {
		t.Fatalf("body = %s, err = %v", rec.Body, err)
	}
}
