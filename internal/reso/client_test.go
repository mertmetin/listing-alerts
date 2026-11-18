package reso

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newTestClient(t *testing.T, h http.HandlerFunc) (*Client, *[]time.Duration) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	var slept []time.Duration
	c := New(srv.URL+"/odata/", "tok").WithHTTPClient(srv.Client())
	c.MinInterval = 0
	c.Sleep = func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil }
	return c, &slept
}

func TestGetSendsQueryAndAuth(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/odata/Property" {
			t.Errorf("path %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("auth %q", got)
		}
		if strings.Contains(r.URL.RawQuery, "+") {
			t.Errorf("query uses + for spaces: %s", r.URL.RawQuery)
		}
		if f := r.URL.Query().Get("$filter"); f != "City eq 'Ottawa'" {
			t.Errorf("filter %q", f)
		}
		if r.URL.Query().Get("$top") != "2" || r.URL.Query().Get("$select") != "A,B" {
			t.Errorf("query %s", r.URL.RawQuery)
		}
		w.Write([]byte(`{"value":[{"A":1},{"A":2}]}`))
	})
	rows, err := c.Get(context.Background(), "Property", Query{Filter: "City eq 'Ottawa'", Select: []string{"A", "B"}, Top: 2})
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows %v err %v", rows, err)
	}
}

func TestRetriesHonourRetryAfter(t *testing.T) {
	var calls atomic.Int32
	c, slept := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
		case 2:
			w.WriteHeader(http.StatusBadGateway)
		default:
			w.Write([]byte(`{"value":[]}`))
		}
	})
	if _, err := c.Get(context.Background(), "Property", Query{}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 || len(*slept) != 2 || (*slept)[0] != 7*time.Second {
		t.Fatalf("calls %d, slept %v", calls.Load(), *slept)
	}
}

func TestDoesNotRetryAuthErrors(t *testing.T) {
	var calls atomic.Int32
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte("token expired"))
	})
	_, err := c.Get(context.Background(), "Property", Query{})
	var se *StatusError
	if !errors.As(err, &se) || !se.Unauthorized() || calls.Load() != 1 {
		t.Fatalf("err %v, calls %d", err, calls.Load())
	}
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	var calls atomic.Int32
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	c.MaxAttempts = 3
	if _, err := c.Get(context.Background(), "Property", Query{}); err == nil || calls.Load() != 3 {
		t.Fatalf("err %v, calls %d", err, calls.Load())
	}
}

func TestQuote(t *testing.T) {
	if got := Quote("O'Neil"); got != "'O''Neil'" {
		t.Errorf("Quote = %s", got)
	}
}
