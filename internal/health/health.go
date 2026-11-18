// Package health runs dependency checks for readiness probes.
//
// Liveness (/healthz) only says the process is up. Readiness (/readyz) says it
// can do useful work: every dependency answered within the timeout. Kubernetes
// stops routing to a pod that is not ready but does not restart it, which is
// what we want when, say, a broker is briefly unreachable.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Check returns nil when the dependency is usable.
type Check func(ctx context.Context) error

// Checker runs named checks concurrently with a shared timeout.
type Checker struct {
	timeout time.Duration
	mu      sync.RWMutex
	checks  map[string]Check
}

// New returns a Checker whose checks each get at most timeout.
func New(timeout time.Duration) *Checker {
	return &Checker{timeout: timeout, checks: map[string]Check{}}
}

// Add registers a check under name, replacing any check with that name.
func (c *Checker) Add(name string, check Check) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checks[name] = check
}

// Result is the outcome of one check.
type Result struct {
	Name    string `json:"name"`
	OK      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
	Latency string `json:"latency"`
}

// Report is the outcome of all checks.
type Report struct {
	OK     bool     `json:"ok"`
	Checks []Result `json:"checks"`
}

// Run executes every check concurrently. A check that outlives the timeout
// fails even if it ignores its context.
func (c *Checker) Run(ctx context.Context) Report {
	c.mu.RLock()
	names := make([]string, 0, len(c.checks))
	for n := range c.checks {
		names = append(names, n)
	}
	checks := make(map[string]Check, len(c.checks))
	for n, ch := range c.checks {
		checks[n] = ch
	}
	c.mu.RUnlock()
	sort.Strings(names)

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	results := make([]Result, len(names))
	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			done := make(chan error, 1)
			go func() { done <- checks[name](ctx) }()
			var err error
			select {
			case err = <-done:
			case <-ctx.Done():
				err = ctx.Err()
			}
			r := Result{Name: name, OK: err == nil, Latency: time.Since(start).Round(time.Microsecond).String()}
			if err != nil {
				r.Error = err.Error()
			}
			results[i] = r
		}()
	}
	wg.Wait()

	rep := Report{OK: true, Checks: results}
	for _, r := range results {
		rep.OK = rep.OK && r.OK
	}
	return rep
}

// Handler serves the report as JSON: 200 when every check passes, 503 otherwise.
func (c *Checker) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rep := c.Run(r.Context())
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if !rep.OK {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(rep)
	})
}
