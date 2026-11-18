// Package reso is a small client for RESO Web API (OData) servers such as
// PropTx (query.ampre.ca): bearer auth, client-side rate limiting, and retries
// on 429/5xx that honour Retry-After.
package reso

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Client fetches OData collections. The zero value is not usable; use New.
type Client struct {
	baseURL   string
	token     string
	userAgent string
	http      *http.Client

	// MaxAttempts per request (default 5).
	MaxAttempts int
	// MinInterval between requests from this client (default 250ms), so a
	// backfill cannot hammer the server.
	MinInterval time.Duration
	// Sleep is replaceable in tests.
	Sleep func(ctx context.Context, d time.Duration) error

	mu   sync.Mutex
	next time.Time
}

// StatusError is a non-retryable (or retries-exhausted) HTTP failure.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string { return fmt.Sprintf("reso: HTTP %d: %s", e.Code, e.Body) }

// Unauthorized reports whether the server rejected the token.
func (e *StatusError) Unauthorized() bool { return e.Code == 401 || e.Code == 403 }

// New returns a client for the OData service root baseURL.
func New(baseURL, token string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		// PropTx's firewall rejects some default client signatures.
		userAgent:   "listing-alerts/1 (+https://github.com/mertmetin/listing-alerts)",
		http:        &http.Client{Timeout: 60 * time.Second},
		MaxAttempts: 5,
		MinInterval: 250 * time.Millisecond,
		Sleep:       sleep,
	}
}

// WithUserAgent overrides the User-Agent header.
func (c *Client) WithUserAgent(ua string) *Client { c.userAgent = ua; return c }

// WithHTTPClient overrides the HTTP client (tests point it at httptest).
func (c *Client) WithHTTPClient(h *http.Client) *Client { c.http = h; return c }

// Query is an OData query.
type Query struct {
	Filter  string
	Select  []string
	OrderBy string
	Top     int
}

func (q Query) encode() string {
	v := url.Values{}
	if q.Filter != "" {
		v.Set("$filter", q.Filter)
	}
	if len(q.Select) > 0 {
		v.Set("$select", strings.Join(q.Select, ","))
	}
	if q.OrderBy != "" {
		v.Set("$orderby", q.OrderBy)
	}
	if q.Top > 0 {
		v.Set("$top", strconv.Itoa(q.Top))
	}
	// OData servers expect %20, not the + that form encoding produces.
	return strings.ReplaceAll(v.Encode(), "+", "%20")
}

// Get returns the "value" array of resource for q.
func (c *Client) Get(ctx context.Context, resource string, q Query) ([]map[string]any, error) {
	u := c.baseURL + "/" + resource + "?" + q.encode()
	var last error
	for attempt := 1; attempt <= c.MaxAttempts; attempt++ {
		if err := c.wait(ctx); err != nil {
			return nil, err
		}
		rows, retryAfter, err := c.do(ctx, u)
		if err == nil {
			return rows, nil
		}
		last = err
		var se *StatusError
		retryable := !errors.As(err, &se) || se.Code == http.StatusTooManyRequests || se.Code >= 500
		if !retryable || ctx.Err() != nil || attempt == c.MaxAttempts {
			break
		}
		backoff := retryAfter
		if backoff == 0 {
			// Exponential with full jitter: 0.5s, 1s, 2s, 4s ... capped at 30s.
			ceil := min(time.Duration(500<<(attempt-1))*time.Millisecond, 30*time.Second)
			backoff = time.Duration(rand.Int64N(int64(ceil))) + ceil/2
		}
		if err := c.Sleep(ctx, backoff); err != nil {
			return nil, err
		}
	}
	return nil, last
}

func (c *Client) do(ctx context.Context, u string) ([]map[string]any, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err // network errors are retryable
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return nil, retryAfter(resp.Header.Get("Retry-After")), &StatusError{Code: resp.StatusCode, Body: strings.TrimSpace(string(body))}
	}
	var page struct {
		Value []map[string]any `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, 0, fmt.Errorf("reso: decode %s: %w", resp.Request.URL.Path, err)
	}
	return page.Value, 0, nil
}

// wait enforces MinInterval between requests.
func (c *Client) wait(ctx context.Context) error {
	c.mu.Lock()
	now := time.Now()
	at := c.next
	if at.Before(now) {
		at = now
	}
	c.next = at.Add(c.MinInterval)
	c.mu.Unlock()
	if d := time.Until(at); d > 0 {
		return c.Sleep(ctx, d)
	}
	return nil
}

func retryAfter(h string) time.Duration {
	if h == "" {
		return 0
	}
	if s, err := strconv.Atoi(h); err == nil && s >= 0 {
		return min(time.Duration(s)*time.Second, 5*time.Minute)
	}
	if t, err := http.ParseTime(h); err == nil {
		return min(max(time.Until(t), 0), 5*time.Minute)
	}
	return 0
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Quote returns s as an OData string literal.
func Quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// Timestamp formats t as an OData DateTimeOffset literal.
func Timestamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
