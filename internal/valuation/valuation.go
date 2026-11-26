// Package valuation forwards home value estimates to the ottawa-home-value
// service (FastAPI, on the same Docker network). The engine adds the site
// check (VOW sites only); the valuation service owns the model.
package valuation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client calls POST {URL}/v1/estimate with the X-API-Key header.
type Client struct {
	URL  string
	Key  string
	HTTP *http.Client
}

// New returns a client with a short timeout: an estimate takes well under a second.
func New(url, key string) *Client {
	return &Client{URL: url, Key: key, HTTP: &http.Client{Timeout: 10 * time.Second}}
}

// Estimate sends the answers and returns the service's status and JSON body as is.
func (c *Client) Estimate(ctx context.Context, answers json.RawMessage) (int, json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/v1/estimate", bytes.NewReader(answers))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Key != "" {
		req.Header.Set("X-API-Key", c.Key)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("valuation: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return 0, nil, fmt.Errorf("valuation: %w", err)
	}
	if !json.Valid(body) {
		return 0, nil, fmt.Errorf("valuation: status %d, not JSON", resp.StatusCode)
	}
	return resp.StatusCode, body, nil
}
