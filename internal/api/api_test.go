package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mertmetin/listing-alerts/internal/search"
)

type memStore struct {
	mu   sync.Mutex
	rows map[string]*search.Search
	keys map[string]string
}

func (m *memStore) Create(_ context.Context, s *search.Search) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, r := range m.rows {
		if r.Site == s.Site && r.UserID == s.UserID {
			n++
		}
	}
	if n >= search.MaxPerUser {
		return search.ErrLimit
	}
	s.ID, s.UpdatedAt = search.NewID(), time.Now()
	c := *s
	m.rows[s.ID] = &c
	return nil
}

func (m *memStore) Update(_ context.Context, s *search.Search) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rows[s.ID]
	if !ok || r.Site != s.Site || r.UserID != s.UserID {
		return search.ErrNotFound
	}
	c := *s
	m.rows[s.ID] = &c
	return nil
}

func (m *memStore) Delete(_ context.Context, site, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.rows[id]; !ok || r.Site != site {
		return search.ErrNotFound
	}
	delete(m.rows, id)
	return nil
}

func (m *memStore) Get(_ context.Context, site, id string) (*search.Search, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.rows[id]; ok && r.Site == site {
		return r, nil
	}
	return nil, search.ErrNotFound
}

func (m *memStore) List(_ context.Context, site, user string) ([]*search.Search, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*search.Search
	for _, r := range m.rows {
		if r.Site == site && r.UserID == user {
			out = append(out, r)
		}
	}
	return out, nil
}

func (m *memStore) SiteForKey(_ context.Context, key string) (string, error) { return m.keys[key], nil }

func newServer(t *testing.T) *httptest.Server {
	st := &memStore{rows: map[string]*search.Search{}, keys: map[string]string{"k-mm": "mertmetin", "k-hr": "hotrentals"}}
	s := New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry())
	mux := http.NewServeMux()
	s.Routes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func call(t *testing.T, srv *httptest.Server, method, path, key, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	b, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(b, &out)
	return resp.StatusCode, out
}

const ottawa = `{"user_id":"u1","email":"buyer@example.com","audience":"vow","name":"Orleans under 800K",
  "transaction":"sale","max_price":800000,"min_beds":3,
  "areas":[[{"lat":45.45,"lng":-75.55},{"lat":45.45,"lng":-75.45},{"lat":45.50,"lng":-75.45},{"lat":45.50,"lng":-75.55}]]}`

func TestCRUD(t *testing.T) {
	srv := newServer(t)
	code, out := call(t, srv, "POST", "/v1/searches", "k-mm", ottawa)
	if code != 201 || out["id"] == nil || out["site"] != "mertmetin" {
		t.Fatalf("create: %d %v", code, out)
	}
	id := out["id"].(string)

	if code, out = call(t, srv, "GET", "/v1/searches?user_id=u1", "k-mm", ""); code != 200 || len(out["searches"].([]any)) != 1 {
		t.Fatalf("list: %d %v", code, out)
	}
	upd := strings.Replace(ottawa, `"max_price":800000`, `"max_price":900000`, 1)
	if code, out = call(t, srv, "PUT", "/v1/searches/"+id, "k-mm", upd); code != 200 || out["max_price"].(float64) != 900000 {
		t.Fatalf("update: %d %v", code, out)
	}
	if code, _ = call(t, srv, "DELETE", "/v1/searches/"+id, "k-mm", ""); code != 204 {
		t.Fatalf("delete: %d", code)
	}
	if code, _ = call(t, srv, "GET", "/v1/searches/"+id, "k-mm", ""); code != 404 {
		t.Fatalf("get after delete: %d", code)
	}
}

func TestAuthAndIsolation(t *testing.T) {
	srv := newServer(t)
	if code, _ := call(t, srv, "POST", "/v1/searches", "", ottawa); code != 401 {
		t.Errorf("no key: %d", code)
	}
	if code, _ := call(t, srv, "POST", "/v1/searches", "wrong", ottawa); code != 401 {
		t.Errorf("bad key: %d", code)
	}
	_, out := call(t, srv, "POST", "/v1/searches", "k-mm", ottawa)
	id := out["id"].(string)
	// hotrentals cannot read, change or delete mertmetin's search.
	if code, _ := call(t, srv, "GET", "/v1/searches/"+id, "k-hr", ""); code != 404 {
		t.Errorf("cross-site get: %d", code)
	}
	if code, _ := call(t, srv, "DELETE", "/v1/searches/"+id, "k-hr", ""); code != 404 {
		t.Errorf("cross-site delete: %d", code)
	}
	// Nor can a site move a search to another user.
	other := strings.Replace(ottawa, `"user_id":"u1"`, `"user_id":"u2"`, 1)
	if code, _ := call(t, srv, "PUT", "/v1/searches/"+id, "k-mm", other); code != 404 {
		t.Errorf("reassign user: %d", code)
	}
}

func TestValidation(t *testing.T) {
	srv := newServer(t)
	cases := map[string]string{
		"swapped lat/lng": strings.ReplaceAll(strings.ReplaceAll(ottawa, `"lat":45`, `"lat":-75`), `"lng":-75`, `"lng":45`),
		"no area or city": `{"user_id":"u1","email":"a@b.c","audience":"vow","transaction":"sale"}`,
		"bad audience":    strings.Replace(ottawa, `"vow"`, `"everyone"`, 1),
		"unknown field":   strings.Replace(ottawa, `"name"`, `"nmae"`, 1),
		"min over max":    strings.Replace(ottawa, `"max_price":800000`, `"max_price":800000,"min_price":900000`, 1),
	}
	for name, body := range cases {
		if code, out := call(t, srv, "POST", "/v1/searches", "k-mm", body); code != 422 && code != 400 {
			t.Errorf("%s: %d %v", name, code, out)
		}
	}
}

func TestPerUserLimit(t *testing.T) {
	srv := newServer(t)
	for i := range search.MaxPerUser {
		if code, out := call(t, srv, "POST", "/v1/searches", "k-mm", ottawa); code != 201 {
			t.Fatalf("search %d: %d %v", i, code, out)
		}
	}
	if code, _ := call(t, srv, "POST", "/v1/searches", "k-mm", ottawa); code != 409 {
		t.Fatalf("over the limit: %d", code)
	}
}
