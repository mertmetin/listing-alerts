package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mertmetin/listing-alerts/internal/hub"
	"github.com/mertmetin/listing-alerts/internal/search"
	"github.com/mertmetin/listing-alerts/internal/sold"
	"github.com/mertmetin/listing-alerts/internal/unsub"
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

func (m *memStore) DeleteUser(_ context.Context, site, user string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for id, r := range m.rows {
		if r.Site == site && r.UserID == user {
			delete(m.rows, id)
			n++
		}
	}
	return n, nil
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
	s.UnsubSecret = []byte("test-secret")
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

func TestUnsubscribe(t *testing.T) {
	srv := newServer(t)
	_, out := call(t, srv, "POST", "/v1/searches", "k-mm", ottawa)
	id := out["id"].(string)
	_, _ = call(t, srv, "POST", "/v1/searches", "k-mm", ottawa)
	secret := []byte("test-secret")
	one := unsub.Sign(secret, unsub.Target{Scope: unsub.Search, Site: "mertmetin", ID: id})

	// A GET (mail scanner, link preview) only shows the confirmation page.
	resp, _ := http.Get(srv.URL + "/v1/unsubscribe?t=" + url.QueryEscape(one))
	resp.Body.Close()
	if _, l := call(t, srv, "GET", "/v1/searches?user_id=u1", "k-mm", ""); resp.StatusCode != 200 || len(l["searches"].([]any)) != 2 {
		t.Fatalf("GET unsubscribed something (status %d)", resp.StatusCode)
	}
	// RFC 8058 one-click POST.
	resp, _ = http.Post(srv.URL+"/v1/unsubscribe?t="+url.QueryEscape(one), "application/x-www-form-urlencoded", strings.NewReader("List-Unsubscribe=One-Click"))
	resp.Body.Close()
	if _, l := call(t, srv, "GET", "/v1/searches?user_id=u1", "k-mm", ""); resp.StatusCode != 200 || len(l["searches"].([]any)) != 1 {
		t.Fatalf("one-click: status %d, %v left", resp.StatusCode, l)
	}
	// Twice is fine.
	resp, _ = http.Post(srv.URL+"/v1/unsubscribe?t="+url.QueryEscape(one), "application/x-www-form-urlencoded", nil)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("second unsubscribe: %d", resp.StatusCode)
	}
	// All of a user's searches, via the confirm form.
	all := unsub.Sign(secret, unsub.Target{Scope: unsub.User, Site: "mertmetin", ID: "u1"})
	resp, _ = http.PostForm(srv.URL+"/v1/unsubscribe", url.Values{"t": {all}})
	resp.Body.Close()
	if _, l := call(t, srv, "GET", "/v1/searches?user_id=u1", "k-mm", ""); len(l["searches"].([]any)) != 0 {
		t.Fatalf("user unsubscribe left %v", l)
	}
	// Forged or foreign-secret tokens do nothing.
	bad := unsub.Sign([]byte("other"), unsub.Target{Scope: unsub.User, Site: "hotrentals", ID: "x"})
	resp, _ = http.PostForm(srv.URL+"/v1/unsubscribe", url.Values{"t": {bad}})
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("forged token: %d", resp.StatusCode)
	}
}

type fakeListings struct {
	got struct {
		after int64
		limit int
		f     hub.Filter
	}
}

func (f *fakeListings) Changes(_ context.Context, after int64, limit int, filt hub.Filter) ([]hub.Listing, error) {
	f.got.after, f.got.limit, f.got.f = after, limit, filt
	return []hub.Listing{{Seq: after + 1, Key: "X1", Present: true}, {Seq: after + 2, Key: "X2"}}, nil
}

func (f *fakeListings) Get(_ context.Context, key string) (hub.Listing, bool, error) {
	if key != "X1" {
		return hub.Listing{}, false, nil
	}
	return hub.Listing{Seq: 7, Key: "X1", Present: true, Photos: []hub.Photo{{Large: "https://img/1.jpg"}}}, true, nil
}

func TestListingGet(t *testing.T) {
	st := &memStore{rows: map[string]*search.Search{}, keys: map[string]string{"k-mm": "mertmetin"}}
	s := New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry())
	s.Hub = &fakeListings{}
	mux := http.NewServeMux()
	s.Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	if code, _ := call(t, srv, "GET", "/v1/listings/X1", "", ""); code != 401 {
		t.Fatalf("no key: %d", code)
	}
	code, out := call(t, srv, "GET", "/v1/listings/X1", "k-mm", "")
	if code != 200 || out["key"] != "X1" || len(out["photos"].([]any)) != 1 {
		t.Fatalf("get: %d %v", code, out)
	}
	if code, _ := call(t, srv, "GET", "/v1/listings/X9", "k-mm", ""); code != 404 {
		t.Fatalf("unknown: %d", code)
	}
	if code, _ := call(t, srv, "GET", "/v1/listings/bad%20key", "k-mm", ""); code != 400 {
		t.Fatalf("bad key: %d", code)
	}
	// The feed route still wins over {key}.
	if code, out := call(t, srv, "GET", "/v1/listings/changes?after=0", "k-mm", ""); code != 200 || out["listings"] == nil {
		t.Fatalf("changes shadowed: %d %v", code, out)
	}
}

func TestListingChangesFeed(t *testing.T) {
	st := &memStore{rows: map[string]*search.Search{}, keys: map[string]string{"k-mm": "mertmetin"}}
	fl := &fakeListings{}
	s := New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry())
	s.Hub = fl
	mux := http.NewServeMux()
	s.Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	if code, _ := call(t, srv, "GET", "/v1/listings/changes", "", ""); code != 401 {
		t.Fatalf("no key: %d", code)
	}
	code, out := call(t, srv, "GET", "/v1/listings/changes?after=40&limit=2&county=Ottawa&transaction=lease", "k-mm", "")
	if code != 200 || out["next"].(float64) != 42 || out["more"] != true || len(out["listings"].([]any)) != 2 {
		t.Fatalf("feed: %d %v", code, out)
	}
	if fl.got.after != 40 || fl.got.limit != 2 || fl.got.f.County != "Ottawa" || fl.got.f.Transaction != "For Lease" {
		t.Fatalf("passed %+v", fl.got)
	}
	for _, bad := range []string{"after=-1", "limit=5000", "transaction=rent"} {
		if code, _ := call(t, srv, "GET", "/v1/listings/changes?"+bad, "k-mm", ""); code != 400 {
			t.Errorf("%s: %d, want 400", bad, code)
		}
	}
}

type fakeHomes struct{ got sold.Address }

func (f *fakeHomes) Lookup(_ context.Context, a sold.Address) (*sold.Home, error) {
	f.got = a
	if a.Number == "12" {
		return &sold.Home{Key: "X1", Source: "sold", Date: "2025-03", Facts: map[string]any{"PropertySubType": "Detached"}}, nil
	}
	return nil, nil
}

func (f *fakeHomes) StreetMix(_ context.Context, a sold.Address) (*sold.StreetMix, error) {
	f.got = a
	return &sold.StreetMix{Types: map[string]int{"Detached": 9, "Semi-Detached": 1}, N: 10}, nil
}

func (f *fakeHomes) Neighbours(_ context.Context, a sold.Address, limit int) ([]sold.Neighbour, error) {
	f.got = a
	return []sold.Neighbour{{Gap: 2, Facts: map[string]any{"PropertySubType": "Detached"}}}, nil
}

func TestHomeLookupVOWOnly(t *testing.T) {
	st := &memStore{rows: map[string]*search.Search{}, keys: map[string]string{"k-mm": "mertmetin", "k-hr": "hotrentals"}}
	s := New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry())
	homes := &fakeHomes{}
	s.Homes = homes
	s.VOWSites = map[string]bool{"mertmetin": true}
	mux := http.NewServeMux()
	s.Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	if code, _ := call(t, srv, "GET", "/v1/homes/lookup?number=12&street=Main", "", ""); code != 401 {
		t.Fatalf("no key: %d", code)
	}
	if code, _ := call(t, srv, "GET", "/v1/homes/lookup?number=12&street=Main", "k-hr", ""); code != 403 {
		t.Fatalf("IDX-only site got VOW data: %d", code)
	}
	if code, _ := call(t, srv, "GET", "/v1/homes/lookup?street=Main", "k-mm", ""); code != 400 {
		t.Fatalf("missing number: %d", code)
	}
	code, out := call(t, srv, "GET", "/v1/homes/lookup?number=12&street=Main%20St&unit=%204", "k-mm", "")
	home, _ := out["home"].(map[string]any)
	if code != 200 || home["key"] != "X1" || homes.got.Unit != "4" || homes.got.Street != "Main St" {
		t.Fatalf("lookup: %d %v %+v", code, out, homes.got)
	}
	if code, out := call(t, srv, "GET", "/v1/homes/lookup?number=99&street=Main", "k-mm", ""); code != 200 || out["home"] != nil {
		t.Fatalf("no match: %d %v", code, out)
	}
}

func TestStreetMix(t *testing.T) {
	st := &memStore{rows: map[string]*search.Search{}, keys: map[string]string{"k-mm": "mertmetin", "k-hr": "hotrentals"}}
	s := New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry())
	s.Homes = &fakeHomes{}
	s.VOWSites = map[string]bool{"mertmetin": true}
	mux := http.NewServeMux()
	s.Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	if code, _ := call(t, srv, "GET", "/v1/homes/street-mix?number=12&street=Main", "k-hr", ""); code != 403 {
		t.Fatalf("IDX-only site: %d", code)
	}
	code, out := call(t, srv, "GET", "/v1/homes/street-mix?number=12&street=Main", "k-mm", "")
	mix, _ := out["mix"].(map[string]any)
	if code != 200 || mix["n"] != 10.0 {
		t.Fatalf("mix: %d %v", code, out)
	}
}

func TestNeighbours(t *testing.T) {
	st := &memStore{rows: map[string]*search.Search{}, keys: map[string]string{"k-mm": "mertmetin", "k-hr": "hotrentals"}}
	s := New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry())
	s.Homes = &fakeHomes{}
	s.VOWSites = map[string]bool{"mertmetin": true}
	mux := http.NewServeMux()
	s.Routes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	if code, _ := call(t, srv, "GET", "/v1/homes/neighbours?number=12&street=Main", "k-hr", ""); code != 403 {
		t.Fatalf("IDX-only site: %d", code)
	}
	code, out := call(t, srv, "GET", "/v1/homes/neighbours?number=12&street=Main", "k-mm", "")
	homes, _ := out["homes"].([]any)
	if code != 200 || len(homes) != 1 {
		t.Fatalf("neighbours: %d %v", code, out)
	}
}
