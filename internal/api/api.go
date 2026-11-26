// Package api is the saved-search API that mertmetin.ca and hotrentals.ca
// call on behalf of their signed-in users.
//
//	POST   /v1/searches                 create
//	GET    /v1/searches?user_id=...     list a user's searches
//	GET    /v1/searches/{id}            read
//	PUT    /v1/searches/{id}            replace
//	DELETE /v1/searches/{id}            delete
//
// Every request carries "Authorization: Bearer <site key>". The key decides
// the site; a site only ever sees its own searches. The site vouches for its
// user: user_id, email, and audience "vow" only for users who registered,
// confirmed their email and are logged in.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mertmetin/listing-alerts/internal/hub"
	"github.com/mertmetin/listing-alerts/internal/search"
	"github.com/mertmetin/listing-alerts/internal/sold"
	"github.com/mertmetin/listing-alerts/internal/unsub"
)

// Store is what the API needs from search.Store.
type Store interface {
	Create(ctx context.Context, s *search.Search) error
	Update(ctx context.Context, s *search.Search) error
	Delete(ctx context.Context, site, id string) error
	DeleteUser(ctx context.Context, site, user string) (int, error)
	Get(ctx context.Context, site, id string) (*search.Search, error)
	List(ctx context.Context, site, user string) ([]*search.Search, error)
	SiteForKey(ctx context.Context, key string) (string, error)
}

// Server serves the API.
// Listings is the listing hub's changes feed (hub.Hub).
type Listings interface {
	Changes(ctx context.Context, after int64, limit int, f hub.Filter) ([]hub.Listing, error)
	Get(ctx context.Context, key string) (hub.Listing, bool, error)
}

// Homes looks up an address's newest MLS record (sold.Homes).
type Homes interface {
	Lookup(ctx context.Context, a sold.Address) (*sold.Home, error)
	StreetMix(ctx context.Context, a sold.Address) (*sold.StreetMix, error)
}

type Server struct {
	Store Store
	// Homes serves /v1/homes/lookup to the sites in VOWSites only: its
	// records come from the VOW feed, which only a VOW site may show (and
	// only to signed-in clients). nil disables it.
	Homes    Homes
	VOWSites map[string]bool
	// Hub serves /v1/listings/changes; nil disables it.
	Hub Listings
	// UnsubSecret signs unsubscribe links in emails (shared with delivery).
	// Empty disables /v1/unsubscribe.
	UnsubSecret []byte
	Log         *slog.Logger
	requests    *prometheus.CounterVec

	mu   sync.Mutex
	keys map[string]keyEntry // key -> site, cached briefly
}

type keyEntry struct {
	site string
	at   time.Time
}

// New returns a Server registering its metrics on reg.
func New(st Store, log *slog.Logger, reg prometheus.Registerer) *Server {
	s := &Server{Store: st, Log: log, keys: map[string]keyEntry{},
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "listing_alerts", Subsystem: "api",
			Name: "requests_total", Help: "API requests by route and status."}, []string{"route", "code"})}
	reg.MustRegister(s.requests)
	return s
}

// Routes mounts the API on mux.
func (s *Server) Routes(mux *http.ServeMux) {
	mux.Handle("POST /v1/searches", s.auth("create", s.create))
	mux.Handle("GET /v1/searches", s.auth("list", s.list))
	mux.Handle("GET /v1/searches/{id}", s.auth("get", s.get))
	mux.Handle("PUT /v1/searches/{id}", s.auth("update", s.update))
	mux.Handle("DELETE /v1/searches/{id}", s.auth("delete", s.remove))
	if s.Hub != nil {
		mux.Handle("GET /v1/listings/changes", s.auth("listing_changes", s.listingChanges))
		mux.Handle("GET /v1/listings/{key}", s.auth("listing_get", s.listingGet))
	}
	if s.Homes != nil {
		mux.Handle("GET /v1/homes/lookup", s.auth("home_lookup", s.homeLookup))
		mux.Handle("GET /v1/homes/street-mix", s.auth("home_street_mix", s.homeStreetMix))
	}
	if len(s.UnsubSecret) > 0 {
		mux.HandleFunc("GET /v1/unsubscribe", s.unsubscribePage)
		mux.HandleFunc("POST /v1/unsubscribe", s.unsubscribe)
	}
}

// unsubscribePage asks for confirmation. A GET must not unsubscribe: mail
// scanners and link previews open every URL in a message.
func (s *Server) unsubscribePage(w http.ResponseWriter, r *http.Request) {
	tok := r.URL.Query().Get("t")
	t, err := unsub.Verify(s.UnsubSecret, tok)
	if err != nil {
		page(w, http.StatusBadRequest, "This unsubscribe link is not valid.", "")
		return
	}
	what := "this saved search"
	if t.Scope == unsub.User {
		what = "all your saved-search alerts"
	}
	page(w, http.StatusOK, "Stop emails for "+what+"?",
		`<form method="post"><input type="hidden" name="t" value="`+html.EscapeString(tok)+`"><button type="submit">Unsubscribe</button></form>`)
}

// unsubscribe does it: the confirm button, or a mail client's RFC 8058
// one-click POST (body "List-Unsubscribe=One-Click", token in the URL).
func (s *Server) unsubscribe(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
	tok := r.URL.Query().Get("t")
	if tok == "" {
		_ = r.ParseForm()
		tok = r.PostForm.Get("t")
	}
	t, err := unsub.Verify(s.UnsubSecret, tok)
	if err != nil {
		s.requests.WithLabelValues("unsubscribe", http.StatusText(http.StatusBadRequest)).Inc()
		page(w, http.StatusBadRequest, "This unsubscribe link is not valid.", "")
		return
	}
	switch t.Scope {
	case unsub.Search:
		err = s.Store.Delete(r.Context(), t.Site, t.ID)
		if errors.Is(err, search.ErrNotFound) {
			err = nil // already gone: unsubscribing twice is fine
		}
	case unsub.User:
		_, err = s.Store.DeleteUser(r.Context(), t.Site, t.ID)
	}
	if err != nil {
		s.Log.Error("unsubscribe", "err", err)
		s.requests.WithLabelValues("unsubscribe", http.StatusText(http.StatusInternalServerError)).Inc()
		page(w, http.StatusInternalServerError, "Something went wrong. Please try the link again in a minute.", "")
		return
	}
	s.requests.WithLabelValues("unsubscribe", http.StatusText(http.StatusOK)).Inc()
	s.Log.Info("unsubscribed", "scope", t.Scope, "site", t.Site)
	page(w, http.StatusOK, "You are unsubscribed.", "<p>You will not get these alerts any more.</p>")
}

func page(w http.ResponseWriter, code int, title, body string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	fmt.Fprintf(w, `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Alerts</title>
<style>body{font:16px/1.5 -apple-system,Segoe UI,Roboto,sans-serif;max-width:32rem;margin:15vh auto;padding:0 16px;color:#1d1d1b;background:#f4f2ee}
button{font:inherit;font-weight:600;padding:.6rem 1.2rem;border:0;border-radius:8px;background:#1d1d1b;color:#fff;cursor:pointer}</style></head>
<body><h1 style="font-size:1.4rem">%s</h1>%s</body></html>`, html.EscapeString(title), body)
}

type handler func(w http.ResponseWriter, r *http.Request, site string) (int, any)

func (s *Server) auth(route string, h handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code, body := s.serve(r, h)
		s.requests.WithLabelValues(route, http.StatusText(code)).Inc()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(code)
		if body != nil {
			_ = json.NewEncoder(w).Encode(body)
		}
	})
}

func (s *Server) serve(r *http.Request, h handler) (int, any) {
	key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || key == "" {
		return http.StatusUnauthorized, errBody("missing API key")
	}
	site, err := s.site(r.Context(), key)
	if err != nil {
		s.Log.Error("key lookup", "err", err)
		return http.StatusInternalServerError, errBody("internal error")
	}
	if site == "" {
		return http.StatusUnauthorized, errBody("invalid API key")
	}
	r.Body = http.MaxBytesReader(nil, r.Body, 256<<10)
	return h(nil, r, site)
}

// site resolves a key, caching for a minute so revocations apply quickly
// without a database read per request.
func (s *Server) site(ctx context.Context, key string) (string, error) {
	s.mu.Lock()
	e, ok := s.keys[key]
	s.mu.Unlock()
	if ok && time.Since(e.at) < time.Minute {
		return e.site, nil
	}
	site, err := s.Store.SiteForKey(ctx, key)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	if len(s.keys) > 10000 {
		clear(s.keys)
	}
	s.keys[key] = keyEntry{site, time.Now()}
	s.mu.Unlock()
	return site, nil
}

func errBody(msg string) map[string]string { return map[string]string{"error": msg} }

func (s *Server) decode(r *http.Request) (*search.Search, int, any) {
	var in search.Search
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		return nil, http.StatusBadRequest, errBody("invalid JSON: " + err.Error())
	}
	if err := in.Validate(); err != nil {
		return nil, http.StatusUnprocessableEntity, map[string]any{"error": "invalid search", "details": strings.Split(err.Error(), "\n")}
	}
	return &in, 0, nil
}

func (s *Server) create(_ http.ResponseWriter, r *http.Request, site string) (int, any) {
	in, code, body := s.decode(r)
	if in == nil {
		return code, body
	}
	in.Site, in.Deleted = site, false
	switch err := s.Store.Create(r.Context(), in); {
	case errors.Is(err, search.ErrLimit):
		return http.StatusConflict, errBody(err.Error())
	case err != nil:
		s.Log.Error("create search", "err", err)
		return http.StatusInternalServerError, errBody("internal error")
	}
	return http.StatusCreated, in
}

// listingChanges: GET /v1/listings/changes?after=<seq>&limit=<n>&county=Ottawa&transaction=sale|lease
//
// Listings (full IDX records with photos and rooms) changed after seq, oldest
// first. Gone listings come with present=false and no record. Sites keep the
// last seq they stored and ask again; "more" says another page is ready now.
func (s *Server) listingChanges(_ http.ResponseWriter, r *http.Request, _ string) (int, any) {
	q := r.URL.Query()
	after, err := strconv.ParseInt(q.Get("after"), 10, 64)
	if q.Get("after") != "" && (err != nil || after < 0) {
		return http.StatusBadRequest, errBody("after must be a non-negative number")
	}
	limit := 500
	if v := q.Get("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit < 1 || limit > 1000 {
			return http.StatusBadRequest, errBody("limit must be 1 to 1000")
		}
	}
	f := hub.Filter{County: q.Get("county")}
	switch q.Get("transaction") {
	case "", "all":
	case "sale":
		f.Transaction = "For Sale"
	case "lease":
		f.Transaction = "For Lease"
	default:
		return http.StatusBadRequest, errBody(`transaction must be "sale" or "lease"`)
	}
	rows, err := s.Hub.Changes(r.Context(), after, limit, f)
	if err != nil {
		s.Log.Error("listing changes", "err", err)
		return http.StatusInternalServerError, errBody("internal error")
	}
	next := after
	if n := len(rows); n > 0 {
		next = rows[n-1].Seq
	}
	return http.StatusOK, map[string]any{"listings": rows, "next": next, "more": len(rows) == limit}
}

func (s *Server) list(_ http.ResponseWriter, r *http.Request, site string) (int, any) {
	user := r.URL.Query().Get("user_id")
	if user == "" {
		return http.StatusBadRequest, errBody("user_id is required")
	}
	out, err := s.Store.List(r.Context(), site, user)
	if err != nil {
		s.Log.Error("list searches", "err", err)
		return http.StatusInternalServerError, errBody("internal error")
	}
	if out == nil {
		out = []*search.Search{} // always a JSON array, never null
	}
	return http.StatusOK, map[string]any{"searches": out}
}

func (s *Server) get(_ http.ResponseWriter, r *http.Request, site string) (int, any) {
	out, err := s.Store.Get(r.Context(), site, r.PathValue("id"))
	switch {
	case errors.Is(err, search.ErrNotFound):
		return http.StatusNotFound, errBody(err.Error())
	case err != nil:
		s.Log.Error("get search", "err", err)
		return http.StatusInternalServerError, errBody("internal error")
	}
	return http.StatusOK, out
}

func (s *Server) update(_ http.ResponseWriter, r *http.Request, site string) (int, any) {
	in, code, body := s.decode(r)
	if in == nil {
		return code, body
	}
	in.ID, in.Site, in.Deleted = r.PathValue("id"), site, false
	switch err := s.Store.Update(r.Context(), in); {
	case errors.Is(err, search.ErrNotFound):
		return http.StatusNotFound, errBody("search not found for this user")
	case err != nil:
		s.Log.Error("update search", "err", err)
		return http.StatusInternalServerError, errBody("internal error")
	}
	return http.StatusOK, in
}

func (s *Server) remove(_ http.ResponseWriter, r *http.Request, site string) (int, any) {
	switch err := s.Store.Delete(r.Context(), site, r.PathValue("id")); {
	case errors.Is(err, search.ErrNotFound):
		return http.StatusNotFound, errBody(err.Error())
	case err != nil:
		s.Log.Error("delete search", "err", err)
		return http.StatusInternalServerError, errBody("internal error")
	}
	return http.StatusNoContent, nil
}

// listingGet: GET /v1/listings/{key}
//
// One listing as the hub holds it now (full record, photos, rooms). For pages
// that need a single listing's photos when the board's own API is slow or down.
func (s *Server) listingGet(_ http.ResponseWriter, r *http.Request, _ string) (int, any) {
	key := r.PathValue("key")
	if !listingKeyRe.MatchString(key) {
		return http.StatusBadRequest, errBody("invalid listing key")
	}
	l, found, err := s.Hub.Get(r.Context(), key)
	if err != nil {
		s.Log.Error("listing get", "err", err)
		return http.StatusInternalServerError, errBody("internal error")
	}
	if !found {
		return http.StatusNotFound, errBody("no such listing")
	}
	return http.StatusOK, l
}

var listingKeyRe = regexp.MustCompile(`^[A-Za-z0-9-]{1,40}$`)

// homeLookup: GET /v1/homes/lookup?number=12&street=Main%20St&unit=4
//
// What MLS knows about one address (type, rooms, size, age, systems...),
// from its newest sold or listed record. Never prices. {"home": null} when
// there's no match, or when a building has several units and no unit was
// given.
func (s *Server) homeLookup(_ http.ResponseWriter, r *http.Request, site string) (int, any) {
	if !s.VOWSites[site] {
		return http.StatusForbidden, errBody("home lookups are for VOW sites only")
	}
	q := r.URL.Query()
	a := sold.Address{Number: strings.TrimSpace(q.Get("number")), Street: strings.TrimSpace(q.Get("street")), Unit: strings.TrimSpace(q.Get("unit"))}
	if a.Number == "" || a.Street == "" || len(a.Number) > 12 || len(a.Street) > 80 || len(a.Unit) > 12 {
		return http.StatusBadRequest, errBody("number and street are required")
	}
	h, err := s.Homes.Lookup(r.Context(), a)
	if err != nil {
		s.Log.Error("home lookup", "err", err)
		return http.StatusInternalServerError, errBody("internal error")
	}
	return http.StatusOK, map[string]any{"home": h}
}

// homeStreetMix: GET /v1/homes/street-mix?number=12&street=Main%20St
//
// Home types among the nearest homes on the same street, the address itself
// left out: {"mix": {"types": {"Detached": 11, "Att/Row/Townhouse": 2}, "n": 13}}.
// Lets a site guess a visitor's home type from the neighbours.
func (s *Server) homeStreetMix(_ http.ResponseWriter, r *http.Request, site string) (int, any) {
	if !s.VOWSites[site] {
		return http.StatusForbidden, errBody("home lookups are for VOW sites only")
	}
	q := r.URL.Query()
	a := sold.Address{Number: strings.TrimSpace(q.Get("number")), Street: strings.TrimSpace(q.Get("street"))}
	if a.Number == "" || a.Street == "" || len(a.Number) > 12 || len(a.Street) > 80 {
		return http.StatusBadRequest, errBody("number and street are required")
	}
	mix, err := s.Homes.StreetMix(r.Context(), a)
	if err != nil {
		s.Log.Error("street mix", "err", err)
		return http.StatusInternalServerError, errBody("internal error")
	}
	return http.StatusOK, map[string]any{"mix": mix}
}
