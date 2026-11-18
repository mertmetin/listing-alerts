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
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mertmetin/listing-alerts/internal/search"
)

// Store is what the API needs from search.Store.
type Store interface {
	Create(ctx context.Context, s *search.Search) error
	Update(ctx context.Context, s *search.Search) error
	Delete(ctx context.Context, site, id string) error
	Get(ctx context.Context, site, id string) (*search.Search, error)
	List(ctx context.Context, site, user string) ([]*search.Search, error)
	SiteForKey(ctx context.Context, key string) (string, error)
}

// Server serves the API.
type Server struct {
	Store    Store
	Log      *slog.Logger
	requests *prometheus.CounterVec

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
