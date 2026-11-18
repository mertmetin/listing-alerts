package search

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned for unknown or deleted searches.
var ErrNotFound = errors.New("search not found")

// ErrLimit is returned when a user already has MaxPerUser searches.
var ErrLimit = errors.New("too many saved searches for this user")

// MaxPerUser caps saved searches per user per site.
const MaxPerUser = 50

// Store persists saved searches and API keys in Postgres.
type Store struct{ Pool *pgxpool.Pool }

// NewID returns a random search ID like "ss_k3j...".
func NewID() string {
	var b [15]byte
	_, _ = rand.Read(b[:])
	return "ss_" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:]))
}

// Create stores a new search (s.ID and s.UpdatedAt are set).
func (st Store) Create(ctx context.Context, s *Search) error {
	return pgx.BeginFunc(ctx, st.Pool, func(tx pgx.Tx) error {
		// Serialise per user so the limit cannot be raced past.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))`, s.Site, s.UserID); err != nil {
			return err
		}
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM saved_searches WHERE site=$1 AND user_id=$2 AND NOT deleted`, s.Site, s.UserID).Scan(&n); err != nil {
			return err
		}
		if n >= MaxPerUser {
			return ErrLimit
		}
		s.ID = NewID()
		spec, err := json.Marshal(s)
		if err != nil {
			return err
		}
		return tx.QueryRow(ctx, `INSERT INTO saved_searches (id, site, user_id, spec) VALUES ($1, $2, $3, $4) RETURNING updated_at`,
			s.ID, s.Site, s.UserID, spec).Scan(&s.UpdatedAt)
	})
}

// Update replaces a search's spec. The site and user cannot change.
func (st Store) Update(ctx context.Context, s *Search) error {
	spec, err := json.Marshal(s)
	if err != nil {
		return err
	}
	err = st.Pool.QueryRow(ctx, `UPDATE saved_searches SET spec = $4, updated_at = now()
		WHERE id = $1 AND site = $2 AND user_id = $3 AND NOT deleted RETURNING updated_at`,
		s.ID, s.Site, s.UserID, spec).Scan(&s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

// Delete soft-deletes a search, so matcher shards see the deletion.
func (st Store) Delete(ctx context.Context, site, id string) error {
	tag, err := st.Pool.Exec(ctx, `UPDATE saved_searches SET deleted = true, updated_at = now() WHERE id = $1 AND site = $2 AND NOT deleted`, id, site)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteUser soft-deletes every search of a user on a site and returns how many.
func (st Store) DeleteUser(ctx context.Context, site, user string) (int, error) {
	tag, err := st.Pool.Exec(ctx, `UPDATE saved_searches SET deleted = true, updated_at = now() WHERE site = $1 AND user_id = $2 AND NOT deleted`, site, user)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// Get returns one live search of site.
func (st Store) Get(ctx context.Context, site, id string) (*Search, error) {
	var spec []byte
	var upd time.Time
	err := st.Pool.QueryRow(ctx, `SELECT spec, updated_at FROM saved_searches WHERE id=$1 AND site=$2 AND NOT deleted`, id, site).Scan(&spec, &upd)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return decode(spec, upd, false)
}

// List returns a user's live searches, newest first.
func (st Store) List(ctx context.Context, site, user string) ([]*Search, error) {
	rows, err := st.Pool.Query(ctx, `SELECT spec, updated_at FROM saved_searches WHERE site=$1 AND user_id=$2 AND NOT deleted ORDER BY created_at DESC`, site, user)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*Search{}
	for rows.Next() {
		var spec []byte
		var upd time.Time
		if err := rows.Scan(&spec, &upd); err != nil {
			return nil, err
		}
		s, err := decode(spec, upd, false)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Changes calls fn for every search (live or deleted) updated at or after
// since, in update order, and returns the newest updated_at seen.
func (st Store) Changes(ctx context.Context, since time.Time, fn func(*Search) error) (time.Time, error) {
	rows, err := st.Pool.Query(ctx, `SELECT spec, updated_at, deleted FROM saved_searches WHERE updated_at >= $1 ORDER BY updated_at, id`, since)
	if err != nil {
		return since, err
	}
	defer rows.Close()
	last := since
	for rows.Next() {
		var spec []byte
		var upd time.Time
		var del bool
		if err := rows.Scan(&spec, &upd, &del); err != nil {
			return last, err
		}
		s, err := decode(spec, upd, del)
		if err != nil {
			return last, err
		}
		if err := fn(s); err != nil {
			return last, err
		}
		last = upd
	}
	return last, rows.Err()
}

func decode(spec []byte, upd time.Time, deleted bool) (*Search, error) {
	var s Search
	if err := json.Unmarshal(spec, &s); err != nil {
		return nil, err
	}
	s.UpdatedAt, s.Deleted = upd.UTC(), deleted
	return &s, nil
}

// HashKey is how API keys are stored.
func HashKey(key string) []byte {
	sum := sha256.Sum256([]byte(key))
	return sum[:]
}

// NewAPIKey creates a key for site and returns it. Only its hash is kept,
// so this is the only time the key is seen.
func (st Store) NewAPIKey(ctx context.Context, site, label string) (string, error) {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	key := "la_" + site + "_" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:]))
	_, err := st.Pool.Exec(ctx, `INSERT INTO api_keys (key_hash, site, label) VALUES ($1, $2, $3)`, HashKey(key), site, label)
	return key, err
}

// SiteForKey returns the site a key belongs to, or "" if unknown or revoked.
func (st Store) SiteForKey(ctx context.Context, key string) (string, error) {
	var site string
	err := st.Pool.QueryRow(ctx, `SELECT site FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL`, HashKey(key)).Scan(&site)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return site, err
}
