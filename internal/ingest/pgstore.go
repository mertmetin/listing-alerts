package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mertmetin/listing-alerts/internal/reso"
)

// PGStore is the Postgres Store.
type PGStore struct{ Pool *pgxpool.Pool }

func (s PGStore) Cursor(ctx context.Context, source string) (reso.Cursor, error) {
	var c reso.Cursor
	err := s.Pool.QueryRow(ctx, `SELECT cursor FROM ingest_cursors WHERE source = $1`, source).Scan(&c)
	if errors.Is(err, pgx.ErrNoRows) {
		return reso.Cursor{}, nil
	}
	return c, err
}

func (s PGStore) Versions(ctx context.Context, source string, keys []string) (map[string]Version, error) {
	out := make(map[string]Version, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	rows, err := s.Pool.Query(ctx, `SELECT listing_key, content_hash, modified_at, present
		FROM source_listings WHERE source = $1 AND listing_key = ANY($2)`, source, keys)
	if err != nil {
		return nil, err
	}
	return collect(rows, out)
}

func (s PGStore) PresentKeys(ctx context.Context, source string) (map[string]Version, error) {
	rows, err := s.Pool.Query(ctx, `SELECT listing_key, content_hash, modified_at, present
		FROM source_listings WHERE source = $1 AND present`, source)
	if err != nil {
		return nil, err
	}
	return collect(rows, map[string]Version{})
}

func collect(rows pgx.Rows, out map[string]Version) (map[string]Version, error) {
	defer rows.Close()
	for rows.Next() {
		var k string
		var v Version
		if err := rows.Scan(&k, &v.Hash, &v.ModifiedAt, &v.Present); err != nil {
			return nil, err
		}
		v.ModifiedAt = v.ModifiedAt.UTC()
		out[k] = v
	}
	return out, rows.Err()
}

// Commit upserts snapshots and the cursor in one transaction. Removals keep
// the last known listing body (listing = COALESCE(new, old)).
func (s PGStore) Commit(ctx context.Context, source string, cur *reso.Cursor, snaps []Snapshot) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		if len(snaps) > 0 {
			n := len(snaps)
			keys, hashes, present := make([]string, n), make([]string, n), make([]bool, n)
			mods := make([]time.Time, n)
			bodies := make([]*string, n)
			for i, sn := range snaps {
				keys[i], hashes[i], mods[i], present[i] = sn.Key, sn.Hash, sn.ModifiedAt, sn.Present
				if sn.Listing != nil {
					b, err := json.Marshal(sn.Listing)
					if err != nil {
						return err
					}
					str := string(b)
					bodies[i] = &str
				}
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO source_listings (source, listing_key, content_hash, modified_at, present, listing)
				SELECT $1, k, h, m, p, b::jsonb
				FROM unnest($2::text[], $3::text[], $4::timestamptz[], $5::bool[], $6::text[]) AS t(k, h, m, p, b)
				ON CONFLICT (source, listing_key) DO UPDATE SET
					content_hash = EXCLUDED.content_hash,
					modified_at  = EXCLUDED.modified_at,
					present      = EXCLUDED.present,
					listing      = COALESCE(EXCLUDED.listing, source_listings.listing),
					updated_at   = now()`,
				source, keys, hashes, mods, present, bodies); err != nil {
				return err
			}
		}
		if cur != nil {
			if _, err := tx.Exec(ctx, `
				INSERT INTO ingest_cursors (source, cursor) VALUES ($1, $2)
				ON CONFLICT (source) DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()`,
				source, cur); err != nil {
				return err
			}
		}
		return nil
	})
}
