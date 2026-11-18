package detect

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGStore keeps listing states in Postgres.
type PGStore struct{ Pool *pgxpool.Pool }

func (s PGStore) Load(ctx context.Context, keys []string) (map[string]State, error) {
	out := make(map[string]State, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	rows, err := s.Pool.Query(ctx, `SELECT listing_key, version, state FROM listing_state WHERE listing_key = ANY($1)`, keys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var st State
		var raw []byte
		if err := rows.Scan(&st.Key, &st.Version, &raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &st); err != nil {
			return nil, err
		}
		out[st.Key] = st
	}
	return out, rows.Err()
}

// Save inserts new states (Version 0) and updates existing ones only if their
// stored version still equals the one loaded. One statement, one transaction:
// all or nothing.
func (s PGStore) Save(ctx context.Context, states []State) error {
	if len(states) == 0 {
		return nil
	}
	keys := make([]string, len(states))
	versions := make([]int64, len(states))
	bodies := make([]string, len(states))
	for i, st := range states {
		b, err := json.Marshal(st)
		if err != nil {
			return err
		}
		keys[i], versions[i], bodies[i] = st.Key, st.Version, string(b)
	}
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		var written int
		err := tx.QueryRow(ctx, `
			WITH input AS (
				SELECT * FROM unnest($1::text[], $2::bigint[], $3::text[]) AS t(k, v, s)
			), ins AS (
				INSERT INTO listing_state (listing_key, version, state)
				SELECT k, 1, s::jsonb FROM input WHERE v = 0
				ON CONFLICT (listing_key) DO NOTHING
				RETURNING 1
			), upd AS (
				UPDATE listing_state ls SET version = ls.version + 1, state = i.s::jsonb, updated_at = now()
				FROM input i
				WHERE ls.listing_key = i.k AND i.v > 0 AND ls.version = i.v
				RETURNING 1
			)
			SELECT (SELECT count(*) FROM ins) + (SELECT count(*) FROM upd)`,
			keys, versions, bodies).Scan(&written)
		if err != nil {
			return err
		}
		if written != len(states) {
			return ErrConflict // returning an error rolls the transaction back
		}
		return nil
	})
}
