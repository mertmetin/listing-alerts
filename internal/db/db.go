// Package db applies the embedded SQL migrations.
package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate applies every migration not yet recorded in schema_migrations, each
// in its own transaction. An advisory lock lets many replicas start at once.
func Migrate(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	const lockID = 7_261_934 // arbitrary, constant for this app
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		return nil, err
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockID) //nolint:errcheck

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return nil, err
	}

	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(names)

	var applied []string
	for _, path := range names {
		name := strings.TrimSuffix(strings.TrimPrefix(path, "migrations/"), ".sql")
		var done bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = $1)`, name).Scan(&done); err != nil {
			return applied, err
		}
		if done {
			continue
		}
		sql, err := migrations.ReadFile(path)
		if err != nil {
			return applied, err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return applied, err
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			_ = tx.Rollback(ctx)
			return applied, fmt.Errorf("migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return applied, err
		}
		if err := tx.Commit(ctx); err != nil {
			return applied, err
		}
		applied = append(applied, name)
	}
	return applied, nil
}
