// Package deliver turns alerts into emails (and, later, SMS or push):
// a durable queue in Postgres, per-user rate limits in Redis, retries with
// backoff, and a dead-letter topic for what cannot be delivered.
package deliver

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mertmetin/listing-alerts/internal/event"
)

// Item is a claimed delivery.
type Item struct {
	AlertID  string
	Attempts int
	Alert    event.Alert
}

// Store is the delivery queue.
type Store interface {
	// Enqueue inserts alerts as pending at notBefore; duplicates are ignored.
	// It returns how many were new.
	Enqueue(ctx context.Context, alerts []event.Alert, notBefore time.Time) (int, error)
	// Claim leases up to n due pending items, oldest first, never splitting a
	// user's due items across concurrent claimers.
	Claim(ctx context.Context, n int, lease time.Duration) ([]Item, error)
	MarkSent(ctx context.Context, ids []string, messageID string) error
	// Retry puts items back to pending at at, counting an attempt and the error.
	Retry(ctx context.Context, ids []string, at time.Time, cause string) error
	// Defer puts items back to pending at at without counting an attempt
	// (rate limited, not failed).
	Defer(ctx context.Context, ids []string, at time.Time) error
	MarkFailed(ctx context.Context, ids []string, cause string) error
	// Reap returns expired leases to pending: their sender died mid-send.
	Reap(ctx context.Context) (int, error)
}

// DedupKey identifies "this search, this listing, this happened, at this price".
func DedupKey(a event.Alert) string {
	p := func(v *float64) string {
		if v == nil {
			return ""
		}
		return fmt.Sprintf("%.0f", *v)
	}
	return a.SearchID + "|" + a.ListingKey + "|" + string(a.Type) + "|" + p(a.Price) + "|" + p(a.ClosePrice)
}

// PGStore is the Postgres Store.
type PGStore struct{ Pool *pgxpool.Pool }

func (s PGStore) Enqueue(ctx context.Context, alerts []event.Alert, notBefore time.Time) (int, error) {
	if len(alerts) == 0 {
		return 0, nil
	}
	n := len(alerts)
	ids, keys, sites, users, bodies := make([]string, n), make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	for i, a := range alerts {
		b, err := json.Marshal(a)
		if err != nil {
			return 0, err
		}
		ids[i], keys[i], sites[i], users[i], bodies[i] = a.ID, DedupKey(a), a.Site, a.UserID, string(b)
	}
	tag, err := s.Pool.Exec(ctx, `
		INSERT INTO deliveries (alert_id, dedup_key, site, user_id, alert, next_at)
		SELECT i, k, s, u, b::jsonb, $6 FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[]) AS t(i, k, s, u, b)
		ON CONFLICT DO NOTHING`, ids, keys, sites, users, bodies, notBefore)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s PGStore) Claim(ctx context.Context, n int, lease time.Duration) ([]Item, error) {
	rows, err := s.Pool.Query(ctx, `
		UPDATE deliveries d SET status = 'sending', lease_until = now() + $2::interval
		FROM (SELECT alert_id FROM deliveries p WHERE status = 'pending' AND next_at <= now()
		        -- One claimer per user at a time: concurrent senders skip a
		        -- user another sender is claiming, so a user's due alerts
		        -- become one email instead of one per sender.
		        AND pg_try_advisory_xact_lock(hashtext(p.site), hashtext(p.user_id))
		      ORDER BY next_at LIMIT $1 FOR UPDATE SKIP LOCKED) due
		WHERE d.alert_id = due.alert_id
		RETURNING d.alert_id, d.attempts, d.alert`, n, lease.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Item
	for rows.Next() {
		var it Item
		var raw []byte
		if err := rows.Scan(&it.AlertID, &it.Attempts, &raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &it.Alert); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

func (s PGStore) MarkSent(ctx context.Context, ids []string, messageID string) error {
	_, err := s.Pool.Exec(ctx, `UPDATE deliveries SET status='sent', sent_at=now(), message_id=$2, lease_until=NULL, attempts=attempts+1
		WHERE alert_id = ANY($1)`, ids, messageID)
	return err
}

func (s PGStore) Retry(ctx context.Context, ids []string, at time.Time, cause string) error {
	_, err := s.Pool.Exec(ctx, `UPDATE deliveries SET status='pending', next_at=$2, last_error=$3, lease_until=NULL, attempts=attempts+1
		WHERE alert_id = ANY($1)`, ids, at, cause)
	return err
}

func (s PGStore) Defer(ctx context.Context, ids []string, at time.Time) error {
	_, err := s.Pool.Exec(ctx, `UPDATE deliveries SET status='pending', next_at=$2, lease_until=NULL WHERE alert_id = ANY($1)`, ids, at)
	return err
}

func (s PGStore) MarkFailed(ctx context.Context, ids []string, cause string) error {
	_, err := s.Pool.Exec(ctx, `UPDATE deliveries SET status='failed', last_error=$2, lease_until=NULL, attempts=attempts+1
		WHERE alert_id = ANY($1)`, ids, cause)
	return err
}

func (s PGStore) Reap(ctx context.Context) (int, error) {
	tag, err := s.Pool.Exec(ctx, `UPDATE deliveries SET status='pending', next_at=now(), lease_until=NULL,
		last_error='lease expired: the sender stopped mid-send, so this may be a resend'
		WHERE status='sending' AND lease_until < now()`)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// Counts returns the queue size by status (for metrics).
func (s PGStore) Counts(ctx context.Context) (map[string]int, error) {
	rows, err := s.Pool.Query(ctx, `SELECT status, count(*) FROM deliveries WHERE status <> 'sent' OR sent_at > now() - interval '1 day' GROUP BY 1`)
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (struct {
		k string
		v int
	}, error) {
		var x struct {
			k string
			v int
		}
		err := r.Scan(&x.k, &x.v)
		return x, err
	})
	m := map[string]int{}
	for _, x := range out {
		m[x.k] = x.v
	}
	return m, err
}
