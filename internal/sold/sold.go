// Package sold keeps an archive of closed for-sale listings from the PropTx
// VOW feed, for home-value research (the ottawa-home-value project).
//
// The alert pipeline only needs each listing's current state, and its
// baseline skipped closed history. The archive is separate: on first run it
// pages through every sale closed since Since (by ListingKey), then follows
// ModificationTimestamp so new sales and corrections land within the poll
// interval. Same client, same exact keyset cursor as the pollers.
package sold

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mertmetin/listing-alerts/internal/geocode"
	"github.com/mertmetin/listing-alerts/internal/hub"
	"github.com/mertmetin/listing-alerts/internal/listing"
	"github.com/mertmetin/listing-alerts/internal/reso"
)

// Fields is what the archive downloads: everything the hub stores (the
// property description) plus the sale itself.
var Fields = append(append([]string{}, hub.Fields...),
	"ClosePrice", "CloseDate", "PurchaseContractDate", "Latitude", "Longitude")

// ClosedForSale limits the feed to closed sales. Closed covers Sold and the
// rare Deal Fell Through after closing; mls_status tells them apart.
const ClosedForSale = "StandardStatus eq 'Closed' and TransactionType eq 'For Sale'"

// Scope builds the full feed filter: the poller's property-type scope, closed
// for-sale listings, and the area (e.g. "CountyOrParish eq 'Ottawa'").
func Scope(propertyScope, area string) string {
	var parts []string
	for _, p := range []string{propertyScope, ClosedForSale, area} {
		if p = strings.TrimSpace(p); p != "" {
			parts = append(parts, "("+p+")")
		}
	}
	return strings.Join(parts, " and ")
}

// Since is the baseline filter for history from day d on.
func Since(d time.Time) string { return "CloseDate ge " + d.Format("2006-01-02") }

// Locator geocodes a listing (geocode.Geocoder in production).
type Locator interface {
	Locate(l listing.Listing) (*geocode.Fix, error)
}

// Feed is the part of reso.Source the archive uses.
type Feed interface {
	Fetch(ctx context.Context, cur reso.Cursor, now time.Time) (reso.Page, error)
}

// Archive stores sold listings.
type Archive struct {
	Pool     *pgxpool.Pool
	Feed     Feed
	Geo      Locator // optional
	Log      *slog.Logger
	Interval time.Duration // wait between polls once caught up
	Now      func() time.Time
}

// Row is one archived sale, ready to store.
type Row struct {
	Key, County, MlsStatus, SubType string
	ClosePrice                      *float64
	CloseDate, ContractDate         *time.Time
	ModifiedAt                      time.Time
	Record                          map[string]any
	Lat, Lng                        *float64
	Precision                       string
}

// Prepare turns raw records into rows, geocoding each one. Records without a
// key or timestamp are skipped.
func (a *Archive) Prepare(rows []map[string]any) []Row {
	out := make([]Row, 0, len(rows))
	for _, r := range rows {
		l, err := listing.FromRESO(r)
		if err != nil {
			continue
		}
		row := Row{
			Key: l.Key, County: l.County, MlsStatus: l.MlsStatus, SubType: strings.TrimSpace(l.PropertySubType),
			ClosePrice: l.ClosePrice, CloseDate: day(r["CloseDate"]), ContractDate: day(r["PurchaseContractDate"]),
			ModifiedAt: l.ModifiedAt, Record: r,
		}
		if l.Latitude != nil && l.Longitude != nil && *l.Latitude != 0 {
			row.Lat, row.Lng, row.Precision = l.Latitude, l.Longitude, "feed"
		} else if a.Geo != nil {
			if fix, err := a.Geo.Locate(l); err == nil && fix != nil {
				lat, lng := fix.Point.Lat, fix.Point.Lng
				row.Lat, row.Lng, row.Precision = &lat, &lng, string(fix.Precision)
			}
		}
		out = append(out, row)
	}
	return out
}

// Commit upserts rows and saves the cursor in one transaction. An older
// version never replaces a newer one, so overlap re-reads are harmless.
func (a *Archive) Commit(ctx context.Context, rows []Row, cur reso.Cursor) (int, error) {
	c, err := json.Marshal(cur)
	if err != nil {
		return 0, err
	}
	changed := 0
	err = pgx.BeginFunc(ctx, a.Pool, func(tx pgx.Tx) error {
		for _, r := range rows {
			rec, err := json.Marshal(r.Record)
			if err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, `
				INSERT INTO sold_listings (listing_key, county, mls_status, property_sub_type, close_price, close_date,
					contract_date, modified_at, record, lat, lng, geo_precision)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
				ON CONFLICT (listing_key) DO UPDATE SET county = EXCLUDED.county, mls_status = EXCLUDED.mls_status,
					property_sub_type = EXCLUDED.property_sub_type, close_price = EXCLUDED.close_price,
					close_date = EXCLUDED.close_date, contract_date = EXCLUDED.contract_date,
					modified_at = EXCLUDED.modified_at, record = EXCLUDED.record,
					lat = COALESCE(EXCLUDED.lat, sold_listings.lat), lng = COALESCE(EXCLUDED.lng, sold_listings.lng),
					geo_precision = COALESCE(EXCLUDED.geo_precision, sold_listings.geo_precision), updated_at = now()
				WHERE sold_listings.modified_at < EXCLUDED.modified_at`,
				r.Key, r.County, r.MlsStatus, r.SubType, r.ClosePrice, r.CloseDate, r.ContractDate,
				r.ModifiedAt, rec, r.Lat, r.Lng, nullable(r.Precision))
			if err != nil {
				return fmt.Errorf("store %s: %w", r.Key, err)
			}
			changed += int(tag.RowsAffected())
		}
		_, err := tx.Exec(ctx, `INSERT INTO sold_state (k, v) VALUES ('cursor', $1)
			ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v`, string(c))
		return err
	})
	return changed, err
}

// Cursor loads the saved position (zero when none: start the baseline).
func (a *Archive) Cursor(ctx context.Context) (reso.Cursor, error) {
	var v string
	err := a.Pool.QueryRow(ctx, `SELECT v FROM sold_state WHERE k = 'cursor'`).Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		return reso.Cursor{}, nil
	}
	if err != nil {
		return reso.Cursor{}, err
	}
	var c reso.Cursor
	return c, json.Unmarshal([]byte(v), &c)
}

// Step fetches and stores one page. more is true when the next page should be
// fetched right away.
func (a *Archive) Step(ctx context.Context) (more bool, err error) {
	cur, err := a.Cursor(ctx)
	if err != nil {
		return false, fmt.Errorf("load cursor: %w", err)
	}
	page, err := a.Feed.Fetch(ctx, cur, a.Now())
	if err != nil {
		return false, fmt.Errorf("fetch: %w", err)
	}
	rows := a.Prepare(page.Records)
	n, err := a.Commit(ctx, rows, page.Next)
	if err != nil {
		return false, err
	}
	if len(page.Records) > 0 || page.Next.Phase != cur.Phase {
		a.Log.Info("sold archive", "phase", cur.Phase, "read", len(page.Records), "stored", n,
			"located", located(rows), "next_phase", page.Next.Phase)
	}
	return page.More, nil
}

// Run polls until ctx ends. Errors are logged and retried after a pause.
func (a *Archive) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		more, err := a.Step(ctx)
		wait := a.Interval
		switch {
		case err != nil:
			var se *reso.StatusError
			if errors.As(err, &se) && se.Unauthorized() {
				return err // a bad token will not fix itself
			}
			a.Log.Error("sold archive step failed", "err", err)
			wait = 30 * time.Second
		case more:
			continue
		}
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
	return ctx.Err()
}

func located(rows []Row) int {
	n := 0
	for _, r := range rows {
		if r.Lat != nil {
			n++
		}
	}
	return n
}

func day(v any) *time.Time {
	s, _ := v.(string)
	if len(s) < 10 {
		return nil
	}
	t, err := time.Parse("2006-01-02", s[:10])
	if err != nil {
		return nil
	}
	return &t
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
