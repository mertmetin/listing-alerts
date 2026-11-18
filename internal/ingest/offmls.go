package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"time"

	"github.com/mertmetin/listing-alerts/internal/listing"
)

// OffMLSFiles reads hotrentals.ca's off-MLS rentals: the ones added by hand
// (private-listings.json) and approved landlord submissions
// (submitted-listings.json). Keys match the site's: "P-" + id.
type OffMLSFiles struct {
	Paths []string
}

type offMLSRecord struct {
	ID        string   `json:"id"`
	Active    *bool    `json:"active"`
	ExpiresAt *string  `json:"expires_at"`
	City      string   `json:"city"`
	County    string   `json:"county"`
	Area      string   `json:"area"`
	Address   string   `json:"address"`
	Postal    string   `json:"postal"`
	Price     *float64 `json:"price"`
	Beds      *int     `json:"beds"`
	Baths     *int     `json:"baths"`
	Type      string   `json:"type"`
	Parking   *float64 `json:"parking"`
	ManagedBy string   `json:"managed_by"`
	ListedAt  *string  `json:"listed_at"`
}

// All returns the live off-MLS listings. A listing marked inactive, expired or
// deleted from the file is simply absent, and the poller emits Removed for it.
// ModifiedAt is the file's modification time: deterministic across retries,
// so re-published events keep their IDs.
func (o OffMLSFiles) All(_ context.Context, now time.Time) ([]listing.Listing, error) {
	var out []listing.Listing
	found := 0
	for _, path := range o.Paths {
		st, err := os.Stat(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue // the site writes submitted-listings.json only once one exists
		}
		found++
		if err != nil {
			return nil, err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var recs []offMLSRecord
		if err := json.Unmarshal(b, &recs); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		mod := st.ModTime().UTC().Truncate(time.Second)
		for i, r := range recs {
			if r.Active != nil && !*r.Active {
				continue
			}
			if r.ExpiresAt != nil && *r.ExpiresAt != "" {
				if t, err := time.Parse(time.RFC3339Nano, *r.ExpiresAt); err == nil && t.Before(now) {
					continue
				}
			}
			if r.ID == "" || r.City == "" || r.Price == nil {
				return nil, fmt.Errorf("%s: listing #%d needs id, city and price", path, i+1)
			}
			l := listing.Listing{
				Key: "P-" + r.ID, TransactionType: "For Lease", StandardStatus: "Active", MlsStatus: "New",
				PropertyType: "Off-MLS", PropertySubType: r.Type,
				ListPrice: r.Price, Bedrooms: r.Beds, Bathrooms: r.Baths, Parking: r.Parking,
				Address: r.Address, City: r.City, Community: r.Area, County: r.County, Province: "ON", PostalCode: r.Postal,
				AddressDisplay: r.Address != "", ListingDisplay: true, ListOffice: r.ManagedBy,
				ModifiedAt: mod,
			}
			if r.ListedAt != nil {
				if t, err := time.Parse(time.RFC3339Nano, *r.ListedAt); err == nil {
					t = t.UTC()
					l.ListedAt = &t
				}
			}
			out = append(out, l)
		}
	}
	if found == 0 && len(o.Paths) > 0 {
		// A moved or unmounted folder must not look like "every listing was removed".
		return nil, fmt.Errorf("none of the off-MLS files exist: %v", o.Paths)
	}
	return out, nil
}

// SnapshotPoller polls a source that can only list everything it has.
type SnapshotPoller struct {
	Source   string
	Files    OffMLSFiles
	Store    Store
	Pub      Publisher
	Log      *slog.Logger
	Metrics  *Metrics
	Now      func() time.Time
	Interval time.Duration
}

// Step reads the full snapshot, publishes what changed or disappeared, and commits.
func (p *SnapshotPoller) Step(ctx context.Context) error {
	now := p.Now()
	all, err := p.Files.All(ctx, now)
	if err != nil {
		return err
	}
	keys := make([]string, len(all))
	keep := make(map[string]bool, len(all))
	for i, l := range all {
		keys[i] = l.Key
		keep[l.Key] = true
	}
	prev, err := p.Store.Versions(ctx, p.Source, keys)
	if err != nil {
		return err
	}
	changes, snaps, skips := Diff(p.Source, all, prev, now, false)
	present, err := p.Store.PresentKeys(ctx, p.Source)
	if err != nil {
		return err
	}
	removed, rsnaps := Removals(p.Source, present, keep, now)

	if err := p.Pub.Publish(ctx, append(changes, removed...)); err != nil {
		return err
	}
	if err := p.Store.Commit(ctx, p.Source, nil, append(snaps, rsnaps...)); err != nil {
		return err
	}
	p.Metrics.Fetched.WithLabelValues(p.Source).Add(float64(len(all)))
	p.Metrics.count(p.Source, len(changes), len(removed), skips)
	if len(changes)+len(removed) > 0 {
		p.Log.Info("step", "listings", len(all), "published", len(changes), "removed", len(removed))
	}
	return nil
}

// Run polls until ctx is cancelled.
func (p *SnapshotPoller) Run(ctx context.Context) error {
	for {
		if err := p.Step(ctx); err != nil && ctx.Err() == nil {
			p.Metrics.Errors.WithLabelValues(p.Source).Inc()
			p.Log.Error("snapshot step failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(p.Interval):
		}
	}
}
