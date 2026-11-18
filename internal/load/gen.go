// Package load generates synthetic listing activity for load and chaos
// tests, seeds synthetic saved searches, and records exactly what it
// produced so an auditor can prove no alert was lost.
package load

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/listing"
	"github.com/mertmetin/listing-alerts/internal/search"
)

// Site is the saved-search site used by load tests, so their searches and
// alerts never mix with real ones.
const Site = "loadtest"

type slot struct {
	l      listing.Listing
	public bool
}

// Gen produces a stream of listing changes: listings come on the market,
// drop their price, go conditional, sell or lease, expire, come back, and
// sometimes change nothing but a timestamp (which the pipeline must ignore).
type Gen struct {
	syn   *search.Synth
	run   string
	slots []*slot
	next  int // next listing number
}

// NewGen returns a generator over pool listings at a time.
func NewGen(run string, seed uint64, pool int) *Gen {
	return &Gen{syn: search.NewSynthCanada(seed), run: run, slots: make([]*slot, pool)}
}

// KeyPrefix marks this run's listings.
func KeyPrefix(run string) string { return "LG" + run + "-" }

func (g *Gen) fresh(now time.Time) *slot {
	g.next++
	city, pt := g.syn.Place()
	rng := g.syn.Rand()
	lease := rng.IntN(3) == 0
	tx, price := "For Sale", math.Round(250000+1400000*rng.Float64()*rng.Float64())
	if lease {
		tx, price = "For Lease", math.Round(1200+3500*rng.Float64())
	}
	beds, baths := 1+rng.IntN(5), 1+rng.IntN(3)
	lat, lng := pt.Lat, pt.Lng
	s := &slot{public: rng.IntN(10) < 8, l: listing.Listing{
		Key: fmt.Sprintf("%s%08d", KeyPrefix(g.run), g.next), TransactionType: tx,
		StandardStatus: "Active", MlsStatus: "New", PropertyType: "Residential Freehold",
		PropertySubType: g.syn.SubType(), ListPrice: &price, Bedrooms: &beds, Bathrooms: &baths,
		StreetNumber: fmt.Sprint(1 + rng.IntN(2000)), StreetName: "Synthetic", StreetSuffix: "Street",
		City: city, County: city, Province: "ON", Latitude: &lat, Longitude: &lng,
		AddressDisplay: true, ListingDisplay: true, ListOffice: "LOADTEST REALTY", ModifiedAt: now,
	}}
	return s
}

// Next returns the changes for one step: a VOW observation and, for public
// listings, the matching IDX one (IDX drops closed listings instead).
func (g *Gen) Next(now time.Time) []event.Change {
	rng := g.syn.Rand()
	i := rng.IntN(len(g.slots))
	s := g.slots[i]
	if s == nil {
		s = g.fresh(now)
		g.slots[i] = s
		return g.emit(s, now, false)
	}
	l := &s.l
	if !now.After(l.ModifiedAt) {
		now = l.ModifiedAt.Add(time.Millisecond) // strictly newer per listing
	}
	l.ModifiedAt = now
	p := rng.Float64()
	switch l.StandardStatus {
	case "Active":
		switch {
		case p < 0.45:
			price := math.Round(*l.ListPrice * (0.95 + 0.04*rng.Float64()))
			l.ListPrice, l.MlsStatus = &price, "Price Change"
		case p < 0.55:
			price := math.Round(*l.ListPrice * (1.01 + 0.04*rng.Float64()))
			l.ListPrice, l.MlsStatus = &price, "Price Change"
		case p < 0.67:
			l.StandardStatus, l.MlsStatus = "Active Under Contract", "Sold Conditional"
		case p < 0.80:
			g.close(l, rng.Float64())
		case p < 0.90:
			l.StandardStatus, l.MlsStatus = "Expired", "Expired"
		default:
			// Timestamp-only change: must produce no alert.
		}
	case "Active Under Contract":
		if p < 0.5 {
			g.close(l, rng.Float64())
		} else {
			l.StandardStatus, l.MlsStatus = "Active", "Deal Fell Through"
		}
	default: // Closed or Expired
		if p < 0.3 {
			l.StandardStatus, l.MlsStatus = "Active", "Extension"
			l.ClosePrice = nil
		} else {
			s = g.fresh(now) // a new listing takes the slot
			g.slots[i] = s
		}
	}
	return g.emit(s, now, false)
}

func (g *Gen) close(l *listing.Listing, f float64) {
	cp := math.Round(*l.ListPrice * (0.95 + 0.1*f))
	l.StandardStatus, l.ClosePrice = "Closed", &cp
	l.MlsStatus = "Sold"
	if l.TransactionType != "For Sale" {
		l.MlsStatus = "Leased"
	}
}

func (g *Gen) emit(s *slot, now time.Time, backfill bool) []event.Change {
	out := []event.Change{event.NewObserved(event.SourceVOW, s.l, now, backfill)}
	if s.public {
		switch s.l.StandardStatus {
		case "Active", "Active Under Contract":
			idx := s.l
			idx.ClosePrice = nil
			out = append(out, event.NewObserved(event.SourceIDX, idx, now, backfill))
		default:
			out = append(out, event.NewRemoved(event.SourceIDX, s.l.Key, "x", now, now))
		}
	}
	return out
}

// SeedSearches inserts n synthetic saved searches for run (site loadtest)
// straight into Postgres with COPY, and returns them.
func SeedSearches(ctx context.Context, pool *pgxpool.Pool, run string, seed uint64, n int) ([]*search.Search, error) {
	syn := search.NewSynthCanada(seed)
	out := make([]*search.Search, n)
	rows := make([][]any, n)
	now := time.Now().UTC()
	for i := range n {
		s := syn.Search(i)
		s.ID = fmt.Sprintf("lt-%s-%07d", run, i)
		s.Site, s.UserID = Site, fmt.Sprintf("lt-%s-u%06d", run, i/3)
		s.Email = s.UserID + "@example.com"
		s.UpdatedAt = now
		out[i] = s
		spec, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		rows[i] = []any{s.ID, s.Site, s.UserID, spec, now}
	}
	_, err := pool.CopyFrom(ctx, pgx.Identifier{"saved_searches"}, []string{"id", "site", "user_id", "spec", "updated_at"}, pgx.CopyFromRows(rows))
	return out, err
}

// Log writes every produced change (gzip JSON lines) for the auditor.
type Log struct {
	f  *os.File
	zw *gzip.Writer
	en *json.Encoder
}

// CreateLog opens path for writing.
func CreateLog(path string) (*Log, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	zw := gzip.NewWriter(f)
	return &Log{f: f, zw: zw, en: json.NewEncoder(zw)}, nil
}

// Write appends changes.
func (l *Log) Write(cs []event.Change) error {
	for _, c := range cs {
		if err := l.en.Encode(c); err != nil {
			return err
		}
	}
	return nil
}

// Close flushes the log.
func (l *Log) Close() error {
	if err := l.zw.Close(); err != nil {
		return err
	}
	return l.f.Close()
}

// ReadLog calls fn for every change in the log, in order.
func ReadLog(path string, fn func(event.Change) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(zr)
	for {
		var c event.Change
		if err := dec.Decode(&c); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		if err := fn(c); err != nil {
			return err
		}
	}
}
