// Package hub is the listing database the public sites read from: full IDX
// records with photos and rooms, kept current by the IDX poller and served
// through a changes feed.
//
// The sites used to each sync PropTx themselves (mertmetin.ca hourly in PHP,
// hotrentals.ca hourly in TypeScript). Now one poller pulls once, every
// minute, and each site asks "what changed since my bookmark?".
package hub

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mertmetin/listing-alerts/internal/reso"
)

// Fields is what the hub stores: every field mertmetin.ca or hotrentals.ca
// displays. Anything not here (private remarks, showing instructions,
// commission, seller details) is never downloaded.
var Fields = []string{
	"ListingKey", "ListingId", "StandardStatus", "MlsStatus", "TransactionType", "PropertyType", "PropertySubType",
	"ListPrice", "OriginalListPrice", "ListPriceUnit", "UnparsedAddress", "StreetNumber", "StreetName", "StreetSuffix",
	"StreetDirSuffix", "UnitNumber", "PostalCode", "City", "CityRegion", "CountyOrParish", "StateOrProvince",
	"BedroomsTotal", "BedroomsAboveGrade", "BedroomsBelowGrade", "BathroomsTotalInteger", "LivingAreaRange",
	"LotWidth", "LotDepth", "LotSizeUnits", "ArchitecturalStyle", "Basement", "HeatType", "Cooling", "GarageType",
	"ParkingTotal", "ParkingSpaces", "TaxAnnualAmount", "TaxYear", "AssociationFee", "AssociationFeeIncludes", "ApproximateAge",
	"PublicRemarks", "ListOfficeName", "VirtualTourURLUnbranded", "InternetEntireListingDisplayYN",
	"InternetAddressDisplayYN", "ListingContractDate", "OriginalEntryTimestamp", "ModificationTimestamp",
	"PhotosChangeTimestamp", "CrossStreet", "InteriorFeatures", "ExteriorFeatures", "PoolFeatures", "WaterfrontYN",
	"FireplaceYN", "PetsAllowed", "Furnished", "LeaseTerm", "RentIncludes", "LaundryFeatures", "EnsuiteLaundryYN",
	"ConstructionMaterials", "Roof", "Sewer", "Water", "DirectionFaces", "PossessionType", "MinimumRentalTermMonths",
	"PropertyFeatures",
}

// Photo is one photo in three sizes (PropTx keys the sizes "-l", "-m", "-t").
type Photo struct {
	Large  string `json:"large,omitempty"`
	Medium string `json:"medium,omitempty"`
	Thumb  string `json:"thumb,omitempty"`
}

// Room is one row of PropTx's PropertyRooms.
type Room struct {
	Type     string  `json:"type"`
	Level    string  `json:"level,omitempty"`
	Length   float64 `json:"length,omitempty"` // metres
	Width    float64 `json:"width,omitempty"`
	Features string  `json:"features,omitempty"`
}

// Listing is one hub row as the changes feed serves it.
type Listing struct {
	Seq        int64          `json:"seq"`
	Key        string         `json:"key"`
	Present    bool           `json:"present"`
	ModifiedAt time.Time      `json:"modified_at"`
	Record     map[string]any `json:"record,omitempty"`
	Photos     []Photo        `json:"photos,omitempty"`
	Rooms      []Room         `json:"rooms,omitempty"`
}

// Media fetches photos and rooms for listings (from PropTx in production).
type Media interface {
	Photos(ctx context.Context, keys []string) (map[string][]Photo, error)
	Rooms(ctx context.Context, keys []string) (map[string][]Room, error)
}

// Hub stores and serves listings.
type Hub struct {
	Pool  *pgxpool.Pool
	Media Media
	Log   *slog.Logger
}

// Hash covers every stored field except the modification timestamp, so a
// timestamp-only bump is not a change but a new description is.
func Hash(r map[string]any) string {
	c := make(map[string]any, len(r))
	for k, v := range r {
		if k != "ModificationTimestamp" {
			c[k] = v
		}
	}
	b, _ := json.Marshal(c) // map keys are sorted by encoding/json
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16])
}

type stored struct {
	hash     string
	modified time.Time
	photosAt *time.Time
	present  bool
}

// Apply stores fresh records (as fetched from PropTx). Unchanged and stale
// records are skipped; photos and rooms are fetched for new listings and for
// listings whose PhotosChangeTimestamp moved. Returns how many rows changed.
func (h *Hub) Apply(ctx context.Context, rows []map[string]any) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	byKey := map[string]map[string]any{}
	keys := make([]string, 0, len(rows))
	for _, r := range rows {
		k, _ := r["ListingKey"].(string)
		if k == "" {
			continue
		}
		if _, dup := byKey[k]; !dup {
			keys = append(keys, k)
		}
		byKey[k] = r // last one wins (rows come in modification order)
	}
	have, err := h.load(ctx, keys)
	if err != nil {
		return 0, err
	}

	type change struct {
		key      string
		rec      map[string]any
		hash     string
		modified time.Time
		photosAt *time.Time
		media    bool
	}
	var changes []change
	var mediaKeys []string
	for _, k := range keys {
		r := byKey[k]
		mod := ts(r["ModificationTimestamp"])
		if mod == nil {
			continue
		}
		hash := Hash(r)
		old, ok := have[k]
		if ok && mod.Before(old.modified) {
			continue // stale re-read
		}
		if ok && old.present && old.hash == hash {
			continue // nothing the sites show changed
		}
		pa := ts(r["PhotosChangeTimestamp"])
		media := !ok || !old.present || !sameTime(pa, old.photosAt)
		if media {
			mediaKeys = append(mediaKeys, k)
		}
		changes = append(changes, change{k, r, hash, *mod, pa, media})
	}
	if len(changes) == 0 {
		return 0, nil
	}

	photos, rooms := map[string][]Photo{}, map[string][]Room{}
	if len(mediaKeys) > 0 && h.Media != nil {
		if photos, err = h.Media.Photos(ctx, mediaKeys); err != nil {
			return 0, fmt.Errorf("photos: %w", err)
		}
		if rooms, err = h.Media.Rooms(ctx, mediaKeys); err != nil {
			return 0, fmt.Errorf("rooms: %w", err)
		}
	}

	err = pgx.BeginFunc(ctx, h.Pool, func(tx pgx.Tx) error {
		for _, c := range changes {
			rec, _ := json.Marshal(c.rec)
			county, _ := c.rec["CountyOrParish"].(string)
			tx2, _ := c.rec["TransactionType"].(string)
			if c.media {
				ph, _ := json.Marshal(nonNil(photos[c.key]))
				rm, _ := json.Marshal(nonNilRooms(rooms[c.key]))
				_, err := tx.Exec(ctx, `
					INSERT INTO hub_listings (listing_key, seq, present, modified_at, content_hash, photos_at, county, transaction, record, photos, rooms)
					VALUES ($1, nextval('hub_seq'), true, $2, $3, $4, $5, $6, $7, $8, $9)
					ON CONFLICT (listing_key) DO UPDATE SET seq = nextval('hub_seq'), present = true,
						modified_at = EXCLUDED.modified_at, content_hash = EXCLUDED.content_hash, photos_at = EXCLUDED.photos_at,
						county = EXCLUDED.county, transaction = EXCLUDED.transaction, record = EXCLUDED.record,
						photos = EXCLUDED.photos, rooms = EXCLUDED.rooms, updated_at = now()`,
					c.key, c.modified, c.hash, c.photosAt, county, tx2, rec, ph, rm)
				if err != nil {
					return err
				}
				continue
			}
			_, err := tx.Exec(ctx, `
				UPDATE hub_listings SET seq = nextval('hub_seq'), present = true, modified_at = $2, content_hash = $3,
					county = $4, transaction = $5, record = $6, updated_at = now()
				WHERE listing_key = $1`, c.key, c.modified, c.hash, county, tx2, rec)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return len(changes), nil
}

// Remove marks listings as gone from IDX (sold, leased, expired, opted out),
// so the feed tells the sites to drop them.
func (h *Hub) Remove(ctx context.Context, keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	tag, err := h.Pool.Exec(ctx, `
		UPDATE hub_listings SET present = false, seq = nextval('hub_seq'), updated_at = now()
		WHERE listing_key = ANY($1) AND present`, keys)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (h *Hub) load(ctx context.Context, keys []string) (map[string]stored, error) {
	rows, err := h.Pool.Query(ctx, `SELECT listing_key, content_hash, modified_at, photos_at, present FROM hub_listings WHERE listing_key = ANY($1)`, keys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]stored{}
	for rows.Next() {
		var k string
		var s stored
		if err := rows.Scan(&k, &s.hash, &s.modified, &s.photosAt, &s.present); err != nil {
			return nil, err
		}
		out[k] = s
	}
	return out, rows.Err()
}

// Filter narrows the feed to what a site shows.
type Filter struct {
	County      string // e.g. "Ottawa"; "" = all
	Transaction string // "For Sale", "For Lease"; "" = both
}

// Changes returns up to limit listings changed after seq (gone ones too,
// without their record), oldest first.
func (h *Hub) Changes(ctx context.Context, after int64, limit int, f Filter) ([]Listing, error) {
	q := `SELECT seq, listing_key, present, modified_at, CASE WHEN present THEN record END, CASE WHEN present THEN photos END, CASE WHEN present THEN rooms END
		FROM hub_listings WHERE seq > $1`
	args := []any{after}
	if f.County != "" {
		args = append(args, f.County)
		q += fmt.Sprintf(" AND county = $%d", len(args))
	}
	if f.Transaction != "" {
		args = append(args, f.Transaction)
		q += fmt.Sprintf(" AND transaction = $%d", len(args))
	}
	args = append(args, limit)
	q += fmt.Sprintf(" ORDER BY seq LIMIT $%d", len(args))
	rows, err := h.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Listing{}
	for rows.Next() {
		var l Listing
		var rec, ph, rm []byte
		if err := rows.Scan(&l.Seq, &l.Key, &l.Present, &l.ModifiedAt, &rec, &ph, &rm); err != nil {
			return nil, err
		}
		if rec != nil {
			_ = json.Unmarshal(rec, &l.Record)
			_ = json.Unmarshal(ph, &l.Photos)
			_ = json.Unmarshal(rm, &l.Rooms)
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// Stats for /metrics and status.
func (h *Hub) Stats(ctx context.Context) (present, gone int, lastSeq int64, err error) {
	err = h.Pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE present), count(*) FILTER (WHERE NOT present), coalesce(max(seq), 0) FROM hub_listings`).Scan(&present, &gone, &lastSeq)
	return
}

// State reads and writes small hub settings (backfill progress).
func (h *Hub) State(ctx context.Context, k string) (string, error) {
	var v string
	err := h.Pool.QueryRow(ctx, `SELECT v FROM hub_state WHERE k = $1`, k).Scan(&v)
	if err == pgx.ErrNoRows {
		return "", nil
	}
	return v, err
}

func (h *Hub) SetState(ctx context.Context, k, v string) error {
	_, err := h.Pool.Exec(ctx, `INSERT INTO hub_state (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v`, k, v)
	return err
}

// Backfill loads every active IDX listing once, paging by ListingKey and
// saving progress, so the hub can be added to a poller already past its own
// baseline. Safe to restart: it resumes after the last key it stored.
func (h *Hub) Backfill(ctx context.Context, c *reso.Client, scope, filter string, page int) error {
	if done, _ := h.State(ctx, "backfill_done"); done != "" {
		return nil
	}
	last, err := h.State(ctx, "backfill_after")
	if err != nil {
		return err
	}
	total := 0
	for ctx.Err() == nil {
		conds := []string{}
		for _, s := range []string{scope, filter} {
			if s != "" {
				conds = append(conds, "("+s+")")
			}
		}
		if last != "" {
			conds = append(conds, "(ListingKey gt "+reso.Quote(last)+")")
		}
		rows, err := c.Get(ctx, "Property", reso.Query{Filter: strings.Join(conds, " and "), Select: Fields, OrderBy: "ListingKey", Top: page})
		if err != nil {
			return err
		}
		if _, err := h.Apply(ctx, rows); err != nil {
			return err
		}
		total += len(rows)
		if len(rows) > 0 {
			last, _ = rows[len(rows)-1]["ListingKey"].(string)
			if err := h.SetState(ctx, "backfill_after", last); err != nil {
				return err
			}
		}
		h.Log.Info("hub backfill", "stored", total, "after", last)
		if len(rows) < page {
			return h.SetState(ctx, "backfill_done", time.Now().UTC().Format(time.RFC3339))
		}
	}
	return ctx.Err()
}

func ts(v any) *time.Time {
	s, _ := v.(string)
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil
	}
	t = t.UTC()
	return &t
}

func sameTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

func nonNil(p []Photo) []Photo {
	if p == nil {
		return []Photo{}
	}
	return p
}

func nonNilRooms(r []Room) []Room {
	if r == nil {
		return []Room{}
	}
	return r
}
