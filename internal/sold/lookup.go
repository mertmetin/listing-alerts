package sold

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mertmetin/listing-alerts/internal/geocode"
)

// FactFields are the only fields a home lookup returns: what the home is,
// never what it sold or listed for. Sold records are VOW data, so the site
// shows them only to signed-in clients.
var FactFields = []string{
	"PropertySubType", "BedroomsAboveGrade", "BedroomsBelowGrade", "BedroomsTotal", "BathroomsTotalInteger",
	"LivingAreaRange", "ApproximateAge", "Basement", "CoveredSpaces", "GarageType", "HeatSource", "HeatType",
	"Cooling", "FoundationDetails", "FireplaceYN", "PoolFeatures", "Sewer", "Water", "KitchensTotal",
	"TaxAnnualAmount", "TaxYear", "AssociationFee", "ArchitecturalStyle", "CityRegion",
	"StreetNumber", "StreetName", "StreetSuffix", "UnitNumber",
}

// Home is the newest MLS record of one address.
type Home struct {
	Key    string         `json:"key"`
	Source string         `json:"source"` // "sold" (archive) or "listed" (on the market now)
	Date   string         `json:"date"`   // YYYY-MM of the sale or listing
	Facts  map[string]any `json:"facts"`
}

// Address is what a visitor typed, split up.
type Address struct {
	Number, Street, Unit string
}

var unitRe = regexp.MustCompile(`[^0-9A-Za-z]`)

func normUnit(u string) string {
	return strings.TrimLeft(strings.ToUpper(unitRe.ReplaceAllString(u, "")), "0")
}

type candidate struct {
	key, source, unit string
	at                time.Time
	rec               map[string]any
}

// Lookup finds the newest record for an address in county: sold archive and
// homes listed now. It returns nil when there is none, or when the address
// is a building with several units and no unit was given (it won't guess).
func Lookup(ctx context.Context, pool *pgxpool.Pool, county string, a Address) (*Home, error) {
	num := strings.TrimSpace(a.Number)
	core := geocode.StreetCore(a.Street)
	if num == "" || core == "" {
		return nil, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT listing_key, 'sold', coalesce(contract_date, close_date, modified_at::date)::timestamptz, record
		  FROM sold_listings WHERE county = $1 AND record->>'StreetNumber' = $2
		UNION ALL
		SELECT listing_key, 'listed', modified_at, record
		  FROM hub_listings WHERE present AND county = $1 AND transaction = 'For Sale' AND record->>'StreetNumber' = $2`,
		county, num)
	if err != nil {
		return nil, fmt.Errorf("lookup: %w", err)
	}
	defer rows.Close()
	var cands []candidate
	for rows.Next() {
		var c candidate
		var raw []byte
		if err := rows.Scan(&c.key, &c.source, &c.at, &raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &c.rec); err != nil {
			continue
		}
		name, _ := c.rec["StreetName"].(string)
		if geocode.StreetCore(name) != core {
			continue
		}
		u, _ := c.rec["UnitNumber"].(string)
		c.unit = normUnit(u)
		cands = append(cands, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if want := normUnit(a.Unit); want != "" {
		kept := cands[:0]
		for _, c := range cands {
			if c.unit == want {
				kept = append(kept, c)
			}
		}
		cands = kept
	} else {
		units := map[string]bool{}
		for _, c := range cands {
			units[c.unit] = true
		}
		if len(units) > 1 {
			return nil, nil // a building with several units: don't guess which one
		}
	}
	if len(cands) == 0 {
		return nil, nil
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].at.After(cands[j].at) })
	best := cands[0]
	facts := map[string]any{}
	for _, f := range FactFields {
		if v, ok := best.rec[f]; ok && v != nil && v != "" {
			facts[f] = v
		}
	}
	return &Home{Key: best.key, Source: best.source, Date: best.at.Format("2006-01"), Facts: facts}, nil
}

// Homes serves lookups for the API.
type Homes struct {
	Pool   *pgxpool.Pool
	County string
}

// Lookup implements api.Homes.
func (h Homes) Lookup(ctx context.Context, a Address) (*Home, error) {
	return Lookup(ctx, h.Pool, h.County, a)
}

// StreetMix counts home types among the nearest records on the same street
// (sold in the archive or listed now), leaving out the address itself. It is
// an aggregate of the neighbours, never one home's record, so a site can use
// it to guess "most homes near you are townhouses" for anyone.
type StreetMix struct {
	Types map[string]int `json:"types"` // PropertySubType -> homes
	N     int            `json:"n"`
}

// MixNearest is how many of the closest house numbers count.
const MixNearest = 15

func (h Homes) StreetMix(ctx context.Context, a Address) (*StreetMix, error) {
	num, err := strconv.Atoi(strings.TrimLeft(strings.TrimSpace(a.Number), "0"))
	core := geocode.StreetCore(a.Street)
	if err != nil || core == "" {
		return &StreetMix{Types: map[string]int{}}, nil
	}
	first := strings.ToUpper(strings.Fields(core)[0])
	rows, err := h.Pool.Query(ctx, `
		SELECT DISTINCT ON (addr) addr, num, street, type FROM (
			SELECT record->>'StreetNumber' || ' ' || coalesce(record->>'UnitNumber', '') AS addr, record->>'StreetNumber' AS num,
			       record->>'StreetName' AS street, coalesce(record->>'PropertySubType', '') AS type, modified_at AS at
			  FROM sold_listings WHERE county = $1 AND upper(record->>'StreetName') LIKE $2
			UNION ALL
			SELECT record->>'StreetNumber' || ' ' || coalesce(record->>'UnitNumber', ''), record->>'StreetNumber',
			       record->>'StreetName', coalesce(record->>'PropertySubType', ''), modified_at
			  FROM hub_listings WHERE present AND county = $1 AND transaction = 'For Sale' AND upper(record->>'StreetName') LIKE $2
		) x ORDER BY addr, at DESC`, h.County, "%"+first+"%")
	if err != nil {
		return nil, fmt.Errorf("street mix: %w", err)
	}
	defer rows.Close()
	type near struct {
		dist int
		typ  string
	}
	var homes []near
	for rows.Next() {
		var addr, n, street, typ string
		if err := rows.Scan(&addr, &n, &street, &typ); err != nil {
			return nil, err
		}
		if geocode.StreetCore(street) != core || typ == "" {
			continue
		}
		v, err := strconv.Atoi(strings.TrimLeft(strings.TrimSpace(n), "0"))
		if err != nil || v == num {
			continue // the address itself never counts
		}
		d := v - num
		if d < 0 {
			d = -d
		}
		homes = append(homes, near{d, strings.TrimSpace(typ)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(homes, func(i, j int) bool { return homes[i].dist < homes[j].dist })
	if len(homes) > MixNearest {
		homes = homes[:MixNearest]
	}
	mix := &StreetMix{Types: map[string]int{}, N: len(homes)}
	for _, h := range homes {
		mix.Types[h.typ]++
	}
	return mix, nil
}
