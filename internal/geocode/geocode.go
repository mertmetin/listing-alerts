// Package geocode places listings on the map from municipal open-data
// address points (the same database hotrentals.ca builds), so addresses are
// never sent to a third-party geocoding service.
//
// Lookup: same house number and street (types and directions dropped, so
// "St. Laurent Blvd" = "ST LAURENT") in the address-point sources for the
// listing's county, preferring the same postal area (FSA). Fallback: the
// centre of the listing's FSA. Otherwise no coordinates, and the listing is
// matched by city name only.
package geocode

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/text/unicode/norm"
	_ "modernc.org/sqlite" // pure Go: works in the static image

	"github.com/mertmetin/listing-alerts/internal/geo"
	"github.com/mertmetin/listing-alerts/internal/listing"
)

// Precision of a fix.
type Precision string

const (
	Address Precision = "address" // matched a civic address point
	FSA     Precision = "fsa"     // centre of the postal area (a few km)
)

// Fix is a geocoding result.
type Fix struct {
	Point     geo.Point `json:"point"`
	Precision Precision `json:"precision"`
}

// countySources maps PropTx CountyOrParish to address-point sources.
var countySources = map[string][]string{
	"Ottawa": {"ottawa"}, "Toronto": {"toronto"}, "Peel": {"peel"}, "York": {"york"}, "Durham": {"durham"},
	"Hamilton": {"hamilton"}, "Niagara": {"niagara"}, "Middlesex": {"london"}, "Waterloo": {"kitchener", "waterloo"},
	"Halton": {"oakville", "milton", "burlington"}, "Simcoe": {"barrie"}, "Wellington": {"guelph"},
	"Brant": {"brantford"}, "Brantford": {"brantford"},
}

// Geocoder looks up address points. Safe for concurrent use.
type Geocoder struct {
	db   *sql.DB
	stmt *sql.Stmt
	fsa  map[string]geo.Point

	mu    sync.Mutex
	cache map[string]*Fix // by normalised address; nil = known miss
}

// Open opens the address-points SQLite file read-only.
func Open(path string) (*Geocoder, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro&immutable=1")
	if err != nil {
		return nil, err
	}
	stmt, err := db.Prepare(`SELECT src, fsa, lat, lng FROM points WHERE num = ? AND road = ?`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("address points %s: %w", path, err)
	}
	g := &Geocoder{db: db, stmt: stmt, fsa: map[string]geo.Point{}, cache: map[string]*Fix{}}
	rows, err := db.Query(`SELECT fsa, avg(lat), avg(lng) FROM points WHERE fsa <> '' GROUP BY fsa`)
	if err != nil {
		db.Close()
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var f string
		var p geo.Point
		if err := rows.Scan(&f, &p.Lat, &p.Lng); err != nil {
			return nil, err
		}
		g.fsa[strings.ToUpper(f)] = p
	}
	return g, rows.Err()
}

// Close releases the database.
func (g *Geocoder) Close() error { return g.db.Close() }

// FSAs is how many postal areas have a fallback centre.
func (g *Geocoder) FSAs() int { return len(g.fsa) }

var leadingNum = regexp.MustCompile(`^(\d+)`)

// Locate returns a fix for l, or nil.
func (g *Geocoder) Locate(l listing.Listing) (*Fix, error) {
	fsa := ""
	if pc := strings.ReplaceAll(strings.ToUpper(l.PostalCode), " ", ""); len(pc) >= 3 {
		fsa = pc[:3]
	}
	num := leadingNum.FindString(strings.TrimSpace(l.StreetNumber))
	road := StreetCore(l.StreetName)
	key := l.County + "|" + num + "|" + road + "|" + fsa

	g.mu.Lock()
	if f, ok := g.cache[key]; ok {
		g.mu.Unlock()
		return f, nil
	}
	g.mu.Unlock()

	f, err := g.lookup(l.County, num, road, fsa)
	if err != nil {
		return nil, err
	}
	g.mu.Lock()
	if len(g.cache) > 200_000 {
		clear(g.cache)
	}
	g.cache[key] = f
	g.mu.Unlock()
	return f, nil
}

func (g *Geocoder) lookup(county, num, road, fsa string) (*Fix, error) {
	if n, err := strconv.Atoi(num); err == nil && road != "" {
		rows, err := g.stmt.Query(n, road)
		if err != nil {
			return nil, err
		}
		type cand struct {
			src, fsa string
			p        geo.Point
		}
		var cands []cand
		for rows.Next() {
			var c cand
			if err := rows.Scan(&c.src, &c.fsa, &c.p.Lat, &c.p.Lng); err != nil {
				rows.Close()
				return nil, err
			}
			cands = append(cands, c)
		}
		rows.Close()

		srcs, known := countySources[county]
		var inCounty []cand
		for _, c := range cands {
			for _, s := range srcs {
				if c.src == s {
					inCounty = append(inCounty, c)
				}
			}
		}
		pick := func(cs []cand) *Fix {
			for _, c := range cs {
				if fsa != "" && strings.EqualFold(c.fsa, fsa) {
					return &Fix{c.p, Address}
				}
			}
			if len(cs) > 0 {
				return &Fix{cs[0].p, Address}
			}
			return nil
		}
		if f := pick(inCounty); f != nil {
			return f, nil
		}
		// County not covered by a source: trust an address only if it is
		// unambiguous or confirmed by the postal area.
		if !known {
			for _, c := range cands {
				if fsa != "" && strings.EqualFold(c.fsa, fsa) {
					return &Fix{c.p, Address}, nil
				}
			}
			if len(cands) == 1 {
				return &Fix{cands[0].p, Address}, nil
			}
		}
	}
	if p, ok := g.fsa[fsa]; ok {
		return &Fix{p, FSA}, nil
	}
	return nil, nil
}

// roadTail is dropped from the end of street names (hotrentals' list).
var roadTail = map[string]bool{}

func init() {
	for _, w := range strings.Fields("st street ave av avenue rd road dr drive cres crescent cr blvd boulevard ct court crt way pl place lane ln trail trl tr " +
		"terr terrace ter gate gt circle cir crcl pkwy parkway hwy highway line sq square grove grv gdns gardens hts heights " +
		"path pt point cove row walk lawn mews common cmn close glen park pk ridge rdg run hill hl heath landing lndg wood woods " +
		"n s e w ne nw se sw north south east west") {
		roadTail[w] = true
	}
}

var nonAlnum = regexp.MustCompile(`[^a-z0-9]+`)

// StreetCore normalises a street name the way the address-points database
// stores it: accents and punctuation removed, lower case, street type and
// direction dropped from the end ("St. Laurent Blvd" -> "st laurent").
func StreetCore(s string) string {
	var b strings.Builder
	for _, r := range norm.NFD.String(s) {
		if r < 0x300 || r > 0x36f {
			b.WriteRune(r)
		}
	}
	w := strings.Fields(nonAlnum.ReplaceAllString(strings.ToLower(b.String()), " "))
	for len(w) > 1 && roadTail[w[len(w)-1]] {
		w = w[:len(w)-1]
	}
	return strings.Join(w, " ")
}
