package search

import (
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/mertmetin/listing-alerts/internal/event"
	"github.com/mertmetin/listing-alerts/internal/geo"
)

// MinPrecision is the coarsest cell in a covering (about 156 x 156 km).
const MinPrecision = 3

// maxCellsPerArea caps covering work for very large areas.
const maxCellsPerArea = 4000

// Query is one listing event, reduced to what matching needs.
type Query struct {
	Point    geo.Point
	HasPoint bool
	// Cities are the names a city-picked search may know the listing by,
	// normalised: PropTx puts districts and neighbourhoods in City
	// ("Toronto W08", "Barrhaven") and the city itself in County.
	Cities   []string
	Lease    bool
	Price    float64
	HasPrice bool
	Beds     int // -1 unknown
	Baths    int // -1 unknown
	SubType  string
	Type     event.Type
	Public   bool
}

// QueryFor builds the Query for a listing event.
func QueryFor(e event.ListingEvent) Query {
	l := e.Listing
	q := Query{
		Cities:  CityNames(l.City, l.County),
		Lease:   l.TransactionType != "For Sale",
		Beds:    -1,
		Baths:   -1,
		SubType: l.PropertySubType,
		Type:    e.Type,
		Public:  e.Public,
	}
	if l.Latitude != nil && l.Longitude != nil {
		q.Point, q.HasPoint = geo.Point{Lat: *l.Latitude, Lng: *l.Longitude}, true
	}
	if e.Price != nil {
		q.Price, q.HasPrice = *e.Price, true
	}
	if l.Bedrooms != nil {
		q.Beds = *l.Bedrooms
	}
	if l.Bathrooms != nil {
		q.Baths = *l.Bathrooms
	}
	return q
}

var districtCode = regexp.MustCompile(`^(.+?)\s+[a-z]\d{2}$`)

// CityNames lists the normalised names a listing goes by: its City, the
// City without a Toronto-style district code ("toronto w08" -> "toronto"),
// and its County ("Ottawa" for a listing in "Barrhaven"). Duplicates and
// empty names are dropped.
func CityNames(city, county string) []string {
	var out []string
	add := func(s string) {
		if s = NormCity(s); s != "" && !slices.Contains(out, s) {
			out = append(out, s)
		}
	}
	add(city)
	if m := districtCode.FindStringSubmatch(NormCity(city)); m != nil {
		add(m[1])
	}
	add(county)
	return out
}

// Prepared is a search with its covering computed once, so it can be added
// to many shards cheaply.
type Prepared struct {
	S     *Search
	Cover []geo.Covered
}

// Prepare computes the covering of every area.
func Prepare(s *Search) Prepared {
	p := Prepared{S: s}
	for _, a := range s.Areas {
		p.Cover = append(p.Cover, geo.Cover(a, MinPrecision, maxCellsPerArea)...)
	}
	return p
}

// filter is the per-search data the hot loop reads, kept small and flat.
type filter struct {
	minPrice, maxPrice float32
	minBeds, minBaths  int8
	public             bool
	alive              bool
	// multi: more than one area, or areas and cities, so one point can reach
	// the search through two entries and needs the dedup check.
	multi    bool
	events   uint16
	typeMask uint32
}

const exactBit = 1 << 31

// Index is one shard's reverse index: listing location -> saved searches.
//
// Cells of every precision live in one map keyed by geo.Cell.Key. A point
// is looked up at precisions 3..6 (4 map reads); because each search's
// covering cells don't overlap, a point hits at most one cell per area.
// Entries in edge cells carry exactBit and get a point-in-polygon test.
// Searches picked by city live in a second map.
//
// Updates tombstone the old slot and append a new one; Compact rebuilds
// when tombstones pile up. Matching takes a read lock; updates a write lock.
type Index struct {
	mu       sync.RWMutex
	searches []*Search
	filters  []filter
	slotOf   map[string]uint32
	cells    map[uint64][]uint32
	cities   map[string][]uint32
	dead     int
	types    *typeBits
	scratch  sync.Pool
}

// NewIndex returns an empty index.
func NewIndex() *Index {
	return &Index{slotOf: map[string]uint32{}, cells: map[uint64][]uint32{}, cities: map[string][]uint32{}, types: newTypeBits()}
}

// Owns says whether this shard is responsible for a ShardPrecision cell.
// nil means every cell.
type Owns func(c geo.Cell) bool

// Upsert adds or replaces a search. Deleted searches are removed. Only the
// cells this shard owns are indexed; city lists are indexed on every shard.
// It returns whether the search is held by this shard.
func (ix *Index) Upsert(p Prepared, owns Owns) bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.upsertLocked(p, owns)
}

func (ix *Index) upsertLocked(p Prepared, owns Owns) bool {
	s := p.S
	if old, ok := ix.slotOf[s.ID]; ok {
		ix.filters[old].alive = false
		ix.dead++
		delete(ix.slotOf, s.ID)
	}
	if s.Deleted {
		return false
	}

	var keep []geo.Covered
	for _, c := range p.Cover {
		if owns == nil || ownsCell(c.Cell, owns) {
			keep = append(keep, c)
		}
	}
	if len(keep) == 0 && len(s.Cities) == 0 {
		return false
	}

	slot := uint32(len(ix.searches))
	ix.searches = append(ix.searches, s)
	ix.filters = append(ix.filters, ix.filterFor(s))
	ix.slotOf[s.ID] = slot
	for _, c := range keep {
		e := slot
		if c.Exact {
			e |= exactBit
		}
		k := cellKey(c.Cell, s.Transaction == "lease")
		ix.cells[k] = append(ix.cells[k], e)
	}
	for _, city := range s.Cities {
		k := cityKey(city, s.Transaction == "lease")
		ix.cities[k] = append(ix.cities[k], slot)
	}
	return true
}

// ownsCell: a cell at or below the shard size belongs to the owner of its
// ShardPrecision ancestor. A bigger cell (only huge drawn areas have them)
// spans many shard cells, so every shard keeps it: correct, and cheap
// because there are few.
func ownsCell(c geo.Cell, owns Owns) bool {
	if c.Precision >= ShardPrecision {
		return owns(c.Parent(ShardPrecision))
	}
	return true
}

// cellKey adds the transaction to a cell key, so a lease event never scans
// sale searches (and vice versa): the cell's entries are split in two.
func cellKey(c geo.Cell, lease bool) uint64 {
	k := c.Key()
	if lease {
		k |= 1 << 57
	}
	return k
}

func cityKey(city string, lease bool) string {
	if lease {
		return "L" + city
	}
	return "S" + city
}

// Remove deletes a search.
func (ix *Index) Remove(id string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if old, ok := ix.slotOf[id]; ok {
		ix.filters[old].alive = false
		ix.dead++
		delete(ix.slotOf, id)
	}
}

// Len is the number of live searches.
func (ix *Index) Len() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return len(ix.slotOf)
}

// Stats for metrics.
type Stats struct{ Searches, Slots, Cells, Entries, Cities int }

func (ix *Index) Stats() Stats {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	st := Stats{Searches: len(ix.slotOf), Slots: len(ix.searches), Cells: len(ix.cells), Cities: len(ix.cities)}
	for _, es := range ix.cells {
		st.Entries += len(es)
	}
	return st
}

// NeedsCompact reports whether tombstones exceed a quarter of the slots.
func (ix *Index) NeedsCompact() bool {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.dead > 1000 && ix.dead*4 > len(ix.searches)
}

// Compact rebuilds the index without tombstones. Coverings are recomputed,
// so it is not free (about the cost of loading the searches); call it from
// a background loop when NeedsCompact says so.
func (ix *Index) Compact(owns Owns) {
	ix.mu.RLock()
	live := make([]Prepared, 0, len(ix.slotOf))
	for _, slot := range ix.slotOf {
		live = append(live, Prepare(ix.searches[slot]))
	}
	ix.mu.RUnlock()

	fresh := NewIndex()
	fresh.types = ix.types
	for _, p := range live {
		fresh.upsertLocked(p, owns)
	}
	ix.mu.Lock()
	ix.searches, ix.filters, ix.slotOf = fresh.searches, fresh.filters, fresh.slotOf
	ix.cells, ix.cities, ix.dead = fresh.cells, fresh.cities, 0
	ix.mu.Unlock()
}

func (ix *Index) filterFor(s *Search) filter {
	f := filter{
		minPrice: float32(s.MinPrice), maxPrice: float32(s.MaxPrice),
		minBeds: int8(s.MinBeds), minBaths: int8(s.MinBaths),
		public: s.Audience == Public, alive: true,
		multi: len(s.Areas)+min(len(s.Cities), 1) > 1,
	}
	evs := s.Events
	if len(evs) == 0 {
		evs = DefaultEvents
	}
	for _, e := range evs {
		f.events |= eventBit(e)
	}
	for _, t := range s.Types {
		f.typeMask |= ix.types.bit(t)
	}
	return f
}

type scratch struct {
	seen []uint32
	gen  uint32
}

// Match appends the searches q matches to dst and returns it. Pass a reused
// dst[:0] in hot loops to avoid allocating.
func (ix *Index) Match(q Query, dst []*Search) []*Search {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	sc, _ := ix.scratch.Get().(*scratch)
	if sc == nil {
		sc = &scratch{}
	}
	defer ix.scratch.Put(sc)
	if len(sc.seen) < len(ix.searches) {
		sc.seen = make([]uint32, len(ix.searches)+len(ix.searches)/4+64)
		sc.gen = 0
	}
	sc.gen++
	if sc.gen == 0 { // wrapped: clear
		clear(sc.seen)
		sc.gen = 1
	}

	// What the event means to each audience.
	vowBits := eventBit(q.Type) // now_public is not news to VOW users
	var pubBits uint16
	if q.Public {
		if q.Type == event.NowPublic {
			pubBits = eventBit(event.NewListing)
		} else {
			pubBits = eventBit(q.Type)
		}
	}
	if vowBits|pubBits == 0 {
		return dst
	}
	typeBit := ix.types.lookup(q.SubType)
	price := float32(q.Price)

	// match reports whether slot passes the filters (polygon test aside).
	// The filter record is the only per-candidate memory read on the
	// common path; the profile showed scattered reads dominate.
	filters := ix.filters
	match := func(f *filter) bool {
		bits := vowBits
		if f.public {
			bits = pubBits
		}
		return f.events&bits != 0 &&
			(f.minPrice == 0 || (q.HasPrice && price >= f.minPrice)) &&
			(f.maxPrice == 0 || (q.HasPrice && price <= f.maxPrice)) &&
			(f.minBeds == 0 || q.Beds >= int(f.minBeds)) &&
			(f.minBaths == 0 || q.Baths >= int(f.minBaths)) &&
			(f.typeMask == 0 || f.typeMask&typeBit != 0)
	}
	inAreas := func(slot uint32) bool {
		for _, a := range ix.searches[slot].Areas {
			if a.Contains(q.Point) {
				return true
			}
		}
		return false
	}

	gen, seen := sc.gen, sc.seen
	if q.HasPoint {
		c6 := geo.Encode(q.Point, geo.MaxPrecision)
		for p := MinPrecision; p <= geo.MaxPrecision; p++ {
			for _, e := range ix.cells[cellKey(c6.Parent(p), q.Lease)] {
				slot := e &^ exactBit
				f := &filters[slot]
				if !f.alive || !match(f) || (e&exactBit != 0 && !inAreas(slot)) {
					continue
				}
				if f.multi {
					if seen[slot] == gen {
						continue
					}
					seen[slot] = gen
				}
				dst = append(dst, ix.searches[slot])
			}
		}
	}
	for _, city := range q.Cities {
		for _, slot := range ix.cities[cityKey(city, q.Lease)] {
			f := &filters[slot]
			if !f.alive || !match(f) {
				continue
			}
			if f.multi {
				if seen[slot] == gen {
					continue
				}
				seen[slot] = gen
			}
			dst = append(dst, ix.searches[slot])
		}
	}
	return dst
}

// typeBits maps property sub-types to bits. 31 named types; the rest share
// bit 31, so a search for an unknown type still matches only unknown types.
type typeBits struct {
	mu   sync.RWMutex
	bits map[string]uint32
}

func newTypeBits() *typeBits { return &typeBits{bits: map[string]uint32{}} }

func (t *typeBits) lookup(s string) uint32 {
	s = strings.ToLower(strings.TrimSpace(s))
	t.mu.RLock()
	b, ok := t.bits[s]
	t.mu.RUnlock()
	if ok {
		return b
	}
	return 1 << 31
}

func (t *typeBits) bit(s string) uint32 {
	s = strings.ToLower(strings.TrimSpace(s)) // "Att/Row/Townhouse" = "att/row/townhouse"
	t.mu.Lock()
	defer t.mu.Unlock()
	if b, ok := t.bits[s]; ok {
		return b
	}
	if len(t.bits) < 31 {
		b := uint32(1) << len(t.bits)
		t.bits[s] = b
		return b
	}
	return 1 << 31
}
