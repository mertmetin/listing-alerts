// Package geo has the spatial pieces the matcher needs: geohash cells as
// integers, polygons, and covering a polygon with a mix of cell sizes.
package geo

import (
	"math"
	"strings"
)

// Point is a WGS84 coordinate.
type Point struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

// Cell is a geohash cell: Bits holds 5*Precision bits, longitude first.
type Cell struct {
	Bits      uint64
	Precision int
}

// MaxPrecision is the finest cell the matcher indexes (about 1.2 x 0.6 km).
const MaxPrecision = 6

// Encode returns the cell of precision p (1..12) containing pt.
func Encode(pt Point, p int) Cell {
	latLo, latHi, lngLo, lngHi := -90.0, 90.0, -180.0, 180.0
	var h uint64
	for i := 0; i < 5*p; i++ {
		h <<= 1
		if i%2 == 0 {
			mid := (lngLo + lngHi) / 2
			if pt.Lng >= mid {
				h |= 1
				lngLo = mid
			} else {
				lngHi = mid
			}
		} else {
			mid := (latLo + latHi) / 2
			if pt.Lat >= mid {
				h |= 1
				latLo = mid
			} else {
				latHi = mid
			}
		}
	}
	return Cell{Bits: h, Precision: p}
}

// Parent returns the cell's ancestor at precision p (p <= c.Precision).
func (c Cell) Parent(p int) Cell {
	return Cell{Bits: c.Bits >> (5 * uint(c.Precision-p)), Precision: p}
}

// Children returns the 32 cells one precision finer.
func (c Cell) Children() [32]Cell {
	var out [32]Cell
	for i := range out {
		out[i] = Cell{Bits: c.Bits<<5 | uint64(i), Precision: c.Precision + 1}
	}
	return out
}

// Key packs a cell into one integer that is unique across precisions.
func (c Cell) Key() uint64 { return uint64(c.Precision)<<58 | c.Bits }

// Rect is a lat/lng box.
type Rect struct{ MinLat, MinLng, MaxLat, MaxLng float64 }

// Bounds returns the cell's box.
func (c Cell) Bounds() Rect {
	r := Rect{-90, -180, 90, 180}
	n := 5 * c.Precision
	for i := 0; i < n; i++ {
		bit := c.Bits>>(uint(n-1-i))&1 == 1
		if i%2 == 0 {
			mid := (r.MinLng + r.MaxLng) / 2
			if bit {
				r.MinLng = mid
			} else {
				r.MaxLng = mid
			}
		} else {
			mid := (r.MinLat + r.MaxLat) / 2
			if bit {
				r.MinLat = mid
			} else {
				r.MaxLat = mid
			}
		}
	}
	return r
}

const base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

// String is the usual geohash text, e.g. "f244m" for central Ottawa.
func (c Cell) String() string {
	var b strings.Builder
	for i := c.Precision - 1; i >= 0; i-- {
		b.WriteByte(base32[(c.Bits>>(5*uint(i)))&31])
	}
	return b.String()
}

// Polygon is a simple ring of points (closed implicitly).
type Polygon []Point

// Bounds returns the polygon's box.
func (pg Polygon) Bounds() Rect {
	r := Rect{math.Inf(1), math.Inf(1), math.Inf(-1), math.Inf(-1)}
	for _, p := range pg {
		r.MinLat, r.MaxLat = math.Min(r.MinLat, p.Lat), math.Max(r.MaxLat, p.Lat)
		r.MinLng, r.MaxLng = math.Min(r.MinLng, p.Lng), math.Max(r.MaxLng, p.Lng)
	}
	return r
}

// Contains reports whether pt is inside (ray casting; edges count as inside
// for practical purposes, either way is fine for alerts).
func (pg Polygon) Contains(pt Point) bool {
	in := false
	n := len(pg)
	for i, j := 0, n-1; i < n; j, i = i, i+1 {
		a, b := pg[i], pg[j]
		if (a.Lat > pt.Lat) != (b.Lat > pt.Lat) {
			x := (b.Lng-a.Lng)*(pt.Lat-a.Lat)/(b.Lat-a.Lat) + a.Lng
			if pt.Lng < x {
				in = !in
			}
		}
	}
	return in
}

func (r Rect) intersects(o Rect) bool {
	return r.MinLat <= o.MaxLat && o.MinLat <= r.MaxLat && r.MinLng <= o.MaxLng && o.MinLng <= r.MaxLng
}

func (r Rect) contains(p Point) bool {
	return p.Lat >= r.MinLat && p.Lat <= r.MaxLat && p.Lng >= r.MinLng && p.Lng <= r.MaxLng
}

// relation of a cell box to a polygon.
type relation int

const (
	outside relation = iota
	inside
	partial
)

func (pg Polygon) relate(r Rect, pb Rect) relation {
	if !r.intersects(pb) {
		return outside
	}
	corners := [4]Point{{r.MinLat, r.MinLng}, {r.MinLat, r.MaxLng}, {r.MaxLat, r.MaxLng}, {r.MaxLat, r.MinLng}}
	in := 0
	for _, c := range corners {
		if pg.Contains(c) {
			in++
		}
	}
	edgeCross := false
	n := len(pg)
	for i, j := 0, n-1; i < n && !edgeCross; j, i = i, i+1 {
		if r.contains(pg[i]) {
			edgeCross = true
			break
		}
		for k := 0; k < 4; k++ {
			if segmentsCross(pg[j], pg[i], corners[k], corners[(k+1)%4]) {
				edgeCross = true
				break
			}
		}
	}
	switch {
	case in == 4 && !edgeCross:
		return inside
	case in > 0 || edgeCross:
		return partial
	}
	return outside
}

func segmentsCross(a, b, c, d Point) bool {
	o := func(p, q, r Point) float64 { return (q.Lng-p.Lng)*(r.Lat-p.Lat) - (q.Lat-p.Lat)*(r.Lng-p.Lng) }
	d1, d2, d3, d4 := o(c, d, a), o(c, d, b), o(a, b, c), o(a, b, d)
	return ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) && ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))
}

// Covered is one cell of a covering. Exact means the cell straddles the
// polygon's edge, so a point in it still needs a point-in-polygon test.
type Covered struct {
	Cell  Cell
	Exact bool
}

// Cover returns non-overlapping cells, between precision minP and
// MaxPrecision, whose union contains the polygon: big cells where the
// polygon is solid, small ones along its edge. maxCells caps the work for
// huge polygons by stopping refinement early (cells are then Exact).
func Cover(pg Polygon, minP, maxCells int) []Covered {
	if len(pg) < 3 {
		return nil
	}
	pb := pg.Bounds()
	var out []Covered
	var visit func(c Cell)
	visit = func(c Cell) {
		switch pg.relate(c.Bounds(), pb) {
		case outside:
		case inside:
			out = append(out, Covered{c, false})
		case partial:
			if c.Precision >= MaxPrecision || len(out) >= maxCells {
				out = append(out, Covered{c, true})
				return
			}
			for _, ch := range c.Children() {
				visit(ch)
			}
		}
	}
	for _, c := range cellsInRect(pb, minP) {
		visit(c)
	}
	return out
}

// cellsInRect lists the cells of precision p overlapping r.
func cellsInRect(r Rect, p int) []Cell {
	b := Encode(Point{r.MinLat, r.MinLng}, p).Bounds()
	h, w := b.MaxLat-b.MinLat, b.MaxLng-b.MinLng
	seen := map[uint64]bool{}
	var out []Cell
	for lat := b.MinLat + h/2; lat < r.MaxLat+h; lat += h {
		for lng := b.MinLng + w/2; lng < r.MaxLng+w; lng += w {
			c := Encode(Point{math.Min(lat, 89.999), lng}, p)
			if cb := c.Bounds(); cb.intersects(r) && !seen[c.Key()] {
				seen[c.Key()] = true
				out = append(out, c)
			}
		}
	}
	return out
}

// Circle approximates a circle of radius km around center with n vertices.
func Circle(center Point, km float64, n int) Polygon {
	pg := make(Polygon, n)
	dLat := km / 111.32
	dLng := km / (111.32 * math.Cos(center.Lat*math.Pi/180))
	for i := range pg {
		a := 2 * math.Pi * float64(i) / float64(n)
		pg[i] = Point{center.Lat + dLat*math.Sin(a), center.Lng + dLng*math.Cos(a)}
	}
	return pg
}
