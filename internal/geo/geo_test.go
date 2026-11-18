package geo

import (
	"math/rand/v2"
	"testing"
)

var parliament = Point{45.4236, -75.7009} // Ottawa

func TestEncodeKnownGeohash(t *testing.T) {
	if got := Encode(parliament, 6).String(); got != "f244mm" {
		t.Errorf("geohash = %s, want f244mm", got)
	}
	// The usual reference example (Jutland, Denmark).
	if got := Encode(Point{57.64911, 10.40744}, 11).String(); got != "u4pruydqqvj" {
		t.Errorf("reference geohash = %s", got)
	}
	c := Encode(parliament, 6)
	if c.Parent(4).String() != "f244" || !c.Bounds().contains(parliament) {
		t.Errorf("parent %s, bounds %+v", c.Parent(4), c.Bounds())
	}
}

func TestContains(t *testing.T) {
	sq := Polygon{{45, -76}, {45, -75}, {46, -75}, {46, -76}}
	if !sq.Contains(Point{45.5, -75.5}) || sq.Contains(Point{44.9, -75.5}) || sq.Contains(Point{45.5, -74.9}) {
		t.Error("square containment wrong")
	}
}

// TestCoverIsComplete: every random point inside the polygon falls in a
// covering cell, and points marked non-exact are really inside.
func TestCoverIsComplete(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	shapes := map[string]Polygon{
		"neighbourhood": Circle(parliament, 1.5, 16),
		"city":          Circle(parliament, 20, 24),
		"region":        Circle(Point{44.0, -79.5}, 120, 32),
		"concave L":     {{45.30, -75.90}, {45.30, -75.60}, {45.35, -75.60}, {45.35, -75.85}, {45.50, -75.85}, {45.50, -75.90}},
	}
	for name, pg := range shapes {
		cov := Cover(pg, 3, 5000)
		cells := map[uint64]bool{}
		for _, c := range cov {
			cells[c.Cell.Key()] = c.Exact
		}
		b := pg.Bounds()
		hits := 0
		for range 20000 {
			pt := Point{b.MinLat + rng.Float64()*(b.MaxLat-b.MinLat), b.MinLng + rng.Float64()*(b.MaxLng-b.MinLng)}
			var exact, found bool
			c6 := Encode(pt, MaxPrecision)
			for p := 3; p <= MaxPrecision; p++ {
				if e, ok := cells[c6.Parent(p).Key()]; ok {
					found, exact = true, e
					break
				}
			}
			in := pg.Contains(pt)
			if in {
				hits++
				if !found {
					t.Fatalf("%s: point %v inside polygon but in no covering cell", name, pt)
				}
			}
			if found && !exact && !in {
				t.Fatalf("%s: point %v in a non-exact cell but outside the polygon", name, pt)
			}
		}
		t.Logf("%-13s %4d cells, %d inside samples", name, len(cov), hits)
		if hits == 0 {
			t.Fatalf("%s: no samples inside", name)
		}
	}
}
