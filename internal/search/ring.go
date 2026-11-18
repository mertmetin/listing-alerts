package search

import (
	"hash/fnv"
	"slices"
	"strconv"

	"github.com/mertmetin/listing-alerts/internal/geo"
)

// ShardPrecision is the geohash cell size that is the unit of sharding:
// precision 5 is about 4.9 x 4.9 km, small enough that a dense city spreads
// over many shards (precision 4, about 39 x 20 km, put most of Toronto on
// one shard).
const ShardPrecision = 5

// Ring is a consistent-hash ring of matcher shards. Keys are geohash cells
// at ShardPrecision ("g:f244m") or, for listings without coordinates,
// cities ("c:ottawa"). Each shard has many virtual nodes, so load spreads evenly
// and adding a shard moves only about 1/N of the keys.
type Ring struct {
	hashes []uint64
	owner  []int
	shards int
}

// NewRing builds a ring of shards 0..n-1 with vnodes virtual nodes each.
func NewRing(n, vnodes int) *Ring {
	r := &Ring{shards: n}
	type pt struct {
		h uint64
		s int
	}
	pts := make([]pt, 0, n*vnodes)
	for s := range n {
		for v := range vnodes {
			pts = append(pts, pt{hash64("shard-" + strconv.Itoa(s) + "#" + strconv.Itoa(v)), s})
		}
	}
	slices.SortFunc(pts, func(a, b pt) int {
		switch {
		case a.h < b.h:
			return -1
		case a.h > b.h:
			return 1
		}
		return 0
	})
	for _, p := range pts {
		r.hashes = append(r.hashes, p.h)
		r.owner = append(r.owner, p.s)
	}
	return r
}

// Shards is the shard count.
func (r *Ring) Shards() int { return r.shards }

// Owner returns the shard for key: the first virtual node clockwise.
func (r *Ring) Owner(key string) int {
	h := hash64(key)
	i, _ := slices.BinarySearch(r.hashes, h)
	if i == len(r.hashes) {
		i = 0
	}
	return r.owner[i]
}

// CellOwner is the shard owning the ShardPrecision cell that contains c
// (c.Precision >= ShardPrecision).
func (r *Ring) CellOwner(c geo.Cell) int { return r.Owner("g:" + c.Parent(ShardPrecision).String()) }

// QueryOwner is the shard that matches q: by location when known, else by city.
func (r *Ring) QueryOwner(q Query) int {
	if q.HasPoint {
		return r.CellOwner(geo.Encode(q.Point, ShardPrecision))
	}
	// Any stable name works, since city searches are on every shard; the
	// last one is the county when there is one.
	if n := len(q.Cities); n > 0 {
		return r.Owner("c:" + q.Cities[n-1])
	}
	return r.Owner("c:")
}

// OwnsFunc returns the Owns predicate for one shard, memoised per cell
// (the same cells come up for many searches).
func (r *Ring) OwnsFunc(shard int) Owns {
	memo := map[uint64]bool{}
	return func(c geo.Cell) bool {
		k := c.Key()
		if v, ok := memo[k]; ok {
			return v
		}
		v := r.CellOwner(c) == shard
		memo[k] = v
		return v
	}
}

func hash64(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	// fnv's low bits are weak for short similar keys; finish with a mixer.
	x := h.Sum64()
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}
