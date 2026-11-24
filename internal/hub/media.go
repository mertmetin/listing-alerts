package hub

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/mertmetin/listing-alerts/internal/reso"
)

// PropTxMedia fetches photos and rooms from the RESO API, in batches.
type PropTxMedia struct {
	Client *reso.Client
}

var sizeSuffix = regexp.MustCompile(`-[a-z]+$`)

func inList(keys []string) string {
	q := make([]string, len(keys))
	for i, k := range keys {
		q[i] = reso.Quote(k)
	}
	return strings.Join(q, ",")
}

func batches(keys []string, n int) [][]string {
	var out [][]string
	for len(keys) > 0 {
		m := min(n, len(keys))
		out = append(out, keys[:m])
		keys = keys[m:]
	}
	return out
}

// Photos returns each listing's public, active photos in three sizes,
// preferred photo first, then by Order. PropTx gives the sizes of one photo
// the same MediaKey with a size suffix ("…-l", "…-m", "…-t").
func (m PropTxMedia) Photos(ctx context.Context, keys []string) (map[string][]Photo, error) {
	type item struct {
		order     float64
		preferred bool
		p         Photo
	}
	out := map[string][]Photo{}
	for _, b := range batches(keys, 15) {
		rows, err := m.Client.Get(ctx, "Media", reso.Query{
			Filter: "ResourceRecordKey in (" + inList(b) + ") and MediaStatus eq 'Active' and MediaCategory eq 'Photo'" +
				" and (ImageSizeDescription eq 'Large' or ImageSizeDescription eq 'Medium' or ImageSizeDescription eq 'Thumbnail')",
			Select: []string{"ResourceRecordKey", "MediaKey", "MediaURL", "ImageSizeDescription", "Order", "PreferredPhotoYN", "Permission"},
			Top:    5000,
		})
		if err != nil {
			return nil, fmt.Errorf("media: %w", err)
		}
		byListing := map[string]map[string]*item{}
		for _, r := range rows {
			if !public(r["Permission"]) {
				continue
			}
			lk, _ := r["ResourceRecordKey"].(string)
			mk, _ := r["MediaKey"].(string)
			url, _ := r["MediaURL"].(string)
			if lk == "" || url == "" {
				continue
			}
			base := sizeSuffix.ReplaceAllString(mk, "")
			if byListing[lk] == nil {
				byListing[lk] = map[string]*item{}
			}
			it := byListing[lk][base]
			if it == nil {
				it = &item{order: 1e9}
				byListing[lk][base] = it
			}
			if o, ok := r["Order"].(float64); ok && o < it.order {
				it.order = o
			}
			if pr, _ := r["PreferredPhotoYN"].(bool); pr {
				it.preferred = true
			}
			switch r["ImageSizeDescription"] {
			case "Large":
				it.p.Large = url
			case "Medium":
				it.p.Medium = url
			case "Thumbnail":
				it.p.Thumb = url
			}
		}
		for lk, items := range byListing {
			list := make([]*item, 0, len(items))
			for _, it := range items {
				list = append(list, it)
			}
			sort.SliceStable(list, func(i, j int) bool {
				if list[i].preferred != list[j].preferred {
					return list[i].preferred
				}
				return list[i].order < list[j].order
			})
			for _, it := range list {
				out[lk] = append(out[lk], it.p)
			}
		}
	}
	return out, nil
}

// public: no Permission list means public; otherwise it must include "Public".
func public(v any) bool {
	list, ok := v.([]any)
	if !ok || len(list) == 0 {
		return true
	}
	for _, p := range list {
		if p == "Public" {
			return true
		}
	}
	return false
}

// Rooms returns each listing's rooms in Order.
func (m PropTxMedia) Rooms(ctx context.Context, keys []string) (map[string][]Room, error) {
	type item struct {
		order float64
		r     Room
	}
	out := map[string][]Room{}
	for _, b := range batches(keys, 40) {
		rows, err := m.Client.Get(ctx, "PropertyRooms", reso.Query{
			Filter: "ListingKey in (" + inList(b) + ")",
			Select: []string{"ListingKey", "Order", "RoomType", "RoomLevel", "RoomLength", "RoomWidth", "RoomFeature1", "RoomFeature2", "RoomFeature3"},
			Top:    5000,
		})
		if err != nil {
			return nil, fmt.Errorf("rooms: %w", err)
		}
		by := map[string][]item{}
		for _, r := range rows {
			lk, _ := r["ListingKey"].(string)
			t, _ := r["RoomType"].(string)
			if lk == "" || t == "" {
				continue
			}
			var feats []string
			for _, f := range []string{"RoomFeature1", "RoomFeature2", "RoomFeature3"} {
				if s, _ := r[f].(string); s != "" {
					feats = append(feats, s)
				}
			}
			it := item{r: Room{Type: t, Features: strings.Join(feats, ", ")}}
			it.r.Level, _ = r["RoomLevel"].(string)
			it.r.Length, _ = r["RoomLength"].(float64)
			it.r.Width, _ = r["RoomWidth"].(float64)
			it.order, _ = r["Order"].(float64)
			by[lk] = append(by[lk], it)
		}
		for lk, items := range by {
			sort.SliceStable(items, func(i, j int) bool { return items[i].order < items[j].order })
			for _, it := range items {
				out[lk] = append(out[lk], it.r)
			}
		}
	}
	return out, nil
}
