package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mertmetin/listing-alerts/internal/reso"
)

func fakeAPI(t *testing.T, routes map[string][]map[string]any) *reso.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res := strings.TrimPrefix(r.URL.Path, "/odata/")
		json.NewEncoder(w).Encode(map[string]any{"value": routes[res]})
	}))
	t.Cleanup(srv.Close)
	c := reso.New(srv.URL+"/odata/", "tok").WithHTTPClient(srv.Client())
	c.MinInterval = 0
	return c
}

func TestPhotosGroupSizesAndOrder(t *testing.T) {
	m := PropTxMedia{Client: fakeAPI(t, map[string][]map[string]any{"Media": {
		{"ResourceRecordKey": "X1", "MediaKey": "a-l", "MediaURL": "https://img/a-l.jpg", "ImageSizeDescription": "Large", "Order": 2.0},
		{"ResourceRecordKey": "X1", "MediaKey": "a-t", "MediaURL": "https://img/a-t.jpg", "ImageSizeDescription": "Thumbnail", "Order": 2.0},
		{"ResourceRecordKey": "X1", "MediaKey": "b-l", "MediaURL": "https://img/b-l.jpg", "ImageSizeDescription": "Large", "Order": 1.0},
		{"ResourceRecordKey": "X1", "MediaKey": "c-l", "MediaURL": "https://img/c-l.jpg", "ImageSizeDescription": "Large", "Order": 9.0, "PreferredPhotoYN": true},
		{"ResourceRecordKey": "X1", "MediaKey": "c-m", "MediaURL": "https://img/c-m.jpg", "ImageSizeDescription": "Medium", "Order": 9.0},
		{"ResourceRecordKey": "X1", "MediaKey": "p-l", "MediaURL": "https://img/private.jpg", "ImageSizeDescription": "Large", "Permission": []any{"Private"}},
		{"ResourceRecordKey": "X2", "MediaKey": "z-l", "MediaURL": "https://img/z-l.jpg", "ImageSizeDescription": "Large", "Order": 1.0},
	}})}
	got, err := m.Photos(context.Background(), []string{"X1", "X2"})
	if err != nil {
		t.Fatal(err)
	}
	x1 := got["X1"]
	if len(x1) != 3 {
		t.Fatalf("X1 photos %+v (private one must be dropped)", x1)
	}
	// Preferred first, then by Order; sizes of one photo merged.
	if x1[0].Large != "https://img/c-l.jpg" || x1[0].Medium != "https://img/c-m.jpg" || x1[1].Large != "https://img/b-l.jpg" ||
		x1[2].Large != "https://img/a-l.jpg" || x1[2].Thumb != "https://img/a-t.jpg" {
		t.Fatalf("X1 order/sizes %+v", x1)
	}
	if len(got["X2"]) != 1 {
		t.Fatalf("X2 %+v", got["X2"])
	}
}

func TestRoomsInOrder(t *testing.T) {
	m := PropTxMedia{Client: fakeAPI(t, map[string][]map[string]any{"PropertyRooms": {
		{"ListingKey": "X1", "Order": 2.0, "RoomType": "Kitchen", "RoomLevel": "Main", "RoomLength": 4.2, "RoomWidth": 3.1, "RoomFeature1": "Quartz"},
		{"ListingKey": "X1", "Order": 1.0, "RoomType": "Living", "RoomLevel": "Main"},
		{"ListingKey": "X1", "Order": 3.0, "RoomType": ""}, // no type: dropped
	}})}
	got, _ := m.Rooms(context.Background(), []string{"X1"})
	r := got["X1"]
	if len(r) != 2 || r[0].Type != "Living" || r[1].Type != "Kitchen" || r[1].Length != 4.2 || r[1].Features != "Quartz" {
		t.Fatalf("rooms %+v", r)
	}
}

func TestHashIgnoresModificationTimestamp(t *testing.T) {
	a := map[string]any{"ListingKey": "X1", "ListPrice": 500000.0, "ModificationTimestamp": "2026-11-24T01:00:00Z"}
	b := map[string]any{"ListingKey": "X1", "ListPrice": 500000.0, "ModificationTimestamp": "2026-11-24T09:00:00Z"}
	if Hash(a) != Hash(b) {
		t.Error("timestamp-only change altered the hash")
	}
	b["PublicRemarks"] = "New roof 2025."
	if Hash(a) == Hash(b) {
		t.Error("a new description did not change the hash")
	}
}

// fakePagedAPI serves Property rows sorted by key, honouring
// "(ListingKey gt 'X')" and $top, like the backfill's paging.
func fakePagedAPI(t *testing.T, rows []map[string]any) *reso.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f := r.URL.Query().Get("$filter")
		after := ""
		if i := strings.Index(f, "ListingKey gt '"); i >= 0 {
			rest := f[i+len("ListingKey gt '"):]
			after = rest[:strings.Index(rest, "'")]
		}
		top := 1000
		fmt.Sscan(r.URL.Query().Get("$top"), &top)
		var out []map[string]any
		for _, row := range rows {
			if row["ListingKey"].(string) > after && len(out) < top {
				out = append(out, row)
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"value": out})
	}))
	t.Cleanup(srv.Close)
	c := reso.New(srv.URL+"/odata/", "tok").WithHTTPClient(srv.Client())
	c.MinInterval = 0
	return c
}
