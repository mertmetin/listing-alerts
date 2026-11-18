package reso

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

// fakeFeed is an in-memory Property collection that understands exactly the
// filter shapes Source sends, so tests exercise real keyset semantics.
type fakeFeed struct {
	mu   sync.Mutex
	rows map[string]fakeRow // by key
	reqs int
}

type fakeRow struct {
	key    string
	ts     time.Time
	active bool
}

var (
	reKeyGT  = regexp.MustCompile(`^\(ListingKey gt '([^']*)'\)$`)
	reAfter  = regexp.MustCompile(`^\(\(ModificationTimestamp gt (\S+) or \(ModificationTimestamp eq (\S+) and ListingKey gt '([^']*)'\)\)\)$`)
	reSince  = regexp.MustCompile(`^\(ModificationTimestamp ge (\S+)\)$`)
	reActive = regexp.MustCompile(`^\(Active\)$`)
)

func (f *fakeFeed) put(key string, ts time.Time, active bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows[key] = fakeRow{key, ts, active}
}

func (f *fakeFeed) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs++
	q := r.URL.Query()
	top, _ := strconv.Atoi(q.Get("$top"))
	byKeyOnly := q.Get("$orderby") == "ListingKey"

	// Split "(a) and (b) and (c)" into its parts.
	var conds []string
	if fl := q.Get("$filter"); fl != "" {
		depth, start := 0, 0
		for i, c := range fl {
			switch c {
			case '(':
				if depth == 0 {
					start = i
				}
				depth++
			case ')':
				depth--
				if depth == 0 {
					conds = append(conds, fl[start:i+1])
				}
			}
		}
	}

	var out []fakeRow
	for _, row := range f.rows {
		ok := true
		for _, c := range conds {
			switch {
			case reActive.MatchString(c):
				ok = ok && row.active
			case reKeyGT.MatchString(c):
				ok = ok && row.key > reKeyGT.FindStringSubmatch(c)[1]
			case reAfter.MatchString(c):
				m := reAfter.FindStringSubmatch(c)
				t1, _ := time.Parse(time.RFC3339Nano, m[1])
				ok = ok && (row.ts.After(t1) || (row.ts.Equal(t1) && row.key > m[3]))
			case reSince.MatchString(c):
				t1, _ := time.Parse(time.RFC3339Nano, reSince.FindStringSubmatch(c)[1])
				ok = ok && !row.ts.Before(t1)
			default:
				http.Error(w, "fake feed cannot parse "+c, http.StatusBadRequest)
				return
			}
		}
		if ok {
			out = append(out, row)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if byKeyOnly || out[i].ts.Equal(out[j].ts) {
			return out[i].key < out[j].key
		}
		return out[i].ts.Before(out[j].ts)
	})
	if top > 0 && len(out) > top {
		out = out[:top]
	}
	vals := make([]map[string]any, len(out))
	for i, row := range out {
		vals[i] = map[string]any{"ListingKey": row.key, "ModificationTimestamp": Timestamp(row.ts)}
	}
	json.NewEncoder(w).Encode(map[string]any{"value": vals})
}

func newFakeSource(t *testing.T, page int) (*Source, *fakeFeed) {
	feed := &fakeFeed{rows: map[string]fakeRow{}}
	c, _ := newTestClient(t, feed.ServeHTTP)
	return &Source{Client: c, BaselineFilter: "Active", PageSize: page, Overlap: time.Minute}, feed
}

// drain fetches until a page says there is no more, collecting keys.
func drain(t *testing.T, s *Source, cur Cursor, now time.Time) (Cursor, []string) {
	t.Helper()
	var keys []string
	for i := 0; i < 1000; i++ {
		p, err := s.Fetch(context.Background(), cur, now)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range p.Records {
			keys = append(keys, r["ListingKey"].(string))
		}
		cur = p.Next
		if !p.More && cur.Phase == Incremental {
			return cur, keys
		}
	}
	t.Fatal("did not converge")
	return cur, nil
}

func TestBaselineThenIncrementalReadsEverythingOnce(t *testing.T) {
	s, feed := newFakeSource(t, 3)
	t0 := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	for i := range 10 {
		feed.put(fmt.Sprintf("A%02d", i), t0.Add(-time.Duration(i+1)*time.Hour), i%3 != 0) // some inactive, all before the overlap window
	}

	cur, got := drain(t, s, Cursor{}, t0)
	if want := 6; len(got) != want { // the active ones
		t.Fatalf("baseline read %d (%v), want %d", len(got), got, want)
	}
	if cur.Phase != Incremental || !cur.TS.Equal(t0.Add(-time.Minute)) {
		t.Fatalf("after baseline cursor = %+v", cur)
	}

	// A burst of 7 changes that share one timestamp: bigger than a page.
	burst := t0.Add(5 * time.Minute)
	for i := range 7 {
		feed.put(fmt.Sprintf("B%02d", i), burst, true)
	}
	feed.put("A01", burst.Add(time.Second), true)

	cur, got = drain(t, s, cur, t0)
	want := []string{"B00", "B01", "B02", "B03", "B04", "B05", "B06", "A01"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("incremental read %v, want %v", got, want)
	}

	// Nothing new: an empty page, cursor unchanged.
	again, got := drain(t, s, cur, t0)
	if len(got) != 0 || again != cur {
		t.Fatalf("idle poll read %v, cursor %+v -> %+v", got, cur, again)
	}
}

func TestRecentCatchesLateRecords(t *testing.T) {
	s, feed := newFakeSource(t, 100)
	t0 := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	feed.put("A1", t0, true)
	cur, _ := drain(t, s, Cursor{Phase: Incremental, TS: t0.Add(-time.Hour)}, t0)

	// A record committed late with a timestamp the cursor already passed.
	feed.put("LATE", t0.Add(-10*time.Second), true)
	_, got := drain(t, s, cur, t0)
	if len(got) != 0 {
		t.Fatalf("cursor unexpectedly re-read %v", got)
	}
	rows, err := s.Recent(context.Background(), t0.Add(-2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0]["ListingKey"] != "LATE" {
		t.Fatalf("Recent = %v", rows)
	}
}

func TestKeysPagesThroughEverything(t *testing.T) {
	s, feed := newFakeSource(t, 100)
	for i := range 12001 {
		feed.put(fmt.Sprintf("K%05d", i), time.Unix(0, 0), i%2 == 0)
	}
	keys, err := s.Keys(context.Background(), "Active")
	if err != nil || len(keys) != 6001 || feed.reqs != 2 {
		t.Fatalf("keys %d, reqs %d, err %v", len(keys), feed.reqs, err)
	}
}
