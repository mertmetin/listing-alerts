package recorder

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRecordAndRead(t *testing.T) {
	dir := t.TempDir()
	r := &Recorder{Dir: dir}
	t0 := time.Date(2026, 11, 17, 23, 59, 0, 0, time.UTC)
	for i := range 3 {
		if err := r.Record("vow", t0, []map[string]any{{"ListingKey": "X", "n": float64(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	_ = r.Record("vow", t0, nil) // skipped

	path := filepath.Join(dir, "vow", "2026-11-17.jsonl.gz")
	if st, err := os.Stat(path); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("stat %v, err %v", st, err)
	}
	var got []float64
	if err := Read(path, func(p Page) error { got = append(got, p.Rows[0]["n"].(float64)); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[2] != 2 {
		t.Fatalf("read %v", got)
	}
}
