// Package recorder keeps raw feed pages on local disk for replay tests.
//
// Files are <dir>/<source>/<YYYY-MM-DD>.jsonl.gz, one JSON line per page:
// {"fetched_at": ..., "rows": [...]}. Each page is its own gzip member,
// written and closed in one call, so a crash never leaves a truncated stream
// (gzip readers concatenate members transparently).
//
// Recordings are MLS data: they stay on this machine (the data directory is
// gitignored) and are never published.
package recorder

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Recorder appends pages under Dir.
type Recorder struct {
	Dir string
	mu  sync.Mutex
}

// Page is one recorded fetch.
type Page struct {
	FetchedAt time.Time        `json:"fetched_at"`
	Rows      []map[string]any `json:"rows"`
}

// Record appends rows fetched from source at fetchedAt. Empty pages are skipped.
func (r *Recorder) Record(source string, fetchedAt time.Time, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	dir := filepath.Join(r.Dir, source)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, fetchedAt.UTC().Format("2006-01-02")+".jsonl.gz"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	zw := gzip.NewWriter(f)
	err = json.NewEncoder(zw).Encode(Page{FetchedAt: fetchedAt.UTC(), Rows: rows})
	err = errors.Join(err, zw.Close(), f.Close())
	return err
}

// Read calls fn for every page in one recording file, in order.
func Read(path string, fn func(Page) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	zr, err := gzip.NewReader(bufio.NewReader(f))
	if err != nil {
		return err
	}
	dec := json.NewDecoder(zr)
	for {
		var p Page
		if err := dec.Decode(&p); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		if err := fn(p); err != nil {
			return err
		}
	}
}
