// Package state owns the replicator's local watermark file.
//
// The file lives next to the database (default: /data/.zapdb-replicate-
// state.json). It records the most recent badger version successfully
// uploaded so a restart resumes the incremental stream where it left
// off without re-uploading the whole DB.
//
// It is NOT the source of truth — that is the backend's HEAD.json. The
// state file is just a cheap local cache that lets the daemon skip
// re-listing the backend on each cycle.
package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// State is the persisted replicator watermark.
type State struct {
	Version           int       `json:"version"`            // schema version (1)
	Network           string    `json:"network"`
	LastVersion       uint64    `json:"lastVersion"`        // badger watermark we last uploaded through
	LastSnapshotKind  string    `json:"lastSnapshotKind"`   // "full" | "increment"
	LastFullVersion   uint64    `json:"lastFullVersion"`    // watermark of the most recent full snapshot
	LastFullTimestamp time.Time `json:"lastFullTimestamp"`  // when the last full snapshot ran
	UpdatedAt         time.Time `json:"updatedAt"`
}

// Path returns the canonical state file path for a database dir.
func Path(dbPath string) string {
	dir := filepath.Dir(dbPath)
	base := filepath.Base(dbPath)
	return filepath.Join(dir, "."+base+".zapdb-replicate-state.json")
}

// Load reads State from path. Returns a zero State (Version=1) if the
// file does not exist; any other error is wrapped.
func Load(path string) (*State, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return &State{Version: 1}, nil
		}
		return nil, fmt.Errorf("state: read %s: %w", path, err)
	}
	var s State
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("state: decode %s: %w", path, err)
	}
	if s.Version != 1 {
		return nil, fmt.Errorf("state: unsupported version %d", s.Version)
	}
	return &s, nil
}

// Save atomically writes State to path via rename. Caller is
// responsible for ensuring dir exists.
func Save(path string, s *State) error {
	s.Version = 1
	s.UpdatedAt = time.Now().UTC()
	body, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("state: marshal: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return fmt.Errorf("state: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("state: rename %s -> %s: %w", tmp, path, err)
	}
	return nil
}
