// Package manifest defines the JSON metadata blobs that describe each
// snapshot of a ZapDB and the HEAD pointer that names the current chain
// of full + incremental manifests.
//
// Layout in the backend:
//
//	<prefix>/HEAD.json                        -- pointer
//	<prefix>/manifests/<ts>.json              -- one per snapshot
//	<prefix>/blocks/<2-hex>/<blake3>.zap.age  -- written by hanzoai/vfs
//
// HEAD names the most recent full snapshot plus the ordered list of
// incrementals applied since. Restore walks HEAD: read the full, then
// each incremental in order, calling db.Load(r) on each.
package manifest

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/hanzoai/vfs"
)

// Type names the snapshot category.
type Type string

const (
	TypeFull        Type = "full"
	TypeIncremental Type = "increment"
)

// Manifest is the per-snapshot record written at <prefix>/manifests/<ts>.json.
//
// Blocks are listed in the order they were produced by vfs.PutBlock so
// that Restore can stream them back into db.Load(r) without an index.
//
// PlainSize is the number of plaintext bytes the chunked writer
// observed before padding+encryption. The last block is zero-padded to
// vfs.BlockSize, so Restore truncates its final block read to
// (PlainSize % BlockSize) when non-zero.
type Manifest struct {
	Version    int            `json:"version"`           // schema version
	Type       Type           `json:"type"`              // full | increment
	Since      uint64         `json:"since"`             // 0 for full
	Until      uint64         `json:"until"`             // badger version watermark after this snap
	Network    string         `json:"network"`           // mainnet|testnet|devnet|...
	Blocks     []vfs.BlockID  `json:"blocks"`            // ordered block IDs
	PlainSize  int64          `json:"plainSize"`         // bytes of plaintext stream
	AgeScheme  string         `json:"ageScheme"`         // "X25519+MLKEM768" etc.
	BlockSize  int            `json:"blockSize"`         // vfs.BlockSize at write time
	CreatedAt  time.Time      `json:"createdAt"`
	Labels     map[string]string `json:"labels,omitempty"`
}

// Validate checks invariants we rely on at restore time.
func (m *Manifest) Validate() error {
	if m.Version != 1 {
		return fmt.Errorf("manifest: unsupported version %d", m.Version)
	}
	switch m.Type {
	case TypeFull:
		if m.Since != 0 {
			return fmt.Errorf("manifest: full snapshot must have since=0, got %d", m.Since)
		}
	case TypeIncremental:
		if m.Since == 0 {
			return fmt.Errorf("manifest: incremental must have since>0")
		}
		if m.Until <= m.Since {
			return fmt.Errorf("manifest: until(%d) must be > since(%d)", m.Until, m.Since)
		}
	default:
		return fmt.Errorf("manifest: unknown type %q", m.Type)
	}
	if m.BlockSize != vfs.BlockSize {
		return fmt.Errorf("manifest: blockSize %d != current vfs.BlockSize %d (cross-version restore unsupported)", m.BlockSize, vfs.BlockSize)
	}
	if len(m.Blocks) == 0 && m.PlainSize > 0 {
		return fmt.Errorf("manifest: plainSize=%d but no blocks listed", m.PlainSize)
	}
	return nil
}

// MarshalJSON produces stable, deterministic JSON (sorted labels).
func (m *Manifest) MarshalJSON() ([]byte, error) {
	type alias Manifest
	a := alias(*m)
	if len(a.Labels) > 0 {
		// json package already sorts map keys for marshaling, but be
		// explicit so the output is deterministic across Go versions.
		keys := make([]string, 0, len(a.Labels))
		for k := range a.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
	}
	return json.Marshal(a)
}

// HEAD is the single pointer object the replicator updates after every
// snapshot. Lives at <prefix>/HEAD.json.
//
// CurrentVersion is the badger watermark of the most recent snapshot.
// Restore should land at exactly CurrentVersion after applying
// LastFullManifest plus every entry of Increments in order.
type HEAD struct {
	Version           int       `json:"version"`            // schema version
	Network           string    `json:"network"`
	CurrentVersion    uint64    `json:"currentVersion"`     // badger watermark
	LastFullManifest  string    `json:"lastFullManifest"`   // <ts>.json
	Increments        []string  `json:"increments"`         // <ts>.json, in apply order
	UpdatedAt         time.Time `json:"updatedAt"`
}

// Key paths inside the backend prefix.
const (
	HeadKey         = "HEAD.json"
	ManifestPrefix  = "manifests/"
)

// ManifestKey returns the backend key for a manifest with the given
// stable identifier. The identifier is the sortable manifest filename
// (e.g. "20260606T040000.123456789Z-v0000000000000001").
func ManifestKey(ts string) string { return ManifestPrefix + ts + ".json" }

// FormatTimestamp returns the canonical filename for a manifest at a
// given (time, badger-watermark). UTC, sortable, with a 9-digit
// nanosecond component AND the watermark suffix so two snapshots in
// the same nanosecond at different versions still hash to distinct
// keys.
func FormatTimestamp(t time.Time) string {
	return t.UTC().Format("20060102T150405.000000000Z")
}

// FormatManifestID is the canonical "<ts>-v<until>" filename ID for a
// manifest. Used by PutManifest to guarantee uniqueness across
// closely-spaced snapshots.
func FormatManifestID(t time.Time, until uint64) string {
	return fmt.Sprintf("%s-v%016d", FormatTimestamp(t), until)
}
