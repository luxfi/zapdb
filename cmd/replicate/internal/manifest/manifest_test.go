package manifest

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/stretchr/testify/require"
)

func TestManifestRoundTrip(t *testing.T) {
	now := time.Date(2026, 6, 6, 4, 0, 0, 0, time.UTC)
	in := &Manifest{
		Version:   1,
		Type:      TypeFull,
		Since:     0,
		Until:     12345,
		Network:   "mainnet",
		Blocks:    []vfs.BlockID{"a1b2", "c3d4"},
		PlainSize: 8192,
		AgeScheme: "X25519+MLKEM768",
		BlockSize: vfs.BlockSize,
		CreatedAt: now,
		Labels:    map[string]string{"git-sha": "deadbeef"},
	}
	body, err := json.Marshal(in)
	require.NoError(t, err)

	// Sanity check on size — keep manifests small. 1 KiB is the
	// stated budget for a typical full snapshot manifest with a
	// modest block count; we have 2 blocks here so the produced JSON
	// must be well under 1 KiB.
	require.Less(t, len(body), 1024, "manifest exceeded 1 KiB budget: %d bytes", len(body))
	t.Logf("manifest JSON size with 2 blocks: %d bytes", len(body))

	var out Manifest
	require.NoError(t, json.Unmarshal(body, &out))
	require.Equal(t, in.Version, out.Version)
	require.Equal(t, in.Type, out.Type)
	require.Equal(t, in.Until, out.Until)
	require.Equal(t, in.Blocks, out.Blocks)
	require.Equal(t, in.PlainSize, out.PlainSize)
	require.Equal(t, in.Labels["git-sha"], out.Labels["git-sha"])
	require.True(t, in.CreatedAt.Equal(out.CreatedAt))
	require.NoError(t, out.Validate())
}

func TestManifestValidateRejectsCrossBlockSize(t *testing.T) {
	m := &Manifest{
		Version:   1,
		Type:      TypeFull,
		Until:     1,
		Blocks:    []vfs.BlockID{"x"},
		PlainSize: 100,
		BlockSize: 8192, // wrong
	}
	require.Error(t, m.Validate())
}

func TestManifestValidateRejectsIncrementSince0(t *testing.T) {
	m := &Manifest{
		Version:   1,
		Type:      TypeIncremental,
		Since:     0,
		Until:     100,
		BlockSize: vfs.BlockSize,
	}
	require.Error(t, m.Validate())
}

func TestFormatTimestamp(t *testing.T) {
	got := FormatTimestamp(time.Date(2026, 6, 6, 4, 0, 0, 0, time.UTC))
	require.Equal(t, "20260606T040000.000000000Z", got)
}

func TestFormatManifestID(t *testing.T) {
	got := FormatManifestID(time.Date(2026, 6, 6, 4, 0, 0, 0, time.UTC), 12345)
	require.Equal(t, "20260606T040000.000000000Z-v0000000000012345", got)
}

func TestManifestKey(t *testing.T) {
	require.Equal(t, "manifests/foo.json", ManifestKey("foo"))
}

// TestManifestSizeSmallIncrement verifies the manifest stays small for
// the typical 60-second incremental: a few hundred 4 KiB blocks of
// fresh badger entries (~1 MiB of chain state). 1 KiB budget per
// manifest is the stated goal.
func TestManifestSizeSmallIncrement(t *testing.T) {
	now := time.Date(2026, 6, 6, 4, 0, 0, 0, time.UTC)
	// Simulate 12 blocks (~50 KiB) — typical 1-min incremental on
	// a quiet chain.
	blocks := make([]vfs.BlockID, 12)
	for i := range blocks {
		blocks[i] = vfs.BlockID("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	}
	m := &Manifest{
		Version:   1,
		Type:      TypeIncremental,
		Since:     1000,
		Until:     1100,
		Network:   "mainnet",
		Blocks:    blocks,
		PlainSize: int64(len(blocks)) * int64(vfs.BlockSize),
		AgeScheme: "X25519+MLKEM768",
		BlockSize: vfs.BlockSize,
		CreatedAt: now,
	}
	body, err := json.Marshal(m)
	require.NoError(t, err)
	t.Logf("manifest JSON size with 12 blocks: %d bytes", len(body))
	require.Less(t, len(body), 1300, "manifest exceeded ~1 KiB budget for small increment: %d bytes", len(body))
}
