package state

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStateRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	in := &State{
		Network:           "mainnet",
		LastVersion:       12345,
		LastSnapshotKind:  "increment",
		LastFullVersion:   12000,
		LastFullTimestamp: time.Date(2026, 6, 6, 0, 0, 0, 0, time.UTC),
	}
	require.NoError(t, Save(path, in))
	out, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, in.Network, out.Network)
	require.Equal(t, in.LastVersion, out.LastVersion)
	require.Equal(t, in.LastFullVersion, out.LastFullVersion)
	require.True(t, in.LastFullTimestamp.Equal(out.LastFullTimestamp))
}

func TestLoadMissingReturnsZero(t *testing.T) {
	s, err := Load(filepath.Join(t.TempDir(), "no-such-file.json"))
	require.NoError(t, err)
	require.NotNil(t, s)
	require.Equal(t, uint64(0), s.LastVersion)
}

func TestPath(t *testing.T) {
	got := Path("/data/db/mainnet")
	require.Equal(t, "/data/db/.mainnet.zapdb-replicate-state.json", got)
}
