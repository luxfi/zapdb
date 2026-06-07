package replica

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/luxfi/age"
	"github.com/stretchr/testify/require"
)

func newTestVFS(t *testing.T) *vfs.VFS {
	t.Helper()
	store := t.TempDir()
	be, err := backend.Open(context.Background(), "file://"+store)
	require.NoError(t, err)
	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	crypto, err := vfs.NewCrypto([]age.Recipient{id.Recipient()}, []age.Identity{id})
	require.NoError(t, err)
	v, err := vfs.New(vfs.Config{Backend: be, Crypto: crypto, CacheMax: 1 << 20})
	require.NoError(t, err)
	return v
}

// roundtrip is the simplest possible test: write N bytes through a
// ChunkWriter, read them back through a ChunkReader, byte-compare.
func TestChunkRoundTrip_ExactBlockMultiple(t *testing.T) {
	roundtrip(t, vfs.BlockSize*3) // exact multiple
}

func TestChunkRoundTrip_OneByteOver(t *testing.T) {
	roundtrip(t, vfs.BlockSize*3+1)
}

func TestChunkRoundTrip_OneByteUnder(t *testing.T) {
	roundtrip(t, vfs.BlockSize*3-1)
}

func TestChunkRoundTrip_SubBlock(t *testing.T) {
	roundtrip(t, 17)
}

func TestChunkRoundTrip_LargeRandom(t *testing.T) {
	roundtrip(t, 1_234_567)
}

func TestChunkRoundTrip_Empty(t *testing.T) {
	v := newTestVFS(t)
	defer v.Close()
	ctx := context.Background()
	cw := NewChunkWriter(ctx, v)
	require.NoError(t, cw.Close())
	require.Equal(t, int64(0), cw.PlainSize())
	require.Empty(t, cw.Blocks())

	r := NewChunkReader(ctx, v, nil, 0)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Empty(t, out)
}

func roundtrip(t *testing.T, n int) {
	t.Helper()
	v := newTestVFS(t)
	defer v.Close()
	ctx := context.Background()

	payload := make([]byte, n)
	_, err := rand.Read(payload)
	require.NoError(t, err)

	cw := NewChunkWriter(ctx, v)
	wrote, err := cw.Write(payload)
	require.NoError(t, err)
	require.Equal(t, n, wrote)
	require.NoError(t, cw.Close())
	require.Equal(t, int64(n), cw.PlainSize())

	cr := NewChunkReader(ctx, v, cw.Blocks(), cw.PlainSize())
	got, err := io.ReadAll(cr)
	require.NoError(t, err)
	require.Equal(t, n, len(got), "length mismatch")
	if !bytes.Equal(payload, got) {
		// Find first mismatch position
		for i := 0; i < len(payload) && i < len(got); i++ {
			if payload[i] != got[i] {
				t.Fatalf("first byte mismatch at %d (block %d, offset %d): want %#x, got %#x; blocks=%v",
					i, i/vfs.BlockSize, i%vfs.BlockSize, payload[i], got[i], cw.Blocks())
			}
		}
	}
}
