package replica

import (
	"bytes"
	"context"
	"io"
	"testing"

	zapdb "github.com/luxfi/zapdb"
	"github.com/stretchr/testify/require"
)

// TestChunkPreservesBackupStream tees the badger Backup stream into
// BOTH a raw bytes.Buffer and our ChunkWriter in the same call, then
// requires that the bytes read back through the ChunkReader equal the
// raw buffer byte-for-byte. This catches any byte loss / reordering /
// truncation in the chunk layer that round-trip tests with random
// bytes would miss.
func TestChunkPreservesBackupStream(t *testing.T) {
	ctx := context.Background()
	v := newTestVFS(t)
	defer v.Close()

	srcOpts := zapdb.DefaultOptions(t.TempDir())
	srcOpts.Logger = nil
	src, err := zapdb.Open(srcOpts)
	require.NoError(t, err)
	require.NoError(t, src.Update(func(txn *zapdb.Txn) error {
		for i := 0; i < 100; i++ {
			if err := txn.Set([]byte{byte(i)}, []byte{byte(i ^ 0xff)}); err != nil {
				return err
			}
		}
		return nil
	}))

	var raw bytes.Buffer
	cw := NewChunkWriter(ctx, v)
	tee := io.MultiWriter(&raw, cw)

	_, err = src.Backup(tee, 0)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, src.Close())

	cr := NewChunkReader(ctx, v, cw.Blocks(), cw.PlainSize())
	got, err := io.ReadAll(cr)
	require.NoError(t, err)

	require.Equal(t, raw.Len(), len(got), "length mismatch")
	if !bytes.Equal(raw.Bytes(), got) {
		for i := 0; i < raw.Len(); i++ {
			if raw.Bytes()[i] != got[i] {
				t.Fatalf("first mismatch at byte %d (of %d, block %d offset %d): raw=%#x got=%#x",
					i, raw.Len(), i/4096, i%4096, raw.Bytes()[i], got[i])
			}
		}
	}
}
