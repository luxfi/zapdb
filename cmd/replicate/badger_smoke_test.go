package main_test

import (
	"bytes"
	"testing"

	badger "github.com/luxfi/zapdb"
	"github.com/stretchr/testify/require"
)

// TestBadgerSmoke confirms our understanding of badger's
// Backup/Load is correct, independent of the chunkwriter/vfs stack.
//
// If this fails, the bug is in our use of badger, not in the chunk
// layer.
func TestBadgerSmoke(t *testing.T) {
	srcOpts := badger.DefaultOptions(t.TempDir())
	srcOpts.Logger = nil
	src, err := badger.Open(srcOpts)
	require.NoError(t, err)
	require.NoError(t, src.Update(func(txn *badger.Txn) error {
		for i := 0; i < 10; i++ {
			if err := txn.Set([]byte{byte('a' + i)}, []byte{byte('A' + i)}); err != nil {
				return err
			}
		}
		return nil
	}))

	var buf bytes.Buffer
	until, err := src.Backup(&buf, 0)
	require.NoError(t, err)
	require.Greater(t, until, uint64(0))
	require.NoError(t, src.Close())

	dstOpts := badger.DefaultOptions(t.TempDir())
	dstOpts.Logger = nil
	dst, err := badger.Open(dstOpts)
	require.NoError(t, err)
	require.NoError(t, dst.Load(&buf, 16))

	require.NoError(t, dst.View(func(txn *badger.Txn) error {
		for i := 0; i < 10; i++ {
			item, err := txn.Get([]byte{byte('a' + i)})
			require.NoError(t, err)
			val, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, []byte{byte('A' + i)}, val, "key %c", 'a'+i)
		}
		return nil
	}))
	require.NoError(t, dst.Close())
}
