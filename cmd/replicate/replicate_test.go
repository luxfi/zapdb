// End-to-end test of the replicate → restore cycle.
//
// Boots an in-memory hanzoai/vfs backed by a file:// backend, writes a
// 1000-entry ZapDB, takes a full Backup() through the chunk writer,
// adds 100 more entries, takes an incremental Backup() into a second
// manifest, then restores both into a fresh ZapDB and verifies every
// key is present.
//
// The test bypasses the replicator daemon's loop (and bypasses the
// luxd-coexistence read-only open path) because we own both writes
// and reads in-process.
package main_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	"github.com/luxfi/age"
	badger "github.com/luxfi/zapdb"
	"github.com/stretchr/testify/require"

	"github.com/luxfi/zapdb/cmd/replicate/internal/manifest"
	"github.com/luxfi/zapdb/cmd/replicate/internal/replica"
)

func TestReplicateRestoreRoundTrip(t *testing.T) {
	ctx := context.Background()

	root := t.TempDir()
	srcPath := filepath.Join(root, "src")
	dstPath := filepath.Join(root, "dst")
	storePath := filepath.Join(root, "store")
	require.NoError(t, mkAll(srcPath, dstPath, storePath))

	// vfs over file:// + age (classical X25519 is fine for tests)
	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	rec := id.Recipient()

	be, err := backend.Open(ctx, "file://"+storePath)
	require.NoError(t, err)
	defer be.Close()

	crypto, err := vfs.NewCrypto([]age.Recipient{rec}, []age.Identity{id})
	require.NoError(t, err)
	v, err := vfs.New(vfs.Config{Backend: be, Crypto: crypto, CacheMax: 4 << 20})
	require.NoError(t, err)
	defer v.Close()

	// Phase 1 — write 1000 keys into the source DB, full backup.
	src := openDB(t, srcPath)
	writeKeys(t, src, 0, 1000)
	full := streamBackup(t, ctx, src, v, 0)
	require.NoError(t, src.Close())

	require.Equal(t, manifest.TypeFull, full.Type)
	require.Greater(t, full.Until, uint64(0), "full backup must advance the badger watermark")
	require.NotEmpty(t, full.Blocks, "full backup must emit at least one block")

	store := manifest.NewStore(be)
	fullKey, err := store.PutManifest(ctx, full)
	require.NoError(t, err)
	head := &manifest.HEAD{
		Network:          "test",
		CurrentVersion:   full.Until,
		LastFullManifest: fullKey,
	}
	require.NoError(t, store.PutHead(ctx, head))

	// Phase 2 — append 100 more keys, incremental backup since
	// full.Until.
	src = openDB(t, srcPath)
	writeKeys(t, src, 1000, 1100)
	inc := streamBackup(t, ctx, src, v, full.Until)
	require.NoError(t, src.Close())

	require.Equal(t, manifest.TypeIncremental, inc.Type)
	require.Equal(t, full.Until, inc.Since)
	require.Greater(t, inc.Until, inc.Since)
	require.NotEmpty(t, inc.Blocks, "incremental backup with 100 new keys must emit blocks")

	incKey, err := store.PutManifest(ctx, inc)
	require.NoError(t, err)
	head.CurrentVersion = inc.Until
	head.Increments = append(head.Increments, incKey)
	require.NoError(t, store.PutHead(ctx, head))

	// Phase 3 — restore into a fresh DB and verify every key.
	dst := openDB(t, dstPath)

	// Apply full
	fullM, err := store.GetManifest(ctx, fullKey)
	require.NoError(t, err)
	rFull := replica.NewChunkReader(ctx, v, fullM.Blocks, fullM.PlainSize)
	require.NoError(t, dst.Load(rFull, 16))

	// Apply incremental
	incM, err := store.GetManifest(ctx, incKey)
	require.NoError(t, err)
	rInc := replica.NewChunkReader(ctx, v, incM.Blocks, incM.PlainSize)
	require.NoError(t, dst.Load(rInc, 16))

	verifyKeys(t, dst, 0, 1100)
	require.NoError(t, dst.Close())
}

func streamBackup(t *testing.T, ctx context.Context, db *badger.DB, v *vfs.VFS, since uint64) *manifest.Manifest {
	t.Helper()
	cw := replica.NewChunkWriter(ctx, v)
	until, err := db.Backup(cw, since)
	require.NoError(t, err)
	require.NoError(t, cw.Close())

	mType := manifest.TypeFull
	if since > 0 {
		mType = manifest.TypeIncremental
	}
	return &manifest.Manifest{
		Version:   1,
		Type:      mType,
		Since:     since,
		Until:     until,
		Network:   "test",
		Blocks:    cw.Blocks(),
		PlainSize: cw.PlainSize(),
		BlockSize: vfs.BlockSize,
	}
}

func openDB(t *testing.T, path string) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	opts.SyncWrites = false // test perf
	db, err := badger.Open(opts)
	require.NoError(t, err)
	return db
}

func writeKeys(t *testing.T, db *badger.DB, start, end int) {
	t.Helper()
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		for i := start; i < end; i++ {
			k := []byte(fmt.Sprintf("k%06d", i))
			v := []byte(fmt.Sprintf("v%06d-padding-to-make-the-stream-non-trivial-and-flush-multiple-pages", i))
			if err := txn.Set(k, v); err != nil {
				return err
			}
		}
		return nil
	}))
}

func verifyKeys(t *testing.T, db *badger.DB, start, end int) {
	t.Helper()
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		for i := start; i < end; i++ {
			k := []byte(fmt.Sprintf("k%06d", i))
			item, err := txn.Get(k)
			if err != nil {
				return fmt.Errorf("get k%06d: %w", i, err)
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("valcopy k%06d: %w", i, err)
			}
			wantPrefix := fmt.Sprintf("v%06d", i)
			if got := string(val); got[:len(wantPrefix)] != wantPrefix {
				return fmt.Errorf("k%06d: want prefix %q, got %q", i, wantPrefix, got)
			}
		}
		return nil
	}))
}

func mkAll(paths ...string) error {
	for _, p := range paths {
		if err := makeDir(p); err != nil {
			return err
		}
	}
	return nil
}
