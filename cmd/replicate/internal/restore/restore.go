// Package restore is the init-container side: read HEAD.json from the
// backend, walk the manifest chain (full + increments in order), stream
// each one through hanzoai/vfs into db.Load(r). After this completes,
// the target ZapDB at the configured path holds a byte-equivalent copy
// of whatever the replicator most recently published.
package restore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	log "github.com/luxfi/log"
	badger "github.com/luxfi/zapdb"

	"github.com/luxfi/zapdb/cmd/replicate/internal/manifest"
	"github.com/luxfi/zapdb/cmd/replicate/internal/replica"
	"github.com/luxfi/zapdb/cmd/replicate/internal/state"
)

// Config wires a single restore run.
type Config struct {
	DBPath  string         // target path; created if it does not exist
	Network string         // sanity check — must match HEAD/manifests
	VFS     *vfs.VFS
	Backend backend.Backend
	// SkipIfNotEmpty: if true (default), restore aborts when the target
	// already contains data. Set false to force restore over an
	// existing dir.
	SkipIfNotEmpty bool
}

// Run executes the restore. Returns nil if the target is already
// populated and SkipIfNotEmpty is true.
func Run(ctx context.Context, cfg Config) error {
	if cfg.DBPath == "" {
		return errors.New("restore: DBPath required")
	}
	if cfg.VFS == nil || cfg.Backend == nil {
		return errors.New("restore: VFS and Backend required")
	}
	if cfg.SkipIfNotEmpty {
		empty, err := isDirEmpty(cfg.DBPath)
		if err != nil {
			return err
		}
		if !empty {
			log.Info(fmt.Sprintf("[zapdb-restore] %s is not empty, skipping (use --force to override)", cfg.DBPath))
			return nil
		}
	}

	if err := os.MkdirAll(cfg.DBPath, 0o755); err != nil {
		return fmt.Errorf("restore: mkdir %s: %w", cfg.DBPath, err)
	}

	store := manifest.NewStore(cfg.Backend)
	head, err := store.GetHead(ctx)
	if err != nil {
		return err
	}
	if head == nil {
		return errors.New("restore: HEAD.json not found in backend — nothing to restore")
	}
	if cfg.Network != "" && head.Network != "" && head.Network != cfg.Network {
		return fmt.Errorf("restore: network mismatch: requested %q, HEAD has %q", cfg.Network, head.Network)
	}
	if head.LastFullManifest == "" {
		return errors.New("restore: HEAD has no LastFullManifest")
	}

	// Open the target DB writable. No BypassLockGuard — we want a
	// regular lock since luxd is NOT running.
	db, err := openWritable(cfg.DBPath)
	if err != nil {
		return fmt.Errorf("restore: open %s: %w", cfg.DBPath, err)
	}
	defer db.Close()

	// Full snapshot
	if err := applyManifest(ctx, db, cfg.VFS, store, head.LastFullManifest, manifest.TypeFull); err != nil {
		return err
	}

	// Increments in order
	for _, key := range head.Increments {
		if err := applyManifest(ctx, db, cfg.VFS, store, key, manifest.TypeIncremental); err != nil {
			return err
		}
	}

	// Persist a watermark so a follow-on replicator picks up here.
	st := &state.State{
		Network:           head.Network,
		LastVersion:       head.CurrentVersion,
		LastSnapshotKind:  string(manifest.TypeIncremental),
		LastFullVersion:   head.CurrentVersion,
		LastFullTimestamp: time.Now().UTC(),
	}
	if err := state.Save(state.Path(cfg.DBPath), st); err != nil {
		log.Warn(fmt.Sprintf("[zapdb-restore] could not write state file: %v (non-fatal)", err))
	}

	log.Info(fmt.Sprintf("[zapdb-restore] complete: path=%s currentVersion=%d", cfg.DBPath, head.CurrentVersion))
	return nil
}

func applyManifest(ctx context.Context, db *badger.DB, v *vfs.VFS, store *manifest.Store, key string, expectType manifest.Type) error {
	m, err := store.GetManifest(ctx, key)
	if err != nil {
		return err
	}
	if m.Type != expectType {
		return fmt.Errorf("restore: %s expected %s, got %s", key, expectType, m.Type)
	}
	log.Info(fmt.Sprintf("[zapdb-restore] applying %s type=%s blocks=%d bytes=%d (until=%d)",
		key, m.Type, len(m.Blocks), m.PlainSize, m.Until))

	r := replica.NewChunkReader(ctx, v, m.Blocks, m.PlainSize)
	if err := db.Load(r, 16); err != nil {
		return fmt.Errorf("restore: apply %s: %w", key, err)
	}
	return nil
}

func openWritable(path string) (*badger.DB, error) {
	opts := badger.DefaultOptions(path)
	opts.SyncWrites = true
	opts.Logger = nil
	return badger.Open(opts)
}

func isDirEmpty(path string) (bool, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, fmt.Errorf("restore: stat %s: %w", path, err)
	}
	// Ignore lock + state side files when judging emptiness — they
	// may exist from a half-init.
	for _, e := range entries {
		name := e.Name()
		if name == "LOCK" || name == "MANIFEST" {
			return false, nil
		}
		// any other data file means populated
		if filepath.Ext(name) != "" {
			return false, nil
		}
		return false, nil
	}
	return true, nil
}
