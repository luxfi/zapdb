package replica

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	log "github.com/luxfi/log"
	zapdb "github.com/luxfi/zapdb"

	"github.com/luxfi/zapdb/cmd/replicate/internal/manifest"
	"github.com/luxfi/zapdb/cmd/replicate/internal/state"
)

// Replica is a single (database, backend) pair that the daemon ticks
// against. Each tick: snapshot ZapDB, stream through vfs, publish a
// manifest, advance HEAD, persist watermark.
type Replica struct {
	cfg     Config
	v       *vfs.VFS
	backend backend.Backend
	store   *manifest.Store
}

// Config wires a single replica.
type Config struct {
	DBPath            string
	Network           string   // e.g. "mainnet"
	VFS               *vfs.VFS // backend + crypto
	Backend           backend.Backend
	StatePath         string        // override state file path; empty -> derived
	Interval          time.Duration // between cycles, default 60s
	FullSnapshotEvery time.Duration // re-snapshot full at this cadence
	AgeSchemeLabel    string        // recorded in manifests for audit
}

// New constructs a Replica. The VFS + Backend are taken in pre-built so
// the daemon can share them across multiple databases.
func New(cfg Config) (*Replica, error) {
	if cfg.DBPath == "" {
		return nil, errors.New("replica: DBPath required")
	}
	if cfg.Network == "" {
		return nil, errors.New("replica: Network required")
	}
	if cfg.VFS == nil || cfg.Backend == nil {
		return nil, errors.New("replica: VFS and Backend required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 60 * time.Second
	}
	if cfg.FullSnapshotEvery <= 0 {
		cfg.FullSnapshotEvery = 168 * time.Hour
	}
	return &Replica{
		cfg:     cfg,
		v:       cfg.VFS,
		backend: cfg.Backend,
		store:   manifest.NewStore(cfg.Backend),
	}, nil
}

// Run loops the replicator until ctx is cancelled. Errors per cycle are
// logged but do not terminate the loop — the next tick retries.
func (r *Replica) Run(ctx context.Context) error {
	log.Info(fmt.Sprintf("[zapdb-replicate] starting replica path=%s net=%s interval=%s", r.cfg.DBPath, r.cfg.Network, r.cfg.Interval))

	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()

	// Run an initial cycle so we publish promptly on boot, before
	// the first ticker fire.
	if err := r.cycleSafe(ctx); err != nil {
		log.Error(fmt.Sprintf("[zapdb-replicate] cycle: %v", err))
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := r.cycleSafe(ctx); err != nil {
				log.Error(fmt.Sprintf("[zapdb-replicate] cycle: %v", err))
			}
		}
	}
}

func (r *Replica) cycleSafe(ctx context.Context) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("zapdb-replicate: panic in cycle: %v", rec)
		}
	}()
	return r.cycle(ctx)
}

func (r *Replica) cycle(ctx context.Context) error {
	statePath := r.cfg.StatePath
	if statePath == "" {
		statePath = state.Path(r.cfg.DBPath)
	}

	st, err := state.Load(statePath)
	if err != nil {
		return err
	}

	// Decide full vs incremental
	now := time.Now().UTC()
	isFull := false
	if st.LastVersion == 0 {
		isFull = true
	} else if st.LastFullTimestamp.IsZero() {
		isFull = true
	} else if now.Sub(st.LastFullTimestamp) >= r.cfg.FullSnapshotEvery {
		isFull = true
	}

	since := st.LastVersion
	if isFull {
		since = 0
	}

	// Open ZapDB read-only with lock-guard bypass so we coexist with luxd.
	db, err := openReadOnly(r.cfg.DBPath)
	if err != nil {
		return fmt.Errorf("zapdb-replicate: open %s: %w", r.cfg.DBPath, err)
	}
	defer db.Close()

	cw := NewChunkWriter(ctx, r.v)
	until, err := db.Backup(cw, since)
	if err != nil {
		return fmt.Errorf("zapdb-replicate: zapdb.Backup(since=%d): %w", since, err)
	}
	if err := cw.Close(); err != nil {
		return fmt.Errorf("zapdb-replicate: chunkwriter close: %w", err)
	}

	// Empty backup (no new entries since last watermark) — skip
	// manifest write so we don't spam the backend with no-op snapshots.
	if !isFull && len(cw.Blocks()) == 0 {
		log.Debug(fmt.Sprintf("[zapdb-replicate] no new entries since v=%d, skipping", since))
		return nil
	}

	m := &manifest.Manifest{
		Version:   1,
		Type:      manifest.TypeIncremental,
		Since:     since,
		Until:     until,
		Network:   r.cfg.Network,
		Blocks:    cw.Blocks(),
		PlainSize: cw.PlainSize(),
		AgeScheme: r.cfg.AgeSchemeLabel,
		BlockSize: vfs.BlockSize,
		CreatedAt: now,
	}
	if isFull {
		m.Type = manifest.TypeFull
		m.Since = 0
	}

	key, err := r.store.PutManifest(ctx, m)
	if err != nil {
		return err
	}
	log.Info(fmt.Sprintf("[zapdb-replicate] published manifest type=%s since=%d until=%d blocks=%d bytes=%d key=%s",
		m.Type, m.Since, m.Until, len(m.Blocks), m.PlainSize, key))

	// Update HEAD
	head, err := r.store.GetHead(ctx)
	if err != nil {
		return err
	}
	if isFull || head == nil {
		head = &manifest.HEAD{
			Network:          r.cfg.Network,
			CurrentVersion:   until,
			LastFullManifest: key,
			Increments:       nil,
		}
	} else {
		head.CurrentVersion = until
		head.Increments = append(head.Increments, key)
	}
	if err := r.store.PutHead(ctx, head); err != nil {
		return err
	}

	// Persist watermark
	st.Network = r.cfg.Network
	st.LastVersion = until
	st.LastSnapshotKind = string(m.Type)
	if isFull {
		st.LastFullVersion = until
		st.LastFullTimestamp = now
	}
	return state.Save(statePath, st)
}

// openReadOnly opens the underlying BadgerDB in read-only mode with
// BypassLockGuard so we coexist with a running luxd. We do not use the
// high-level luxfi/database/zapdb wrapper because it forces SyncWrites
// + GC goroutines that fight a primary writer.
func openReadOnly(path string) (*zapdb.DB, error) {
	opts := zapdb.DefaultOptions(path)
	opts.ReadOnly = true
	opts.BypassLockGuard = true
	opts.Logger = nil
	// Smaller caches in the sidecar — luxd already has the big ones.
	opts.BlockCacheSize = 64 << 20
	opts.IndexCacheSize = 32 << 20
	return zapdb.Open(opts)
}
