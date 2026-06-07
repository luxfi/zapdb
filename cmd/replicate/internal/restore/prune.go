package restore

import (
	"context"
	"fmt"
	"sort"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	log "github.com/luxfi/log"

	"github.com/luxfi/zapdb/cmd/replicate/internal/manifest"
)

// PruneConfig wires a single prune run.
type PruneConfig struct {
	VFS       *vfs.VFS
	Backend   backend.Backend
	KeepFulls int  // keep the N most recent full snapshots; older fulls and their increments are deleted
	DryRun    bool // log only, do not delete
}

// Prune removes manifests older than the Nth-most-recent full
// snapshot, plus the blocks that ONLY those manifests reference.
//
// Block GC: walk every surviving manifest, build a set of referenced
// block IDs. Any block in the to-delete manifests that is NOT in the
// survivor set can be deleted.
func Prune(ctx context.Context, cfg PruneConfig) error {
	if cfg.KeepFulls < 1 {
		return fmt.Errorf("prune: --keep-fulls must be >= 1")
	}
	if cfg.VFS == nil || cfg.Backend == nil {
		return fmt.Errorf("prune: VFS and Backend required")
	}

	store := manifest.NewStore(cfg.Backend)

	allFulls, err := store.ListFulls(ctx)
	if err != nil {
		return err
	}
	allIncs, err := store.ListIncrements(ctx)
	if err != nil {
		return err
	}
	sort.Strings(allFulls)
	sort.Strings(allIncs)

	if len(allFulls) <= cfg.KeepFulls {
		log.Info(fmt.Sprintf("[zapdb-prune] %d fulls present, keep=%d — nothing to prune", len(allFulls), cfg.KeepFulls))
		return nil
	}

	// Split fulls into keep + drop sets.
	dropFulls := allFulls[:len(allFulls)-cfg.KeepFulls]
	keepFulls := allFulls[len(allFulls)-cfg.KeepFulls:]

	// Increments are tied to the most recent preceding full's
	// timestamp. We drop any increment whose key sorts BEFORE the
	// oldest surviving full.
	oldestKept := keepFulls[0]

	var dropIncs []string
	var keepIncs []string
	for _, k := range allIncs {
		if k < oldestKept {
			dropIncs = append(dropIncs, k)
		} else {
			keepIncs = append(keepIncs, k)
		}
	}

	dropManifests := append([]string{}, dropFulls...)
	dropManifests = append(dropManifests, dropIncs...)
	keepManifests := append([]string{}, keepFulls...)
	keepManifests = append(keepManifests, keepIncs...)

	// Build survivor block set.
	survivors := make(map[vfs.BlockID]struct{})
	for _, k := range keepManifests {
		m, err := store.GetManifest(ctx, k)
		if err != nil {
			return err
		}
		for _, b := range m.Blocks {
			survivors[b] = struct{}{}
		}
	}

	// Collect candidates for deletion from the drop manifests.
	deletable := make(map[vfs.BlockID]struct{})
	for _, k := range dropManifests {
		m, err := store.GetManifest(ctx, k)
		if err != nil {
			return err
		}
		for _, b := range m.Blocks {
			if _, kept := survivors[b]; !kept {
				deletable[b] = struct{}{}
			}
		}
	}

	log.Info(fmt.Sprintf("[zapdb-prune] dropping %d manifests (%d fulls, %d increments), %d blocks; dryRun=%v",
		len(dropManifests), len(dropFulls), len(dropIncs), len(deletable), cfg.DryRun))

	if cfg.DryRun {
		return nil
	}

	// Delete blocks first (orphan blocks with no manifest are
	// recoverable; a manifest pointing at deleted blocks is not).
	for bid := range deletable {
		if err := cfg.Backend.Delete(ctx, bid.Path()); err != nil {
			return fmt.Errorf("prune: delete block %s: %w", bid, err)
		}
	}
	for _, k := range dropManifests {
		if err := cfg.Backend.Delete(ctx, k); err != nil {
			return fmt.Errorf("prune: delete manifest %s: %w", k, err)
		}
	}
	return nil
}
