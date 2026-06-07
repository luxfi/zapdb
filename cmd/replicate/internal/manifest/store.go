package manifest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hanzoai/vfs/pkg/backend"
)

// Store reads + writes manifests through a backend.Backend, using the
// configured key prefix. The backend already roots its own prefix; Store
// only adds the manifest/HEAD layout on top.
type Store struct {
	be backend.Backend
}

// NewStore wraps a backend with the manifest layout.
func NewStore(be backend.Backend) *Store { return &Store{be: be} }

// PutManifest writes a manifest. Returns the chosen key.
func (s *Store) PutManifest(ctx context.Context, m *Manifest) (string, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now().UTC()
	}
	key := ManifestKey(FormatManifestID(m.CreatedAt, m.Until))
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "", fmt.Errorf("manifest: marshal: %w", err)
	}
	if err := s.be.Put(ctx, key, body); err != nil {
		return "", fmt.Errorf("manifest: put %s: %w", key, err)
	}
	return key, nil
}

// GetManifest fetches and decodes a manifest by key (e.g.
// "manifests/20260606T040000Z.json").
func (s *Store) GetManifest(ctx context.Context, key string) (*Manifest, error) {
	body, err := s.be.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("manifest: get %s: %w", key, err)
	}
	var m Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("manifest: decode %s: %w", key, err)
	}
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("manifest: validate %s: %w", key, err)
	}
	return &m, nil
}

// PutHead atomically updates the HEAD pointer. S3 has no compare-and-
// swap so we just PUT; the daemon is the only writer for a given
// prefix (enforced by the local state file).
func (s *Store) PutHead(ctx context.Context, h *HEAD) error {
	h.Version = 1
	h.UpdatedAt = time.Now().UTC()
	body, err := json.MarshalIndent(h, "", "  ")
	if err != nil {
		return fmt.Errorf("head: marshal: %w", err)
	}
	if err := s.be.Put(ctx, HeadKey, body); err != nil {
		return fmt.Errorf("head: put: %w", err)
	}
	return nil
}

// GetHead reads HEAD. Returns (nil, nil) if HEAD does not exist yet
// (fresh prefix). Any other error is wrapped.
func (s *Store) GetHead(ctx context.Context) (*HEAD, error) {
	body, err := s.be.Get(ctx, HeadKey)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("head: get: %w", err)
	}
	var h HEAD
	if err := json.Unmarshal(body, &h); err != nil {
		return nil, fmt.Errorf("head: decode: %w", err)
	}
	if h.Version != 1 {
		return nil, fmt.Errorf("head: unsupported schema version %d", h.Version)
	}
	return &h, nil
}

// ListFulls returns all full-snapshot manifest keys under the prefix,
// sorted ascending by timestamp. Used by prune and HEAD recovery.
func (s *Store) ListFulls(ctx context.Context) ([]string, error) {
	return s.listManifestsByType(ctx, TypeFull)
}

// ListIncrements returns all incremental-snapshot manifest keys under
// the prefix, sorted ascending by timestamp.
func (s *Store) ListIncrements(ctx context.Context) ([]string, error) {
	return s.listManifestsByType(ctx, TypeIncremental)
}

func (s *Store) listManifestsByType(ctx context.Context, t Type) ([]string, error) {
	keys, errs := s.be.List(ctx, ManifestPrefix)
	var out []string
	for k := range keys {
		m, err := s.GetManifest(ctx, k)
		if err != nil {
			return nil, err
		}
		if m.Type == t {
			out = append(out, k)
		}
	}
	if err := <-errs; err != nil {
		return nil, fmt.Errorf("manifest: list: %w", err)
	}
	return out, nil
}
