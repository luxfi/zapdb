// Command zapdb-replicate is the sidecar + init-container binary for
// streaming-replicating Lux ZapDB chain state to S3 (or any
// hanzoai/vfs-supported backend) with per-block age encryption.
//
// Subcommands:
//
//	replicate        — daemon: ticks at --interval, publishes a manifest
//	                    per tick, advances HEAD.json.
//	restore          — one-shot: reads HEAD, applies full + increments
//	                    into an empty --path. Intended for init containers.
//	prune            — garbage-collect manifests + blocks older than the
//	                    Nth-most-recent full snapshot.
//	version          — print build version.
//
// Both daemon and one-shot modes read the same YAML config file
// (default: /etc/zapdb-replicate/config.yaml). Flags override.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file"
	_ "github.com/hanzoai/vfs/pkg/backend/s3"
	"github.com/luxfi/age"

	"github.com/luxfi/zapdb/cmd/replicate/internal/replica"
	"github.com/luxfi/zapdb/cmd/replicate/internal/restore"
)

var version = "dev"

// Config is the YAML on-disk shape. Single config file may declare
// multiple databases, each with its own replica destination.
type Config struct {
	Databases []DatabaseConfig `yaml:"databases"`
}

// DatabaseConfig wires one ZapDB to one or more replicas. The replicate
// daemon spawns one goroutine per (database, replica) pair.
type DatabaseConfig struct {
	Path     string          `yaml:"path"`     // /data/db/mainnet
	Network  string          `yaml:"network"`  // "mainnet" | "testnet" | ...
	Replicas []ReplicaConfig `yaml:"replicas"`
}

// ReplicaConfig is one destination. Only S3 + file are supported today.
type ReplicaConfig struct {
	Type                string        `yaml:"type"`                 // "s3" | "file"
	Endpoint            string        `yaml:"endpoint"`             // for s3-compatible
	Bucket              string        `yaml:"bucket"`
	Prefix              string        `yaml:"prefix"`
	Region              string        `yaml:"region"`
	URL                 string        `yaml:"url"`                  // alternative to the above; e.g. "file:///tmp/x"
	AgeRecipientsPath   string        `yaml:"age-recipients"`       // path to file containing one recipient per line
	AgeKeyPath          string        `yaml:"age-key"`              // path to identity file
	Interval            time.Duration `yaml:"interval"`             // default 60s
	FullSnapshotEvery   time.Duration `yaml:"full-snapshot-every"`  // default 168h
	CacheBytes          int64         `yaml:"cache-bytes"`          // vfs LRU cap, default 64MiB
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	root := &cobra.Command{
		Use:     "zapdb-replicate",
		Short:   "Encrypted streaming replication for Lux ZapDB",
		Version: version,
	}

	root.AddCommand(replicateCmd(ctx))
	root.AddCommand(restoreCmd(ctx))
	root.AddCommand(pruneCmd(ctx))
	root.AddCommand(versionCmd())

	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "zapdb-replicate:", err)
		os.Exit(1)
	}
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("zapdb-replicate", version)
		},
	}
}

func replicateCmd(ctx context.Context) *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "replicate",
		Short: "Run the replicator daemon (sidecar mode)",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := loadConfig(configPath)
			if err != nil {
				return err
			}
			return runReplicate(ctx, cfg)
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "/etc/zapdb-replicate/config.yaml", "config file path")
	return cmd
}

func restoreCmd(ctx context.Context) *cobra.Command {
	var (
		dbPath        string
		network       string
		replicaURL    string
		ageRecipsPath string
		ageKeyPath    string
		cacheBytes    int64
		force         bool
	)
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore a ZapDB from S3 (init-container mode)",
		RunE: func(_ *cobra.Command, _ []string) error {
			if dbPath == "" {
				return errors.New("--path is required")
			}
			if replicaURL == "" {
				return errors.New("--url is required (e.g. s3://bucket/prefix?endpoint=...)")
			}
			be, v, err := openReplica(ctx, replicaURL, ageRecipsPath, ageKeyPath, cacheBytes)
			if err != nil {
				return err
			}
			defer be.Close()
			defer v.Close()
			return restore.Run(ctx, restore.Config{
				DBPath:         dbPath,
				Network:        network,
				VFS:            v,
				Backend:        be,
				SkipIfNotEmpty: !force,
			})
		},
	}
	cmd.Flags().StringVar(&dbPath, "path", "", "target ZapDB directory (e.g. /data/db/mainnet)")
	cmd.Flags().StringVar(&network, "network", "", "sanity-check network name (mainnet|testnet|...)")
	cmd.Flags().StringVar(&replicaURL, "url", "", "backend URL (e.g. s3://lux-snapshots/mainnet/zapdb?endpoint=http://minio:9000)")
	cmd.Flags().StringVar(&ageRecipsPath, "age-recipients", "", "path to age recipients file")
	cmd.Flags().StringVar(&ageKeyPath, "age-key", "", "path to age identity file (required for restore)")
	cmd.Flags().Int64Var(&cacheBytes, "cache-bytes", 64<<20, "vfs in-memory cache cap")
	cmd.Flags().BoolVar(&force, "force", false, "restore even if target dir is non-empty")
	return cmd
}

func pruneCmd(ctx context.Context) *cobra.Command {
	var (
		replicaURL    string
		ageRecipsPath string
		ageKeyPath    string
		cacheBytes    int64
		keepFulls     int
		dryRun        bool
	)
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "GC manifests + blocks older than the Nth-most-recent full snapshot",
		RunE: func(_ *cobra.Command, _ []string) error {
			if replicaURL == "" {
				return errors.New("--url is required")
			}
			be, v, err := openReplica(ctx, replicaURL, ageRecipsPath, ageKeyPath, cacheBytes)
			if err != nil {
				return err
			}
			defer be.Close()
			defer v.Close()
			return restore.Prune(ctx, restore.PruneConfig{
				VFS:       v,
				Backend:   be,
				KeepFulls: keepFulls,
				DryRun:    dryRun,
			})
		},
	}
	cmd.Flags().StringVar(&replicaURL, "url", "", "backend URL")
	cmd.Flags().StringVar(&ageRecipsPath, "age-recipients", "", "path to age recipients file")
	cmd.Flags().StringVar(&ageKeyPath, "age-key", "", "path to age identity file")
	cmd.Flags().Int64Var(&cacheBytes, "cache-bytes", 16<<20, "vfs in-memory cache cap")
	cmd.Flags().IntVar(&keepFulls, "keep-fulls", 4, "keep the N most recent full snapshots")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "log only, do not delete")
	return cmd
}

func runReplicate(ctx context.Context, cfg *Config) error {
	if len(cfg.Databases) == 0 {
		return errors.New("config has no databases")
	}

	errCh := make(chan error, len(cfg.Databases))
	for _, dbCfg := range cfg.Databases {
		if len(dbCfg.Replicas) == 0 {
			return fmt.Errorf("db %s: no replicas configured", dbCfg.Path)
		}
		for _, rep := range dbCfg.Replicas {
			rep := rep // capture
			dbCfg := dbCfg
			go func() {
				errCh <- runOneReplica(ctx, dbCfg, rep)
			}()
		}
	}

	// Wait for ctx cancel or any goroutine to surface a hard error.
	// Per-cycle errors are logged + retried; this only triggers on
	// setup failure.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func runOneReplica(ctx context.Context, dbCfg DatabaseConfig, rep ReplicaConfig) error {
	url := replicaBackendURL(rep)
	be, v, err := openReplica(ctx, url, rep.AgeRecipientsPath, rep.AgeKeyPath, rep.CacheBytes)
	if err != nil {
		return fmt.Errorf("db %s: open replica %s: %w", dbCfg.Path, url, err)
	}
	defer be.Close()
	defer v.Close()

	r, err := replica.New(replica.Config{
		DBPath:            dbCfg.Path,
		Network:           dbCfg.Network,
		VFS:               v,
		Backend:           be,
		Interval:          rep.Interval,
		FullSnapshotEvery: rep.FullSnapshotEvery,
		AgeSchemeLabel:    ageSchemeLabel(rep),
	})
	if err != nil {
		return err
	}
	return r.Run(ctx)
}

func replicaBackendURL(rep ReplicaConfig) string {
	if rep.URL != "" {
		return rep.URL
	}
	if rep.Type == "" || rep.Type == "s3" {
		u := "s3://" + rep.Bucket
		if rep.Prefix != "" {
			u += "/" + strings.TrimLeft(rep.Prefix, "/")
		}
		q := []string{}
		if rep.Endpoint != "" {
			q = append(q, "endpoint="+rep.Endpoint)
		}
		if rep.Region != "" {
			q = append(q, "region="+rep.Region)
		}
		if len(q) > 0 {
			u += "?" + strings.Join(q, "&")
		}
		return u
	}
	return rep.URL
}

func ageSchemeLabel(rep ReplicaConfig) string {
	if rep.AgeRecipientsPath == "" {
		return "none"
	}
	return "age" // luxfi/age handles X25519+ML-KEM-768 transparently
}

func openReplica(ctx context.Context, url, recipsPath, keyPath string, cacheBytes int64) (backend.Backend, *vfs.VFS, error) {
	be, err := backend.Open(ctx, url)
	if err != nil {
		return nil, nil, fmt.Errorf("backend.Open(%s): %w", url, err)
	}
	recs, ids, err := loadAge(recipsPath, keyPath)
	if err != nil {
		be.Close()
		return nil, nil, err
	}
	crypto, err := vfs.NewCrypto(recs, ids)
	if err != nil {
		be.Close()
		return nil, nil, err
	}
	if cacheBytes <= 0 {
		cacheBytes = 64 << 20
	}
	v, err := vfs.New(vfs.Config{
		Backend:  be,
		Crypto:   crypto,
		CacheMax: cacheBytes,
	})
	if err != nil {
		be.Close()
		return nil, nil, err
	}
	return be, v, nil
}

func loadAge(recipientsPath, keyPath string) ([]age.Recipient, []age.Identity, error) {
	var recs []age.Recipient
	if recipientsPath != "" {
		body, err := os.ReadFile(recipientsPath)
		if err != nil {
			return nil, nil, fmt.Errorf("read --age-recipients %s: %w", recipientsPath, err)
		}
		parsed, err := age.ParseRecipients(strings.NewReader(string(body)))
		if err != nil {
			return nil, nil, fmt.Errorf("parse age recipients: %w", err)
		}
		recs = parsed
	}
	var ids []age.Identity
	if keyPath != "" {
		body, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, nil, fmt.Errorf("read --age-key %s: %w", keyPath, err)
		}
		parsed, err := age.ParseIdentities(strings.NewReader(string(body)))
		if err != nil {
			return nil, nil, fmt.Errorf("parse age identities: %w", err)
		}
		ids = parsed
	}
	if len(recs) == 0 && len(ids) == 0 {
		return nil, nil, errors.New("at least one of --age-recipients or --age-key required")
	}
	return recs, ids, nil
}

func loadConfig(path string) (*Config, error) {
	if path == "" {
		return nil, errors.New("config path required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	body, err := os.ReadFile(abs)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", abs, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(body, &cfg); err != nil {
		return nil, fmt.Errorf("decode config %s: %w", abs, err)
	}
	return &cfg, nil
}

var _ = io.EOF // keep imports stable across edits
