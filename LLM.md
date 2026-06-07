# LLM.md - Hanzo Zapdb

## Overview
Go module: github.com/luxfi/zapdb/v4

## Tech Stack
- **Language**: Go

## Build & Run
```bash
go build ./...
go test ./...
```

## Replicate sidecar (`cmd/replicate/`)

Streams ZapDB chain state to S3 (or any `hanzoai/vfs`-backed store) via
ZapDB's native `Backup(w, lastVersion)` + `Load(r)` API. Per-block age
encryption. One binary, three subcommands:

- `zapdb-replicate replicate` — daemon, runs as a sidecar next to lqd. Ticks
  every `--interval`, publishes a manifest per tick, advances `HEAD.json`.
- `zapdb-replicate restore` — one-shot, runs as an init-container. Reads
  HEAD, applies full + increments into an empty `--path`.
- `zapdb-replicate prune` — GC manifests + blocks older than the Nth-most-
  recent full snapshot.

Build:
```bash
go build ./cmd/replicate
docker build -t zapdb-replicate -f deploy/replicate/Dockerfile .
```

K8s wiring lives in `deploy/replicate/`: `sidecar.yaml`, `init-container.yaml`,
plus a helm chart under `deploy/replicate/chart/`. Operator wires these
patches onto the lqd StatefulSet — no separate Deployment.

Folded in from the standalone `luxfi/zapdb-replicate` repo (2026-06-07):
that repo was duplicate scaffolding around ZapDB's native streaming-
replication API. Canonical home is here.

## Structure
```
zapdb/
  CHANGELOG.md
  CODE_OF_CONDUCT.md
  CONTRIBUTING.md
  LICENSE
  Makefile
  README.md
  SECURITY.md
  VERSIONING.md
  backup.go
  backup_test.go
  badger/
  batch.go
  batch_test.go
  changes.sh
  compaction.go
```

## Key Files
- `README.md` -- Project documentation
- `go.mod` -- Go module definition
- `Makefile` -- Build automation
