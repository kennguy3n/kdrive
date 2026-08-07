# KChat Drive

Simplified Linode-VM + Wasabi storage subsystem for KChat Drive. See
`/Users/Ken/.devin/plans/plan-e358f95bfb4502ec.md` for the full
architecture plan.

## Build & test

```bash
go build ./...
go test ./...
```

## Layout

- `cmd/drive-gateway` — API + edge + L1 cache in one binary (N replicas behind Traefik).
- `cmd/drive-worker` — background jobs (promotion, repair, purge, backup, guardrail rollups).
- `pkg/blobstore` — provider-agnostic `BlobStore` / `BlobInventory` interfaces.
- `pkg/blobstore/local_fs_dev` — dev/CI filesystem adapter.
- `pkg/blobstore/wasabi` — production Wasabi adapter (AWS SDK v2, S3-compatible).
- `pkg/hotcache` — L1 hot object cache interface + memory/disk implementations.
- `pkg/wasabiguardrails` — Wasabi fair-use egress / min-storage / hit-ratio guardrails.
- `pkg/contracttest` — shared BlobStore conformance suite.
- `internal/blobio` — L1+L2 write/read pipeline with singleflight L2 restore.
- `internal/metadata` — Postgres metadata store (trimmed schema, outbox, erasure ledger).
- `internal/server` — gateway HTTP handler wiring.
- `internal/worker` — drive-worker job loops.
- `internal/config` — gateway/worker JSON config loader.
- `deploy/migrations/001_init.sql` — trimmed Postgres schema.
- `deploy/sme/` — single-VM-pool production docker-compose, backup + upgrade scripts.
- `deploy/dev/` — dev docker-compose (Postgres + gateway, no Wasabi).

## Deployment

```bash
# Production (single Linode VM pool):
cp deploy/sme/.env.example deploy/sme/.env  # fill in secrets
docker compose -f deploy/sme/docker-compose.production.yml up -d
# The gateway auto-migrates the schema on startup; no manual psql step.
# Nightly backup (add to host cron):
./deploy/sme/backup.sh
# Zero-downtime upgrade:
./deploy/sme/upgrade.sh

# Dev:
docker compose -f deploy/dev/docker-compose.yml up -d
```

## Architecture defaults (from plan §10)

- No WORM/Object Lock in phase 1 (Wasabi versioning only).
- Single Postgres instance (no HA) acceptable for v1.
- Erasure ledger kept as the one DR safety net.
