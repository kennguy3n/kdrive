# KChat Drive — Go Gateway

The server-side component of **KChat Drive**: an HTTP gateway that stores
end-to-end encrypted (E2EE) file content and metadata on behalf of KChat
clients. The gateway is an **untrusted storage provider** from the client's
perspective — it never holds plaintext file content, plaintext Drive
encryption keys, domain keys, or share-grant keys. All cryptographic
operations happen client-side (in the Rust/WASM SDK); the gateway only ever
sees ciphertext, wrapped keys, and opaque metadata.

This repository contains the Go implementation of the simplified
Linode-VM + Wasabi storage subsystem described in the architecture plan
(see `ARCHITECTURE.md` for the full design). The companion client SDK lives
in `kdrive-rust-sdk`.

## Table of Contents

- [Quick Start](#quick-start)
- [Build & Test](#build--test)
- [Binaries](#binaries)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [API Endpoints](#api-endpoints)
- [Docker](#docker)
- [Production Deployment](#production-deployment)
- [Observability](#observability)
- [Testing](#testing)
- [Architecture Defaults](#architecture-defaults)
- [Related Documents](#related-documents)

## Quick Start

### Prerequisites

| Tool | Version |
| --- | --- |
| Go | 1.25+ |
| Docker | latest |
| Postgres | 16+ (via Docker) |
| `aws` CLI | for `backup.sh` only |

### Development

Two ways to run the gateway locally:

```bash
# Option A: Docker (Postgres + gateway, no Wasabi credentials needed)
docker compose -f deploy/dev/docker-compose.yml up -d
curl http://localhost:8080/healthz   # → "ok"
curl http://localhost:8080/readyz    # → "ready"

# Option B: Native gateway + Docker Postgres
docker compose -f deploy/dev/docker-compose.yml up -d postgres
go run ./cmd/drive-gateway -addr :8080
```

In dev mode the gateway uses:

- An **in-memory L1 cache**.
- The **`local_fs_dev` BlobStore adapter** rooted at `/tmp/kchat-drive-dev`
  (no Wasabi credentials required).
- A **Postgres metadata store** at
  `postgres://postgres:postgres@localhost:5432/kdrive?sslmode=disable`.

The gateway **auto-migrates the Postgres schema on startup**. Migrations are
idempotent (`CREATE TABLE IF NOT EXISTS` + `ON CONFLICT DO NOTHING`), so no
manual `psql` step is needed.

## Build & Test

```bash
go build ./...
go test ./...
```

The test suite includes:

- Unit tests for the `blobio` pipeline, `hotcache`, `metadata` store,
  `wasabi` adapter (circuit breaker), and `wasabiguardrails`.
- The shared `pkg/contracttest` BlobStore conformance suite, run against the
  `local_fs_dev` adapter (and `wasabi` when credentials are available).
- Worker job tests with injected fakes (`worker.NewWithDeps`).

## Binaries

The repo builds two binaries from a single multi-stage Dockerfile.

### drive-gateway

```
cmd/drive-gateway/main.go
```

The **API + edge + L1 cache** in one process (architecture plan §6). Run N
replicas behind Traefik for zero-downtime deploys. Each replica:

- Serves HTTP on `:8080`.
- Maintains its own L1 disk cache (NVMe/SSD, 400 GiB default).
- Connects to a shared Postgres instance.
- Talks to Wasabi (production) or `local_fs_dev` (dev) for durable storage.
- Auto-migrates the Postgres schema on startup.

**Flags:**

- `-config <path>` — path to gateway config JSON (defaults to
  `$DRIVE_GATEWAY_CONFIG`, then a built-in dev config).
- `-addr <host:port>` — listen address (default `:8080`).

**Dev config** (no config file): in-memory cache, local filesystem blob
store at `/tmp/kchat-drive-dev`, Postgres at
`postgres://postgres:postgres@localhost:5432/kdrive?sslmode=disable`.

**Production config**: JSON file with Wasabi credentials, disk cache, and
circuit-breaker settings. Rendered from a template via `envsubst` at
container start so secrets never land in the image.

Graceful shutdown: on `SIGINT`/`SIGTERM` the gateway drains the HTTP server
(30s timeout), then closes the pipeline (waits for in-flight cache writes),
the L1 cache, and the Postgres pool.

### drive-worker

```
cmd/drive-worker/main.go
```

Background job runner. Does not serve HTTP. Runs the following jobs in
parallel goroutines, each on its own ticker:

| Job | Purpose |
| --- | --- |
| `PromotionJob` | Promote `CACHED` blobs to Wasabi (`COMMITTED_DURABLE`) |
| `RepairJob` | Sample durable blobs and verify their SHA-256 |
| `PurgeJob` | Sweep orphaned multipart uploads |
| `BackupJob` | Trigger / observe the nightly `pg_dump` → Wasabi backup |
| `GuardrailRollupJob` | Roll up cache-hit ratio + CACHED queue depth, emit alerts |

**Flags:**

- `-config <path>` — path to worker config JSON (defaults to
  `$DRIVE_WORKER_CONFIG`, then a built-in dev config).

In dev mode (no Postgres DSN) the worker runs no jobs and idles.

## Project Structure

```
kdrive/
├── cmd/
│   ├── drive-gateway/main.go       # API + edge + L1 cache binary
│   └── drive-worker/main.go        # Background jobs binary
├── internal/
│   ├── server/
│   │   ├── server.go               # Gateway wiring: store, cache, pipeline, mux
│   │   ├── drive_api.go            # Drive REST API HTTP handlers (/v1/* endpoints)
│   │   └── metrics.go              # Prometheus-format /metrics exporter
│   ├── metadata/
│   │   ├── store.go                # Postgres metadata store (CRUD, migrations)
│   │   ├── drive_store.go          # Drive metadata structs + queries (folders, nodes, domains, shares)
│   │   ├── status_store.go         # blob_placements table (blobio.StatusStore)
│   │   └── embedded.go             # Embedded SQL migrations
│   ├── blobio/
│   │   ├── pipeline.go             # L1 cache-aside write/read + singleflight L2 restore
│   │   └── pipeline_test.go
│   ├── config/
│   │   └── config.go               # Gateway + worker JSON config loader
│   ├── worker/
│   │   ├── worker.go               # Job orchestration + dependency wiring
│   │   ├── jobs.go                 # PromotionJob, RepairJob, PurgeJob, BackupJob, GuardrailRollupJob
│   │   └── worker_test.go
│   ├── audit/                      # (reserved) audit log helpers
│   ├── blob/                       # (reserved) blob lifecycle helpers
│   ├── download/                   # (reserved) download authorization
│   ├── erasureledger/              # (reserved) erasure ledger for DR safety
│   ├── placement/                  # (reserved) blob placement strategy
│   ├── purge/                      # (reserved) orphan/multipart purge
│   ├── quota/                      # (reserved) quota reservation helpers
│   ├── rehydrate/                  # (reserved) L2 → L1 rehydration
│   ├── repair/                     # (reserved) repair scan helpers
│   ├── replication/                # (reserved) cross-provider replication
│   └── upload/                     # (reserved) upload session helpers
├── pkg/
│   ├── blobstore/
│   │   ├── blobstore.go            # BlobStore + BlobInventory interfaces + error taxonomy
│   │   ├── local_fs_dev/           # Dev/CI filesystem adapter
│   │   ├── wasabi/                 # Production Wasabi adapter (AWS SDK v2, S3-compatible)
│   │   │   ├── wasabi.go
│   │   │   └── circuit_breaker.go
│   │   └── mock/                   # Mock adapter for tests
│   ├── hotcache/
│   │   ├── hotcache.go             # Cache interface + eviction policy
│   │   ├── memory_cache.go         # In-memory cache (dev)
│   │   ├── disk_cache.go           # Disk cache (production, NVMe/SSD)
│   │   └── hotcache_test.go
│   ├── wasabiguardrails/
│   │   └── guardrails.go           # Fair-use egress / min-storage / hit-ratio types
│   └── contracttest/
│       └── contracttest.go         # Shared BlobStore conformance suite
├── deploy/
│   ├── migrations/
│   │   ├── 001_init.sql            # Core schema (tenants, files, versions, outbox, ledger)
│   │   ├── 002_blob_placements.sql # Blob placement tracking (decoupled from file_versions)
│   │   └── 003_drive_demo.sql      # Drive schema (folders, nodes, domains, shares) + demo seed data
│   ├── dev/
│   │   └── docker-compose.yml      # Dev stack (Postgres + gateway)
│   └── sme/
│       ├── docker-compose.production.yml  # Production (Traefik + 2 gateways + worker + Postgres)
│       ├── .env.example                   # Environment template
│       ├── backup.sh                      # Nightly pg_dump + WAL → Wasabi
│       ├── upgrade.sh                     # Zero-downtime rolling upgrade
│       ├── gateway/gateway.json.tmpl      # Gateway config template
│       ├── worker/worker.json.tmpl        # Worker config template
│       └── traefik/                       # Traefik static config
├── Dockerfile                      # Multi-stage Go build → Alpine runtime
├── go.mod
├── go.sum
├── AGENTS.md
├── ARCHITECTURE.md
└── README.md
```

> The `internal/{audit,blob,download,erasureledger,placement,purge,quota,
> rehydrate,repair,replication,upload}` directories are currently reserved
> placeholders for future decompositions; their logic lives in `internal/worker`
> and `internal/metadata` today.

## Configuration

### Gateway Config (JSON)

```json
{
  "env": "production",
  "http_addr": ":8080",
  "postgres_dsn": "postgres://user:pass@host:5432/kdrive?sslmode=require",
  "wasabi": {
    "endpoint": "s3.ap-southeast-1.wasabisys.com",
    "region": "ap-southeast-1",
    "bucket": "kchat-drive-prod",
    "access_key": "...",
    "secret_key": "...",
    "use_path_style": false
  },
  "cache": {
    "type": "disk",
    "disk_root_path": "/var/lib/kdrive/cache",
    "max_bytes": 429496729600
  },
  "wasabi_circuit_breaker_enabled": false,
  "wasabi_circuit_breaker_threshold": 10
}
```

### Worker Config (JSON)

```json
{
  "env": "production",
  "postgres_dsn": "postgres://user:pass@host:5432/kdrive?sslmode=require",
  "wasabi": { "..." },
  "cache": { "type": "disk", "disk_root_path": "/var/lib/kdrive/cache", "max_bytes": 429496729600 },
  "backup_cron": "17 3 * * *",
  "backup_retention_days": 14,
  "promote_interval_ms": 30000,
  "promote_batch_size": 100,
  "promote_parallelism": 4,
  "repair_interval_ms": 300000,
  "repair_sample_count": 10,
  "queue_depth_alert": 1000,
  "wasabi_circuit_breaker_enabled": false,
  "wasabi_circuit_breaker_threshold": 10
}
```

### Environment Variables (Production)

The compose entrypoint renders the JSON templates with `envsubst`, so every
field below is sourced from the `.env` file at container start.

| Variable | Default | Description |
| --- | --- | --- |
| `KDRIVE_HOST` | required | Domain name for Traefik TLS |
| `LETSENCRYPT_EMAIL` | required | Let's Encrypt registration email |
| `POSTGRES_USER` | `postgres` | Postgres user |
| `POSTGRES_PASSWORD` | required | Postgres password |
| `POSTGRES_DB` | `kdrive` | Postgres database name |
| `WASABI_ENDPOINT` | required | Wasabi S3 endpoint |
| `WASABI_REGION` | required | Wasabi region |
| `WASABI_BUCKET` | required | Wasabi bucket name |
| `WASABI_ACCESS_KEY` | required | Wasabi access key |
| `WASABI_SECRET_KEY` | required | Wasabi secret key |
| `KDRIVE_CACHE_MAX_BYTES` | `429496729600` (400 GiB) | L1 disk cache capacity per replica |
| `KDRIVE_BACKUP_CRON` | `17 3 * * *` | Nightly backup cron expression |
| `KDRIVE_BACKUP_RETENTION_DAYS` | `14` | Backup retention window |
| `KDRIVE_PROMOTE_INTERVAL_MS` | `30000` | Promotion sweep interval |
| `KDRIVE_PROMOTE_BATCH_SIZE` | `100` | Max blobs per promotion sweep |
| `KDRIVE_PROMOTE_PARALLELISM` | `4` | Concurrent promote workers |
| `KDRIVE_REPAIR_INTERVAL_MS` | `300000` | Repair scan interval |
| `KDRIVE_REPAIR_SAMPLE_COUNT` | `10` | Durable blobs to sample per repair scan |
| `KDRIVE_QUEUE_DEPTH_ALERT` | `1000` | CACHED blob count alert threshold |
| `KDRIVE_CIRCUIT_BREAKER_ENABLED` | `false` | Enable Wasabi circuit breaker |
| `KDRIVE_CIRCUIT_BREAKER_THRESHOLD` | `10` | Consecutive failures before breaker opens |

The config loader validates all tuning knobs at startup and fails fast on
negative values or missing production-required fields (`postgres_dsn`,
Wasabi endpoint/region/bucket/credentials).

## API Endpoints

### Core Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/healthz` | Liveness probe (always `200 "ok"`) |
| `GET` | `/readyz` | Readiness probe (checks Postgres `Ping` + a Wasabi `Head` probe) |
| `GET` | `/metrics` | Prometheus-format metrics (see [Observability](#observability)) |

### Drive API Endpoints (`/v1/`)

The Drive REST API is mounted only when Postgres is configured. It
implements the §17 client API surface (plan Part C) and supports all
three privacy modes (Secured / Advanced / Max). The web sample uses
these endpoints to demonstrate end-to-end encryption against the live
gateway.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/v1/tenants` | List all tenants |
| `GET` | `/v1/folders?parent=` | List root or child folders |
| `POST` | `/v1/folders` | Create a folder |
| `GET` | `/v1/folders/{id}/children` | Get folder + sub-folders + nodes |
| `POST` | `/v1/folders/{id}/children` | Create a node (file) in folder |
| `GET` | `/v1/nodes/{id}` | Get node metadata |
| `POST` | `/v1/uploads:initiate` | Start an upload session |
| `POST` | `/v1/uploads/{sid}/chunks/{ordinal}:register` | Register a chunk |
| `POST` | `/v1/versions/{vid}:authorizeDownload` | Authorize a download |
| `GET` | `/v1/versions/{vid}` | Get version metadata |
| `GET` | `/v1/domains/{id}` | Get encryption domain |
| `POST` | `/v1/domains/{id}:rotate` | Rotate domain key |
| `GET` | `/v1/shares` | List share grants |
| `POST` | `/v1/nodes/{id}/shares` | Create a share grant |
| `DELETE` | `/v1/shares/{id}` | Revoke a share grant |
| `GET` | `/v1/vectors` | KDRV1 cross-language test vectors |

All `/v1/` endpoints:

- Accept `X-Demo-Tenant` and `X-Demo-User` headers for multi-tenancy
  routing in the current implementation. These are demo-only auth —
  production replaces `getUserTenant` in `internal/server/drive_api.go`
  with KChat's real identity layer (session tokens, device certificates).
  The gateway is untrusted and does not authenticate these headers — it
  trusts the caller for demo / web-sample purposes only.
- Set CORS headers (`Access-Control-Allow-Origin: *`) so the React web
  sample can call them directly.
- Are wrapped by `withWebHeaders`, which sets `Cross-Origin-Opener-Policy:
  same-origin` and `Cross-Origin-Embedder-Policy: require-corp` for
  SharedWorker + threaded WASM compatibility in the web sample.

## Docker

### Dev Image

```bash
docker build -t kdrive-gateway .
docker run -p 8080:8080 kdrive-gateway drive-gateway -addr :8080
```

### Production Stack

The production `docker-compose.production.yml` runs:

- **Traefik** — TLS termination (Let's Encrypt TLS-ALPN-01), load balancing
  across gateway replicas, HTTP→HTTPS redirect.
- **gateway-1, gateway-2** — two gateway replicas with independent disk
  cache volumes (`gateway-1-cache`, `gateway-2-cache`).
- **worker** — background job runner (no HTTP, no Traefik labels).
- **Postgres 16** — single instance with WAL archiving enabled for PITR.

The Dockerfile is a multi-stage build: `golang:1.25-bookworm` builder →
`alpine:3.20` runtime. The runtime image includes `gettext` (for `envsubst`)
and `ca-certificates`, and runs as non-root user `kdrive` (UID 1000). The
compose file mounts the NVMe cache volume owned by this UID.

## Production Deployment

```bash
cd deploy/sme
cp .env.example .env  # fill in Wasabi + Postgres credentials + KDRIVE_HOST
docker compose -f docker-compose.production.yml up -d
```

The gateway auto-migrates the schema on startup; no manual `psql` step is
required.

### Nightly Backup

Add to host cron (or run manually):

```bash
./deploy/sme/backup.sh
```

`backup.sh` runs `pg_dump` inside the postgres container, compresses with
`gzip -9`, uploads to `s3://$WASABI_BUCKET/backups/postgres/`, syncs the
WAL archive volume to Wasabi (for PITR), and prunes backups older than the
retention window.

### Zero-Downtime Upgrade

```bash
./deploy/sme/upgrade.sh
```

Builds the new image, then rolls each gateway replica one at a time.
Traefik drains in-flight requests to the old container before the new one
takes over, so clients see no interruption. The worker is rolled last.

## Observability

### Metrics (`/metrics`)

The gateway exposes Prometheus text-format metrics. No external dependency
is required — the exporter is a lightweight hand-rolled handler in
`internal/server/metrics.go`.

| Metric | Type | Description |
| --- | --- | --- |
| `kdrive_cache_entries` | gauge | L1 cache entry count |
| `kdrive_cache_bytes_used` | gauge | L1 cache bytes used |
| `kdrive_cache_bytes_limit` | gauge | L1 cache capacity |
| `kdrive_cache_hits_total` | counter | L1 cache hits |
| `kdrive_cache_misses_total` | counter | L1 cache misses |
| `kdrive_cache_evictions_total` | counter | L1 cache evictions |
| `kdrive_cache_hit_ratio` | gauge | Computed hit ratio |
| `kdrive_promote_attempts_total` | counter | Promote attempts |
| `kdrive_promote_successes_total` | counter | Successful promotes |
| `kdrive_promote_failures_total` | counter | Failed promotes |
| `kdrive_promote_skipped_total` | counter | Promotes skipped (HEAD found already-durable) |
| `kdrive_promote_duration_avg_ms` | gauge | Average promote duration |

Worker-side metrics (repair checks, mismatches, queue-depth samples/alerts)
are emitted as structured logs (`slog` JSON) since the worker does not
serve HTTP.

### Logs

Both binaries emit structured JSON logs via `log/slog` at `INFO` level by
default. Key log streams:

- Gateway: startup, request errors, auto-migration, readiness probe
  failures.
- Worker: per-job batch summaries (`promotion: batch complete`,
  `repair: scan complete`, `guardrail rollup: cache stats`), queue-depth
  warnings, checksum-mismatch alerts.

### Readiness

`/readyz` checks Postgres (`Ping`) and the durable store. For Wasabi it
issues a `Head` on a known non-existent probe key — `nil` error or
`ErrNotFound` both mean the store is reachable; any other error (network,
auth, timeout) means not ready. The probe has a 5-second deadline.

## Testing

```bash
go test ./...
```

Notable test packages:

- `internal/blobio` — pipeline write/read/promote, singleflight dedup,
  multipart promote, crash-recovery idempotency (HEAD before PUT).
- `internal/metadata` — Postgres store CRUD, migration idempotency,
  `blob_placements` upsert / `MarkDurable`.
- `internal/worker` — job loops with injected fakes via `NewWithDeps`.
- `pkg/hotcache` — memory + disk cache, LRU eviction, hot-pin policy.
- `pkg/blobstore/wasabi` — circuit breaker open/half-open/closed
  transitions.
- `pkg/wasabiguardrails` — budget cap math, residual billable window.
- `pkg/contracttest` — the shared BlobStore conformance suite (conditional
  create, ambiguous-PUT-timeout HEAD+verify, byte-range GET, multipart
  create/upload/complete/abort, copy-within-provider, retention
  monotonic extension, legal-hold set/clear, purge-all-versions, inventory
  list/versions/multipart/parts, batch delete, and the negative capability
  test proving phase-1 adapters reject direct staging).

## Architecture Defaults (Plan §10)

- **No WORM / Object Lock in phase 1** — Wasabi bucket versioning only.
- **Single Postgres instance** (no HA) is acceptable for v1; WAL archiving
  + nightly `pg_dump` provide PITR with ~15 min RPO.
- **Erasure ledger** is the one DR safety net kept from v1 (ADR-021).
- **L1 disk cache on NVMe** — 400 GiB per replica default, LRU with hot-pin.
- **Circuit breaker optional** — off by default; opens after N consecutive
  Wasabi failures and probes after 30 seconds.
- **Object keys are opaque** — they carry no tenant, user, group, file, or
  folder identifiers (§3 invariant 14).
- **KChat SHA-256 is authoritative** — provider ETags are never used as
  the integrity check (§13.3).

## Related Documents

- `ARCHITECTURE.md` — full system architecture, data flow, schema,
  security boundaries, threat model, and DR design.
- `AGENTS.md` — build/test commands and layout summary for AI agents.
- `KChat-Privacy-Drive-Storage-Architecture-v1.0.md` (in the kdrive-rust-sdk
  repo) — the canonical architecture document this gateway implements.
- `kdrive-rust-sdk` — the cross-platform Rust client SDK (WASM, UniFFI,
  NAPI) that performs all client-side cryptography.
