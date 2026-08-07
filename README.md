# KChat Drive — Go Gateway

The server-side component of KChat Drive: an HTTP gateway that stores
end-to-end encrypted file content and metadata. The gateway never holds
plaintext file content or Drive encryption keys — it is an untrusted storage
provider from the client's perspective.

## Quick Start

### Prerequisites

| Tool | Version |
| --- | --- |
| Go | 1.25+ |
| Docker | latest |
| Postgres | 16+ (via Docker) |

### Development

```bash
# Option A: Docker (Postgres + gateway)
docker compose -f deploy/dev/docker-compose.yml up -d
curl http://localhost:8080/healthz  # → "ok"

# Option B: Native gateway + Docker Postgres
docker compose -f deploy/dev/docker-compose.yml up -d postgres
go run ./cmd/drive-gateway -addr :8080
```

The gateway auto-migrates the Postgres schema on startup (idempotent
`CREATE TABLE IF NOT EXISTS` + `ON CONFLICT DO NOTHING`). No manual `psql`
step is needed.

### Build & Test

```bash
go build ./...
go test ./...
```

### Production Deployment

```bash
cd deploy/sme
cp .env.example .env  # fill in Wasabi + Postgres credentials
docker compose -f docker-compose.production.yml up -d

# Nightly backup (add to host cron):
./backup.sh

# Zero-downtime upgrade:
./upgrade.sh
```

## Binaries

### drive-gateway

```
cmd/drive-gateway/main.go
```

The API + edge + L1 cache in one process. Run N replicas behind Traefik
for zero-downtime deploys.

**Flags:**
- `-config <path>` — path to gateway config JSON (defaults to
  `$DRIVE_GATEWAY_CONFIG`, then dev config)
- `-addr <host:port>` — listen address (default `:8080`)

**Dev config** (no config file): in-memory cache, local filesystem blob
store at `/tmp/kchat-drive-dev`, Postgres at
`postgres://postgres:postgres@localhost:5432/kdrive?sslmode=disable`.

**Production config**: JSON file with Wasabi credentials, disk cache, and
circuit breaker settings. Rendered from a template via `envsubst` at
container start.

### drive-worker

```
cmd/drive-worker/main.go
```

Background jobs: async Wasabi promotion, repair/checksum sampling, orphan
purge, nightly backup, and guardrail/billing rollups.

**Flags:**
- `-config <path>` — path to worker config JSON (defaults to
  `$DRIVE_WORKER_CONFIG`, then dev config)

## Project Structure

```
kdrive/
├── cmd/
│   ├── drive-gateway/main.go       # API + edge + L1 cache binary
│   └── drive-worker/main.go        # Background jobs binary
├── internal/
│   ├── server/
│   │   ├── server.go               # Gateway wiring: store, cache, pipeline, mux
│   │   └── drive_demo.go           # Demo API HTTP handlers (/v1/* endpoints)
│   ├── metadata/
│   │   ├── store.go                # Postgres metadata store (CRUD, migrations)
│   │   ├── drive_demo.go           # Demo metadata structs + queries
│   │   └── status_store.go         # Blob status tracking for pipeline
│   ├── blobio/
│   │   └── pipeline.go             # L1 cache-aside write/read + singleflight L2 restore
│   ├── config/
│   │   └── config.go               # Gateway + worker JSON config loader
│   ├── worker/
│   │   ├── worker.go               # Job orchestration
│   │   ├── promote.go              # CACHED → COMMITTED_DURABLE promotion
│   │   └── repair.go               # Checksum sampling for durable blobs
│   ├── audit/                      # Audit log helpers
│   ├── blob/                       # Blob lifecycle helpers
│   ├── download/                   # Download authorization
│   ├── erasureledger/              # Erasure ledger for DR safety
│   ├── placement/                  # Blob placement strategy
│   ├── purge/                      # Orphan/multipart purge
│   ├── quota/                      # Quota reservation helpers
│   ├── rehydrate/                  # L2 → L1 rehydration
│   ├── repair/                     # Repair scan helpers
│   ├── replication/                # Cross-provider replication
│   └── upload/                     # Upload session helpers
├── pkg/
│   ├── blobstore/
│   │   ├── blobstore.go            # BlobStore + BlobInventory interfaces
│   │   ├── local_fs_dev/           # Dev/CI filesystem adapter
│   │   ├── wasabi/                 # Production Wasabi adapter (AWS SDK v2)
│   │   └── mock/                   # Mock adapter for tests
│   ├── hotcache/
│   │   ├── hotcache.go             # Cache interface
│   │   ├── memory.go               # In-memory cache (dev)
│   │   ├── disk.go                 # Disk cache (production, NVMe/SSD)
│   │   └── policy.go               # LRU eviction policy
│   ├── wasabiguardrails/
│   │   └── guardrails.go           # Fair-use egress / min-storage / hit-ratio
│   └── contracttest/
│       └── contracttest.go         # Shared BlobStore conformance suite
├── deploy/
│   ├── migrations/
│   │   ├── 001_init.sql            # Core schema (tenants, files, versions, outbox, ledger)
│   │   ├── 002_blob_placements.sql # Blob placement tracking
│   │   └── 003_drive_demo.sql      # Demo seed data (tenants, folders, users)
│   ├── dev/
│   │   └── docker-compose.yml      # Dev stack (Postgres + gateway)
│   └── sme/
│       ├── docker-compose.production.yml  # Production (Traefik + 2 gateways + worker + Postgres)
│       ├── .env.example                   # Environment template
│       ├── backup.sh                      # Nightly pg_dump → Wasabi
│       ├── upgrade.sh                     # Zero-downtime upgrade
│       ├── gateway/gateway.json.tmpl      # Gateway config template
│       ├── worker/worker.json.tmpl        # Worker config template
│       └── traefik/                       # Traefik static config
├── Dockerfile                      # Multi-stage Go build → Alpine runtime
├── go.mod
└── go.sum
```

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

| Variable | Default | Description |
| --- | --- | --- |
| `KDRIVE_HOST` | required | Domain name for Traefik TLS |
| `LETSENCRYPT_EMAIL` | required | Let's Encrypt registration email |
| `POSTGRES_USER` | postgres | Postgres user |
| `POSTGRES_PASSWORD` | required | Postgres password |
| `POSTGRES_DB` | kdrive | Postgres database name |
| `WASABI_ENDPOINT` | required | Wasabi S3 endpoint |
| `WASABI_REGION` | required | Wasabi region |
| `WASABI_BUCKET` | required | Wasabi bucket name |
| `WASABI_ACCESS_KEY` | required | Wasabi access key |
| `WASABI_SECRET_KEY` | required | Wasabi secret key |
| `KDRIVE_CACHE_MAX_BYTES` | 400 GiB | L1 disk cache capacity |
| `KDRIVE_BACKUP_CRON` | `17 3 * * *` | Nightly backup cron expression |
| `KDRIVE_BACKUP_RETENTION_DAYS` | 14 | Backup retention window |
| `KDRIVE_PROMOTE_INTERVAL_MS` | 30000 | Promotion sweep interval |
| `KDRIVE_PROMOTE_BATCH_SIZE` | 100 | Max blobs per promotion sweep |
| `KDRIVE_PROMOTE_PARALLELISM` | 4 | Concurrent promote workers |
| `KDRIVE_REPAIR_INTERVAL_MS` | 300000 | Repair scan interval |
| `KDRIVE_REPAIR_SAMPLE_COUNT` | 10 | Durable blobs to sample per repair scan |
| `KDRIVE_QUEUE_DEPTH_ALERT` | 1000 | CACHED blob count alert threshold |
| `KDRIVE_CIRCUIT_BREAKER_ENABLED` | false | Enable Wasabi circuit breaker |
| `KDRIVE_CIRCUIT_BREAKER_THRESHOLD` | 10 | Consecutive failures before breaker opens |

## API Endpoints

### Core Endpoints

| Method | Path | Description |
| --- | --- | --- |
| GET | `/healthz` | Liveness probe |
| GET | `/readyz` | Readiness probe (checks Postgres + store) |
| GET | `/metrics` | Prometheus metrics |

### Demo API Endpoints (`/v1/`)

| Method | Path | Description |
| --- | --- | --- |
| GET | `/v1/tenants` | List all demo tenants |
| GET | `/v1/folders?parent=` | List root or child folders |
| POST | `/v1/folders` | Create a folder |
| GET | `/v1/folders/{id}/children` | Get folder + sub-folders + nodes |
| POST | `/v1/folders/{id}/children` | Create a node (file) in folder |
| GET | `/v1/nodes/{id}` | Get node metadata |
| POST | `/v1/uploads:initiate` | Start an upload session |
| POST | `/v1/uploads/{sid}/chunks/{ordinal}:register` | Register a chunk |
| POST | `/v1/versions/{vid}:authorizeDownload` | Authorize a download |
| GET | `/v1/versions/{vid}` | Get version metadata |
| GET | `/v1/domains/{id}` | Get encryption domain |
| POST | `/v1/domains/{id}:rotate` | Rotate domain key |
| GET | `/v1/shares` | List share grants |
| POST | `/v1/nodes/{id}/shares` | Create a share grant |
| DELETE | `/v1/shares/{id}` | Revoke a share grant |
| GET | `/v1/vectors` | KDRV1 cross-language test vectors |

All `/v1/` endpoints accept `X-Demo-Tenant` and `X-Demo-User` headers for
multi-tenancy routing. CORS headers (`Access-Control-Allow-Origin: *`) are
set on all demo endpoints. COOP/COEP headers are set for WASM web sample
compatibility.

## Docker

### Dev Image

```bash
docker build -t kdrive-gateway .
docker run -p 8080:8080 kdrive-gateway drive-gateway -addr :8080
```

### Production Stack

The production `docker-compose.production.yml` runs:

- **Traefik** — TLS termination, load balancing across gateway replicas
- **gateway-1, gateway-2** — two gateway replicas with independent disk caches
- **worker** — background job runner
- **Postgres** — single instance with WAL archiving for PITR

The Dockerfile is a multi-stage build: `golang:1.25-bookworm` builder →
`alpine:3.20` runtime. The runtime image includes `gettext` (for `envsubst`)
and runs as non-root user `kdrive` (UID 1000).

## Architecture Defaults (Plan §10)

- No WORM/Object Lock in phase 1 (Wasabi versioning only)
- Single Postgres instance (no HA) acceptable for v1
- Erasure ledger kept as the one DR safety net
- L1 disk cache on NVMe (400 GiB per replica default)
- Circuit breaker optional (off by default)
