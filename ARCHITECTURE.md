# KChat Drive — Architecture

## System Overview

KChat Drive is an end-to-end encrypted (E2EE) file storage system. The Go
gateway is the server-side component — it stores ciphertext and opaque
metadata only, never plaintext or encryption keys.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Browser (Client)                              │
│                                                                      │
│  React UI → TypeScript API Client → WASM (Rust crypto SDK)          │
│  IndexedDB key vault (DEKs, domain keys, Ed25519 keys)              │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ HTTP (ciphertext + wrapped keys only)
                               │ X-Demo-Tenant / X-Demo-User headers
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Go Gateway (drive-gateway)                       │
│                                                                      │
│  ┌──────────┐  ┌───────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │ HTTP Mux │─▶│ Demo API  │─▶│ Metadata     │─▶│ Postgres     │    │
│  │ /healthz │  │ /v1/*     │  │ Store        │  │              │    │
│  │ /readyz  │  │           │  │              │  │ tenants      │    │
│  │ /metrics │  │           │  │              │  │ files        │    │
│  └──────────┘  └───────────┘  └──────────────┘  │ file_versions│    │
│                                                   │ outbox       │    │
│  ┌──────────────────────────────────────┐        │ erasure_ledger│   │
│  │ blobio Pipeline                       │        └──────────────┘    │
│  │  Write: L1 cache → async promote      │                           │
│  │  Read:  L1 hit → return               │                           │
│  │         L1 miss → L2 singleflight GET │                           │
│  └───────────┬──────────────────────────┘                           │
│              │            ┌──────────────┐                          │
│              ├───────────▶│ L1 Hot Cache │ (NVMe disk / memory)     │
│              │            └──────────────┘                          │
│              │            ┌──────────────┐                          │
│              └───────────▶│ BlobStore    │ (Wasabi / local_fs_dev)  │
│                           └──────────────┘                          │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                     Go Worker (drive-worker)                         │
│                                                                      │
│  PromotionJob    — CACHED → COMMITTED_DURABLE (L1 → Wasabi)         │
│  RepairJob       — sample durable blobs, verify SHA-256             │
│  PurgeJob        — sweep orphaned multipart uploads                 │
│  BackupJob       — nightly pg_dump → Wasabi                         │
│  GuardrailRollup — per-tenant egress/cache-hit metrics              │
│                                                                      │
│  Reads outbox table → processes events asynchronously                │
└─────────────────────────────────────────────────────────────────────┘
```

## Process Architecture

The system runs two binary types, deployed as Docker containers:

### drive-gateway (N replicas)

Merges API, edge, and L1 cache into one process (plan §6). Each replica:
- Serves HTTP on `:8080`
- Maintains its own L1 disk cache (NVMe/SSD, 400 GiB default)
- Connects to a shared Postgres instance
- Talks to Wasabi (production) or local filesystem (dev) for durable storage
- Runs behind Traefik for TLS termination and load balancing

### drive-worker (1 instance)

Background job runner that:
- Polls the `outbox` table for async events
- Promotes CACHED blobs to Wasabi (COMMITTED_DURABLE)
- Samples durable blobs for checksum verification (repair)
- Purges orphaned multipart uploads and abandoned write intents
- Triggers nightly `pg_dump` → Wasabi backup
- Rolls up per-tenant guardrail metrics (egress, cache-hit ratio)

## Storage Tiers

### L1 — Hot Cache (NVMe/SSD)

- **Interface**: `pkg/hotcache.Cache` — `Get`, `Put`, `Evict`, `Stats`, `Close`
- **Dev**: in-memory (`hotcache.NewMemoryCache`) with LRU eviction
- **Production**: disk-backed (`hotcache.NewDiskCache`) on NVMe, 400 GiB
- **Content**: ciphertext only, keyed by immutable blob ID
- **Eviction**: LRU by bytes, with pinning for hot/non-evictable entries
- **Auth**: checked before cache lookup; cache key is blob ID, not bearer token

### L2 — Durable Origin (Wasabi)

- **Interface**: `pkg/blobstore.BlobStore` — `Put`, `Head`, `Get`, `Delete`,
  multipart operations, retention, capabilities
- **Dev**: `local_fs_dev` adapter at `/tmp/kchat-drive-dev`
- **Production**: `wasabi` adapter using AWS SDK v2 (S3-compatible API)
- **Content**: ciphertext only, opaque object keys (no tenant/user/file IDs)
- **Integrity**: KChat SHA-256 is authoritative; provider ETags never used
- **Versioning**: Wasabi bucket versioning enabled (no Object Lock in phase 1)

### Metadata — Postgres

- **Interface**: `internal/metadata.Store`
- **Schema**: `deploy/migrations/001_init.sql` (auto-migrated on startup)
- **Content**: tenant config, file/folder metadata, version metadata, upload
  sessions, quota reservations, write intents, outbox events, erasure ledger
- **No plaintext**: encrypted names stored as BYTEA, keys are wrapped/encrypted

## Write Path (Upload)

```
Client                          Gateway                        Worker
──────                          ───────                        ──────
1. Encrypt file (WASM)
   → ciphertext chunks
   → wrapped DEK
   → signed manifest
2. POST /v1/folders/{id}/children
   → create node ───────────▶  metadata.Store.CreateNode
   ← node_id                   → Postgres INSERT
3. POST /v1/uploads:initiate
   → chunk_plan, wrapped_dek ─▶  metadata.Store.CreateUploadSession
   ← session_id                → Postgres INSERT
4. POST /uploads/{sid}/chunks/{i}:register
   → ciphertext_hex ─────────▶  blobio.Pipeline.WriteChunk
   ← blob_key                   → L1 cache PUT (immediate)
                                → outbox: PROMOTE_BLOB event
                                → Postgres INSERT file_version
                                  (commit_state = CACHED)
                                                   │
                                                   ▼
                                              PromotionJob
                                                → L1 GET blob
                                                → Wasabi PUT
                                                  (multipart if large)
                                                → Verify SHA-256 + length
                                                → Postgres UPDATE
                                                  commit_state =
                                                  COMMITTED_DURABLE
                                                → outbox: DONE
```

Key properties:
- **Write latency**: L1 cache write only — client never waits for Wasabi
- **Durability**: async promotion to Wasabi via worker
- **Idempotency**: `provider_write_intents` table prevents orphaned objects
- **Crash recovery**: worker HEADs Wasabi before promoting; if blob already
  exists, marks as durable without re-uploading

## Read Path (Download)

```
Client                          Gateway
──────                          ───────
1. POST /v1/versions/{vid}:authorizeDownload
   → ──────────────────────▶  metadata.Store.GetVersion
   ← capability + expiry       → check tenant access
                                → issue capability
2. GET blob (with capability)
   → ──────────────────────▶  blobio.Pipeline.ReadBlob
                                │
                                ├─ L1 hit?  → stream from cache
                                │
                                └─ L1 miss? → singleflight L2 restore
                                              → Wasabi GET
                                              → L1 PUT (stream-while-cache)
                                              → stream to client
```

Key properties:
- **Singleflight**: concurrent reads of the same blob collapse into one
  Wasabi GET per gateway replica
- **Stream-while-cache**: L1 is populated as bytes stream to the client
- **Cache key**: immutable blob ID, independent of bearer token

## blobio Pipeline

`internal/blobio/pipeline.go` — the cache-aside layer between HTTP handlers
and the BlobStore.

### Write

1. Write ciphertext chunk to L1 cache (immediate, sync)
2. Insert `file_version` row with `commit_state = CACHED`
3. Enqueue `PROMOTE_BLOB` event in `outbox` table
4. Worker picks up event, promotes to Wasabi, updates to `COMMITTED_DURABLE`

### Read

1. Check L1 cache for blob
2. Hit → stream from cache
3. Miss → singleflight `golang.org/x/sync/singleflight`:
   - Only one goroutine per blob key does the Wasabi GET
   - Other concurrent readers wait, then all stream from the same response
4. Stream-while-cache: bytes are written to L1 as they stream to the client

### Promote (worker)

1. Deduplicate via singleflight (one promote per blob)
2. HEAD Wasabi to check if blob already exists (crash recovery idempotency)
3. If not found:
   - Small blobs: stream through hashing reader (no full buffering)
   - Large blobs: multipart upload with parallel part workers
4. Verify SHA-256 + length match
5. Update `file_version.commit_state = COMMITTED_DURABLE`
6. Mark outbox event as DONE

## Postgres Schema

### Core Tables (`001_init.sql`)

| Table | Purpose |
| --- | --- |
| `tenants` | Tenant config: pool_id, privacy_mode, guardrails (JSONB) |
| `files` | File/folder metadata: name_encrypted (BYTEA), parent_folder_id |
| `file_versions` | Per-version metadata: blob_key, size, checksum, commit_state |
| `provider_write_intents` | Erasure fence: PROPOSED → ADOPTED/ABANDONED |
| `upload_sessions` | In-progress multipart uploads: parts (JSONB), state |
| `quota_reservations` | Atomic quota checks: HELD → COMMITTED/RELEASED |
| `erasure_ledger` | Append-only DR safety: SOFT_DELETE, HARD_PURGE, QUOTA_RELEASE |
| `outbox` | Transactional outbox: PROMOTE_BLOB, REPAIR_BLOB, PURGE_BLOB, etc. |
| `schema_version` | Migration tracking |

### Blob Placements (`002_blob_placements.sql`)

Tracks which provider stores each blob version (for multi-provider future).

### Demo Seed Data (`003_drive_demo.sql`)

Seeds 4 tenants, 10 root folders, 13 demo users for the web sample.

### Key Invariants

- Object keys carry no tenant/user/group/file/folder identifiers (§3 invariant 14)
- KChat SHA-256 is the authoritative checksum; provider ETags never used
- Erasure ledger is append-only — a restore must not resurrect any row whose
  latest erasure_ledger entry is newer than the backup
- Outbox ensures transactional consistency: metadata + event are committed in
  the same Postgres transaction

## BlobStore Interface

`pkg/blobstore/blobstore.go` defines two interfaces:

### BlobStore

Provider-agnostic write/read operations:
- `Put`, `Head`, `Get`, `Delete`, `PurgeAllVersions`
- Multipart: `CreateMultipart`, `SignUploadPart`, `UploadPart`,
  `CompleteMultipart`, `AbortMultipart`
- `CopyWithinProvider`, retention management, `Capabilities`

### BlobInventory

Trusted reconciliation interface (not client-facing):
- `ListObjects`, `ListObjectVersions`, `DeleteVersion`
- `ListMultipartUploads`, `ListParts`, `DeleteBatch`

### Adapters

| Adapter | Package | Use |
| --- | --- | --- |
| `local_fs_dev` | `pkg/blobstore/local_fs_dev` | Dev/CI — filesystem at `/tmp/kchat-drive-dev` |
| `wasabi` | `pkg/blobstore/wasabi` | Production — AWS SDK v2, S3-compatible |
| `mock` | `pkg/blobstore/mock` | Tests |

### Error Taxonomy

| Error | Meaning |
| --- | --- |
| `ErrNotFound` | Object/version does not exist |
| `ErrPreconditionFailed` | Conditional create (IfNoneMatch) found existing object |
| `ErrThrottled` | Provider rate-limiting — caller should back off |
| `ErrRetentionConflict` | Write/delete would bypass retention lock |
| `ErrVersionMismatch` | Requested version ID doesn't match current |
| `ErrChecksumMismatch` | Provider checksum ≠ expected KChat SHA-256 |
| `ErrAmbiguousTimeout` | PUT timed out — must HEAD + verify before retry |

### Circuit Breaker

Optional (`wasabi_circuit_breaker_enabled`). After N consecutive failures
(default 10), the breaker opens and fails-fast all Wasabi requests. A probe
after 30 seconds closes the breaker if it succeeds.

## Wasabi Guardrails

`pkg/wasabiguardrails/guardrails.go` — enforces Wasabi fair-use policy:

- **Fair-use constraint**: per-tenant monthly egress ≤ per-tenant active
  storage volume
- **Min storage duration**: 90 days (Wasabi policy)
- **Egress budget**: replenishes monthly, sized to stored bytes × ratio
- **Cache hit ratio**: target ≥ 90% to keep egress under budget
- **Queue depth alert**: warns when CACHED blob count exceeds threshold

The guardrails are declarative types; enforcement lives in the gateway's
cache/billing modules and the worker's rollup job.

## Backup & DR

### Nightly Backup

`deploy/sme/backup.sh` runs `pg_dump` and uploads to Wasabi. Scheduled via
cron (`backup_cron` config, default `17 3 * * *`).

### WAL Archiving

Postgres is configured with:
- `wal_level=replica`, `archive_mode=on`
- `archive_command=cp %p /var/lib/postgresql/wal-archive/%f`
- `archive_timeout=900` (15 min) — forces segment switch even with low traffic
- Enables point-in-time recovery with ~15 min RPO

### Erasure Ledger

The `erasure_ledger` table is the one DR safety net (plan §5, ADR-021):
- Append-only, monotonic generation per entity
- After any restore, replay the ledger to ensure deleted rows stay deleted
- A restored "deleted" row cannot come back from a stale backup

### Retention

- Backup retention: 14 days (configurable via `backup_retention_days`)
- Wasabi: no Object Lock in phase 1 (bucket versioning only)

## Security Boundaries

```
┌─────────────────────────────────────────────────┐
│ Client (Browser) — TRUSTED                       │
│  ✓ Plaintext file content                        │
│  ✓ Version DEK (plaintext)                        │
│  ✓ Domain Key / Share Grant Key (plaintext)      │
│  ✓ Ed25519 private signing key                   │
└──────────────────────┬──────────────────────────┘
                       │ Only ciphertext + wrapped keys cross this boundary
┌──────────────────────▼──────────────────────────┐
│ Gateway (Go) — UNTRUSTED                         │
│  ✗ Never sees plaintext DEK                      │
│  ✗ Never sees plaintext file content             │
│  ✗ Never sees domain key / share grant key       │
│  ✓ Stores: ciphertext chunks, wrapped DEKs,      │
│           encrypted names, opaque metadata       │
│  ✓ Enforces: tenant isolation, access control    │
└──────────────────────────────────────────────────┘
```

Even if the gateway is compromised, an attacker cannot decrypt file contents
without the client-side keys. Object keys are opaque and carry no tenant,
user, group, file, or folder identifiers.

## Production Topology

```
                    ┌─────────────┐
                    │   Traefik   │ (TLS, Let's Encrypt)
                    │  :443/:80   │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
     ┌────────▼───┐  ┌────▼─────┐  ┌──▼──────────┐
     │ gateway-1  │  │ gateway-2│  │   worker     │
     │ :8080      │  │ :8080    │  │ (no HTTP)    │
     │ L1 disk    │  │ L1 disk  │  │              │
     │ 400 GiB    │  │ 400 GiB  │  │              │
     └─────┬──────┘  └────┬─────┘  └──────┬───────┘
           │              │               │
           └──────────────┼───────────────┘
                          │
                   ┌──────▼──────┐
                   │  Postgres   │ (WAL archive, PITR)
                   │  :5432      │
                   └─────────────┘

  Wasabi (durable origin) ←── gateway (promote/read) + worker (backup)
```

- Single Linode VM pool
- 2 gateway replicas with independent L1 caches
- 1 worker process
- 1 Postgres instance (no HA in v1)
- Traefik for TLS + zero-downtime deploys
