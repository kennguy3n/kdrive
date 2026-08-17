# KChat Drive — Architecture

This document describes the architecture of the **KChat Drive Go gateway**:
the server-side component of KChat's end-to-end encrypted (E2EE) file storage
system. The gateway is an **untrusted storage provider** — it stores
ciphertext and opaque metadata only, never plaintext or encryption keys.
All cryptography happens client-side in the Rust/WASM SDK
(`kdrive-rust-sdk`).

The canonical source of truth is
`KChat-Privacy-Drive-Storage-Architecture-v1.0.md` (in the kdrive-rust-sdk
repo). This gateway implements the simplified Linode-VM + Wasabi subsystem
described in the architecture plan, with section references (§N) below
pointing at that document.

## Table of Contents

- [System Overview](#system-overview)
- [Trust Model & Security Boundaries](#trust-model--security-boundaries)
- [Privacy Modes](#privacy-modes)
- [Process Architecture](#process-architecture)
- [Storage Tiers](#storage-tiers)
- [Write Path (Upload)](#write-path-upload)
- [Read Path (Download)](#read-path-download)
- [Promote Path (L1 → Wasabi)](#promote-path-l1--wasabi)
- [blobio Pipeline](#blobio-pipeline)
- [Postgres Schema](#postgres-schema)
- [BlobStore Interface](#blobstore-interface)
- [L1 Hot Cache](#l1-hot-cache)
- [Wasabi Guardrails](#wasabi-guardrails)
- [Worker Jobs](#worker-jobs)
- [Backup & DR](#backup--dr)
- [Production Topology](#production-topology)
- [Configuration & Lifecycle](#configuration--lifecycle)
- [Observability](#observability)
- [Failure Modes & Recovery](#failure-modes--recovery)
- [Architecture Defaults](#architecture-defaults)

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Clients (Rust SDK)                          │
│                                                                      │
│  Browser (WASM)        Native (UniFFI)        Electron (NAPI-RS)     │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐       │
│  │ React UI     │      │ Swift/Kotlin │      │ JS addon     │       │
│  │   ↓          │      │   ↓          │      │   ↓          │       │
│  │ wasm-bindgen │      │ uniffi       │      │ napi-rs      │       │
│  │   ↓          │      │   ↓          │      │   ↓          │       │
│  │ DriveFacade  │      │ DriveFacade  │      │ DriveFacade  │       │
│  │   ↓          │      │   ↓          │      │   ↓          │       │
│  │ crypto+mls   │      │ crypto+mls   │      │ crypto+mls   │       │
│  └──────────────┘      └──────────────┘      └──────────────┘       │
│  IndexedDB vault        SQLCipher vault        SQLCipher vault       │
└─────────┬─────────────────────┬─────────────────────┬───────────────┘
          │ HTTPS (ciphertext + wrapped keys only)                    │
          │ Auth: KChat identity layer (session tokens / device certs) │
          │ Demo/dev: X-Demo-Tenant / X-Demo-User headers              │
          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Go Gateway (drive-gateway)                       │
│                                                                      │
│  ┌──────────┐  ┌───────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │ HTTP Mux │─▶│ Drive API │─▶│ Metadata     │─▶│ Postgres     │    │
│  │ /healthz │  │ /v1/*     │  │ Store        │  │              │    │
│  │ /readyz  │  │           │  │              │  │ tenants      │    │
│  │ /metrics │  │           │  │              │  │ files        │    │
│  └──────────┘  └───────────┘  └──────────────┘  │ file_versions│    │
│                                                   │ folders      │    │
│  ┌──────────────────────────────────────┐        │ nodes        │    │
│  │ blobio Pipeline                       │        │ outbox       │    │
│  │  Write: L1 cache → async promote      │        │ erasure_ledger│   │
│  │  Read:  L1 hit → return               │        │ blob_placements│  │
│  │         L1 miss → L2 singleflight GET │        └──────────────┘    │
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
│  BackupJob       — nightly pg_dump + WAL → Wasabi                   │
│  GuardrailRollup — cache-hit ratio + CACHED queue-depth alerts      │
│                                                                      │
│  Reads blob_placements / outbox tables → processes asynchronously    │
└─────────────────────────────────────────────────────────────────────┘
```

## Trust Model & Security Boundaries

```
┌─────────────────────────────────────────────────┐
│ Client (Browser / Native / Electron) — TRUSTED   │
│  ✓ Plaintext file content                        │
│  ✓ Version DEK (plaintext)                       │
│  ✓ Domain Key / Share Grant Key (plaintext)      │
│  ✓ Ed25519 private signing key                   │
│  ✓ MLS exporter secrets (Advanced / Max modes)   │
└──────────────────────┬──────────────────────────┘
                       │ Only ciphertext + wrapped keys cross this boundary
┌──────────────────────▼──────────────────────────┐
│ Gateway (Go) — UNTRUSTED                          │
│  ✗ Never sees plaintext DEK                      │
│  ✗ Never sees plaintext file content             │
│  ✗ Never sees domain key / share grant key       │
│  ✓ Stores: ciphertext chunks, wrapped DEKs,      │
│           encrypted names, opaque metadata       │
│  ✓ Enforces: tenant isolation, access control    │
└──────────────────────────────────────────────────┘
```

**Consequences:** Even if the gateway is fully compromised, an attacker
cannot decrypt file contents without the client-side keys. Object keys in
the BlobStore are opaque and carry no tenant, user, group, file, or folder
identifiers (§3 invariant 14). The gateway stores encrypted names as
`BYTEA`, key material only as wrapped envelopes (`key_envelopes`), and
file content only as ciphertext.

**Authentication:** Production deployments authenticate callers via
KChat's identity layer (session tokens, device certificates) in front of
the Drive API handlers. The current implementation trusts `X-Demo-Tenant`
and `X-Demo-User` headers for demo / web-sample purposes only —
`getUserTenant` in `internal/server/drive_api.go` is the single swap point
for production auth.

## Privacy Modes

The gateway is mode-agnostic — it stores ciphertext and metadata the same
way regardless of mode. The mode determines the **client-side key
hierarchy** (see the Rust SDK for the full crypto design). The gateway
records the mode on folders and encryption domains so it can route
operations correctly.

| Mode | Key unit | Server visibility | Use case |
| --- | --- | --- | --- |
| **Secured (1)** | DomainKey per tenant-governed domain + tenant recovery envelope | Server holds wrapped recovery envelope only | Default B2B |
| **Advanced (2)** | DomainKey per MLS group/folder domain + encrypted backward chain | Server holds encrypted backward-chain envelopes | B2B with MLS groups |
| **Max (3)** | ShareGrantKey per share grant; MLS exporter wraps ShareGrantKey | Server holds per-grant wrapped keys | B2C, high-sensitivity |

In all three modes KChat (the server) **cannot** access or decrypt files.
The gateway's role is to persist the wrapped key envelopes (`key_envelopes`
table) and route rotate/share operations to the right domain/grant rows.

## Process Architecture

The system runs two binary types, deployed as Docker containers.

### drive-gateway (N replicas)

Merges API, edge, and L1 cache into one process (plan §6). Each replica:

- Serves HTTP on `:8080` (Traefik terminates TLS on `:443`).
- Maintains its own L1 disk cache (NVMe/SSD, 400 GiB default).
- Connects to a shared Postgres instance (pool: 20 max open, 10 idle,
  30-min max lifetime, 5-min max idle time).
- Talks to Wasabi (production) or `local_fs_dev` (dev) for durable storage.
- Auto-migrates the Postgres schema on startup (idempotent).
- Runs behind Traefik for TLS termination and zero-downtime load balancing.

### drive-worker (1 instance)

Background job runner. Does not serve HTTP. Runs five jobs in parallel
goroutines (see [Worker Jobs](#worker-jobs)). Reads `blob_placements` and
`outbox` tables; never serves client traffic.

## Storage Tiers

### L1 — Hot Cache (NVMe/SSD)

- **Interface**: `pkg/hotcache.Cache` — `Get`, `Put`, `Evict`, `Stats`,
  `Close`.
- **Dev**: in-memory (`hotcache.NewMemoryCache`) with LRU eviction.
- **Production**: disk-backed (`hotcache.NewDiskCache`) on NVMe, 400 GiB.
- **Content**: ciphertext only, keyed by immutable blob ID.
- **Eviction**: LRU by bytes, with a 10% hot region (`EvictionLRUHotPin`)
  and explicit pinning via `PutOptions.PinHot` / `NonEvictable`.
- **Auth**: checked before cache lookup; cache key is the immutable blob
  ID, not the bearer token (§15.4). This means a cached blob can be served
  to any authorized reader without re-fetching from L2.
- **Non-evictable entries**: CACHED (not-yet-durable) blobs are pinned
  with `NonEvictable=true` so LRU pressure cannot lose the only copy.
  Once the worker promotes them to `COMMITTED_DURABLE`, the entry is
  re-put with `NonEvictable=false`.

### L2 — Durable Origin (S3-compatible)

- **Interface**: `pkg/blobstore.BlobStore` — `Put`, `Head`, `Get`,
  `Delete`, `PurgeAllVersions`, multipart operations, retention,
  `Capabilities`.
- **Dev**: `local_fs_dev` adapter at `/tmp/kchat-drive-dev`.
- **Production**: `s3` adapter using AWS SDK v2. Supports any
  S3-compatible provider (Wasabi, AWS S3, Backblaze B2) selected via
  the `storage.provider` config field.
- **Content**: ciphertext only, opaque object keys (no
  tenant/user/file/folder IDs).
- **Integrity**: KChat SHA-256 is authoritative; provider ETags are never
  used as the integrity check (§13.3).
- **Versioning**: bucket versioning enabled (no Object Lock in
  phase 1). Wasabi and AWS S3 support Object Lock; B2 does not via
  the S3 API.
- **Min storage duration**: Wasabi's 90-day minimum applies when
  `provider=wasabi`; AWS S3 and B2 have no minimum. Short-TTL objects
  must never reach a provider with a min-storage duration (the gateway
  routes them to L1 only).

### Metadata — Postgres

- **Interface**: `internal/metadata.Store` + `PostgresStatusStore`.
- **Schema**: `deploy/migrations/00{1,2,3}_*.sql` (auto-migrated on
  startup).
- **Content**: tenant config, file/folder metadata, version metadata,
  upload sessions, quota reservations, write intents, outbox events,
  erasure ledger, blob placements, encryption domains, key envelopes,
  share grants, access-context snapshots.
- **No plaintext**: encrypted names stored as `BYTEA`, keys are
  wrapped/encrypted in `key_envelopes.ciphertext`.

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
   ← blob_key                   → L1 cache PUT (immediate, NonEvictable=true)
                                → blob_placements INSERT (commit_state=CACHED)
                                → outbox: PROMOTE_BLOB event
                                                   │
                                                   ▼
                                              PromotionJob
                                                → L1 GET blob
                                                → HEAD Wasabi (idempotency)
                                                → Wasabi PUT
                                                  (multipart if large)
                                                → Verify SHA-256 + length
                                                → blob_placements UPDATE
                                                  commit_state =
                                                  COMMITTED_DURABLE
                                                → outbox: DONE
```

**Key properties:**

- **Write latency**: L1 cache write only — the client never waits for
  Wasabi. The write returns as soon as the cache PUT and the
  `blob_placements` INSERT commit.
- **Durability**: async promotion to Wasabi via the worker. The blob is
  the only copy in L1 until promotion completes, hence `NonEvictable=true`.
- **Idempotency**: `provider_write_intents` table prevents orphaned
  objects; the worker HEADs Wasabi before promoting and skips the PUT if
  the blob already exists.
- **Crash recovery**: if the gateway crashes after the cache PUT but
  before the worker promotes, the `blob_placements` row stays `CACHED`
  and the next promotion sweep picks it up. If the worker crashes mid-PUT,
  the next sweep HEADs Wasabi and either verifies the partial upload
  (and marks durable) or retries.
- **Sync promote option**: `WriteOptions.PromoteSync=true` blocks the
  write until Wasabi confirms the PUT (used for locked writes that need
  immediate durability).

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
                                              → buffer body (≤256 MiB)
                                              → return to caller
                                              → async: L1 PUT + status PUT
```

**Key properties:**

- **Singleflight**: concurrent reads of the same blob collapse into one
  Wasabi GET per gateway replica. The first reader issues the GET; later
  readers wait on a `done` channel and receive an independent copy of the
  buffered body.
- **Async cache fill**: the L1 cache PUT and `blob_placements` update
  happen in a background goroutine after the body is returned to the
  caller, so first-byte latency for cold reads is just the Wasabi GET
  time.
- **Restore size limit**: `DefaultMaxRestoreBytes = 256 MiB`. Blobs
  larger than this must be served via a streaming path that bypasses the
  singleflight buffer (configurable via `Pipeline.SetMaxRestoreBytes`).
- **Cache key**: immutable blob ID, independent of bearer token.

## Promote Path (L1 → Wasabi)

`Pipeline.Promote(ctx, blobID)` is called by the worker's `PromotionJob`.

1. **Deduplicate** via `promoteInflight` (`sync.Map`): only one concurrent
   promote per blob, so a burst of CACHED blobs doesn't multiply Wasabi
   PUTs.
2. **HEAD Wasabi** to check if the blob already exists (crash-recovery
   idempotency). If found and the length + SHA-256 match, skip the PUT
   and just `MarkDurable` (`promoteSkipped` metric).
3. **If not found**:
   - Small blobs (≤ `promoteMultipartThreshold`): stream through a
     hashing reader so the full body is never buffered in memory.
   - Large blobs: multipart upload with `DefaultMultipartParallelism = 4`
     concurrent part workers.
4. **Verify SHA-256 + length** match the `blob_placements` row.
5. **`MarkDurable`** on `blob_placements` (targeted UPDATE — no INSERT
   branch, the row was created when the blob was first cached).
6. **Re-put the cache entry** with `NonEvictable=false` so it becomes
   eligible for LRU eviction (the blob is now safely in Wasabi).

## blobio Pipeline

`internal/blobio/pipeline.go` — the cache-aside layer between HTTP
handlers and the BlobStore.

### Write

1. Compute SHA-256 of the body.
2. `cache.Put` with `NonEvictable=true` (CACHED = only copy).
3. `status.Put` on the `StatusStore` (`blob_placements` in production,
   in-memory in dev) with `commit_state = CACHED`.
4. If `PromoteSync`, call `Promote` synchronously and flip the result
   state to `COMMITTED_DURABLE`.

### Read

1. `cache.Get` — on hit, return immediately with status metadata.
2. On `ErrCacheMiss`, call `restoreFromL2`:
   - `inflight.LoadOrStore(blobID, call)` — if another goroutine is
     already restoring the same blob, wait on its `done` channel and
     return a copy of its buffered body.
   - Otherwise issue `store.Get`, buffer the body (subject to
     `maxRestoreBytes`), and close the `done` channel.
3. Spawn a background goroutine (tracked by the pipeline's `WaitGroup`,
   cancelled by `Close`) to `cache.Put` the restored body with
   `NonEvictable=false` and update `status.Put` to `COMMITTED_DURABLE`.
4. Return the body to the caller immediately.

### Status Store

`StatusStore` is the persistence interface for blob placement state:

- `Get(blobID)` — returns `BlobStatus` or nil if not present.
- `Put(status)` — upsert (insert when first cached, update on state
  change).
- `MarkDurable(blobKey, versionID, size, checksum, durableAt)` — targeted
  UPDATE used by the promote path after a successful Wasabi PUT.

Implementations:

- `MemoryStatusStore` — in-memory map for tests/dev.
- `metadata.PostgresStatusStore` — production, backed by the
  `blob_placements` table (atomic upsert via `ON CONFLICT DO UPDATE`).

### Lifecycle

`Pipeline.Close` cancels the lifecycle context (stopping background cache
fills) and waits on the `WaitGroup` so temp files are not orphaned during
graceful shutdown. The gateway calls `Close` after the HTTP server stops.

## Postgres Schema

Migrations are embedded in the binary (`internal/metadata/embedded.go`)
and applied idempotently on gateway startup.

### Core Tables (`001_init.sql`)

| Table | Purpose |
| --- | --- |
| `tenants` | Tenant config: `pool_id`, `privacy_mode`, `guardrails` (JSONB) |
| `files` | File/folder metadata: `name_encrypted` (BYTEA), `parent_folder_id` |
| `file_versions` | Per-version metadata: `blob_key`, `size`, `checksum`, `commit_state` |
| `provider_write_intents` | Erasure fence: `PROPOSED` → `ADOPTED`/`ABANDONED` |
| `upload_sessions` | In-progress multipart uploads: `parts` (JSONB), `state` |
| `quota_reservations` | Atomic quota checks: `HELD` → `COMMITTED`/`RELEASED` |
| `erasure_ledger` | Append-only DR safety: `SOFT_DELETE`, `HARD_PURGE`, `QUOTA_RELEASE` |
| `outbox` | Transactional outbox: `PROMOTE_BLOB`, `REPAIR_BLOB`, `PURGE_BLOB`, `BACKUP_REQUEST`, `GUARDRAIL_ROLLUP` |
| `schema_version` | Migration tracking |

### Blob Placements (`002_blob_placements.sql`)

Decouples placement state from `file_versions` so that when dedup is
implemented, multiple `file_versions` can reference the same `blob_key`
without the UPDATE-by-`blob_key` problem. Keyed by `blob_key` (PRIMARY
KEY). Backfilled from `file_versions` on first run via
`INSERT ... SELECT DISTINCT ON (blob_key)`.

### Drive Schema (`003_drive_demo.sql`)

Extends the core schema with the Drive-specific tables (folders, nodes,
encryption domains, key envelopes, share grants, access-context snapshots)
and seeds demo data for the web sample:

| Table | Purpose |
| --- | --- |
| `tenants` (extended) | Adds `tenant_type` (`b2b`/`b2c`), `bucket_name` |
| `folders` | Hierarchical folder tree per tenant; `name_encrypted` BYTEA; `privacy_mode` |
| `nodes` | Files within folders; `name_encrypted`, `mime_type` |
| `encryption_domains` | Per-domain key generation tracking; `generation`, `prev_generation`, `prev_key_envelope` (backward chain) |
| `key_envelopes` | Wrapped key material: `envelope_type` (`hpke`, `mls_transport`, `domain_wrap`, `share_grant_wrap`, `recovery`), `ciphertext`, `nonce`, `encapsulated_key` |
| `share_grants` | Per-share grant for Max mode; `grantor_user_id`, `grantee_user_id`, `generation`, `is_active`, `key_envelope_id` |
| `access_context_snapshots` | Minimal ACL snapshot; `revision`, `snapshot_hash`, `acl_ciphertext` |

Seeds 4 tenants (`tenant_b2c` + 3 B2B), 10 root folders (3 modes × 3 B2B +
1 B2C), and demo users.

### Key Invariants

- **Object keys carry no tenant/user/group/file/folder identifiers**
  (§3 invariant 14).
- **KChat SHA-256 is the authoritative checksum**; provider ETags are
  never used.
- **Erasure ledger is append-only** — a restore must not resurrect any
  row whose latest `erasure_ledger` entry is newer than the backup
  (ADR-021).
- **Outbox ensures transactional consistency** — metadata + event are
  committed in the same Postgres transaction, so the worker never misses
  an event that was committed.
- **`blob_placements` is keyed by `blob_key`** — dedup-ready: multiple
  `file_versions` can share one blob without cross-version contamination.

## BlobStore Interface

`pkg/blobstore/blobstore.go` defines two interfaces.

### BlobStore

Provider-agnostic write/read operations:

- `Put`, `Head`, `Get`, `Delete`, `PurgeAllVersions`
- Multipart: `CreateMultipart`, `SignUploadPart`, `UploadPart`,
  `CompleteMultipart`, `AbortMultipart`
- `CopyWithinProvider`, retention management (`GetRetention`,
  `UpdateRetention`, `SetLegalHold`), `Capabilities`

### BlobInventory

Trusted reconciliation interface (not client-facing):

- `ListObjects`, `ListObjectVersions`, `DeleteVersion`
- `ListMultipartUploads`, `ListParts`, `DeleteBatch`

### Adapters

| Adapter | Package | Use |
| --- | --- | --- |
| `local_fs_dev` | `pkg/blobstore/local_fs_dev` | Dev/CI — filesystem at `/tmp/kchat-drive-dev` |
| `s3` | `pkg/blobstore/s3` | Production — AWS SDK v2, S3-compatible (Wasabi, AWS S3, Backblaze B2) |
| `mock` | `pkg/blobstore/mock` | Tests |

### Error Taxonomy

| Error | Meaning |
| --- | --- |
| `ErrNotFound` | Object/version does not exist |
| `ErrPreconditionFailed` | Conditional create (`IfNoneMatch`) found existing object |
| `ErrThrottled` | Provider rate-limiting — caller should back off |
| `ErrRetentionConflict` | Write/delete would bypass retention lock |
| `ErrVersionMismatch` | Requested version ID doesn't match current |
| `ErrChecksumMismatch` | Provider checksum ≠ expected KChat SHA-256 |
| `ErrAmbiguousTimeout` | PUT timed out — must HEAD + verify before retry |

Errors are sentinel string values so `errors.Is` / `errors.As` work across
wrapped chains.

### Provider Capabilities

`ProviderCapabilities` is a typed struct (§13.3) the gateway consults
before routing a request: `ProviderVersioning`, `ConditionalPut`,
`NativeSHA256Checksum`, `S3ObjectLock`, `PerVersionRetentionGet/Extend/
Clear`, `LegalHoldSetClear`, `GovernanceBypassDenied`, `Lifecycle`,
`EventNotifications`, `Replication`, `MaxObjectSize`, `MaxPartSize`,
`MaxParts`, `MinPartBytes`, `ChecksumOnComplete`, `ListAndAbortMultipart`,
`BrowserPresignedUpload`, `PathStyleAddressing`,
`VirtualHostAddressing`, `BucketVersioningState`, etc.

### Circuit Breaker

Optional (`storage_circuit_breaker_enabled`, default false). After N
consecutive failures (default 10) the breaker opens and fails-fast all
storage backend requests. A probe after 30 seconds closes the breaker
if it succeeds. Implemented in
`pkg/blobstore/s3/circuit_breaker.go`.

### Conformance Suite

`pkg/contracttest` is the shared suite every adapter must pass:
conditional create, ambiguous-PUT-timeout HEAD+verify, byte-range GET,
multipart create/upload/complete/abort, copy-within-provider, retention
monotonic extension, legal-hold set/clear, purge-all-versions, inventory
list/versions/multipart/parts, batch delete, and the negative capability
test proving phase-1 adapters reject direct staging (ADR-019).

## L1 Hot Cache

`pkg/hotcache` defines the cache interface and two implementations.

### Memory Cache (dev)

`memory_cache.go` — in-memory map with LRU eviction by bytes. Used in dev
mode and tests.

### Disk Cache (production)

`disk_cache.go` — disk-backed on NVMe/SSD. Streams bodies to disk and
records size + hash on completion. `Close` waits for in-flight writes so
temp files are not orphaned during graceful shutdown.

### Eviction Policy

`EvictionPolicy` (`policy.go`):

- `EvictionLRU` — plain LRU by bytes.
- `EvictionLRUHotPin` — LRU with a hot region (default 10% of capacity).
  Entries with `HitCount ≥ HotDemotionHitCount` are promoted to the hot
  region and protected from eviction until demoted.
- `NonEvictable` entries (CACHED blobs) are completely excluded from
  automatic eviction; they can only be removed via explicit `Evict`.

`DefaultEvictionPolicy(maxBytes)` returns `EvictionLRUHotPin` with a 10%
hot region and `HotDemotionHitCount = 1`.

## Storage Guardrails

`pkg/storageguardrails/guardrails.go` — declarative types enforcing
per-provider fair-use policies. Each provider has a `ProviderProfile`
selected by `ProfileFor(providerName)`:

- **Wasabi**: 90-day min storage (`WasabiMinStorageDays`), egress ratio
  1.0×, $6.99/TB-month.
- **AWS S3**: no min storage, no fair-use egress constraint, standard
  pricing.
- **Backblaze B2**: no min storage, generous egress (10×), $6/TB-month.

- **Fair-use constraint**: per-tenant monthly egress ≤ per-tenant active
  storage volume × `EgressStorageRatio`.
- **Min storage duration**: provider-specific (Wasabi: 90 days, S3/B2: 0).
  Short-TTL objects must never reach a provider with a min-storage
  duration.
- **Egress budget**: `FairUseEgressBudget` replenishes monthly, sized to
  stored bytes × `EgressStorageRatio`. `SoftCapBytes` warns;
  `HardCapBytes` triggers `ThrottlePolicy` (default
  `slowdown_origin_reads`).
- **Cache hit ratio**: `CacheHitRatioTarget.Min = 0.9` over a 30-day
  window — keeps egress under budget.
- **Queue depth alert**: warns when CACHED blob count exceeds
  `queue_depth_alert` (default 1000) — a backpressure signal.

The guardrails are declarative types; enforcement lives in the gateway's
cache/billing modules and the worker's `GuardrailRollupJob`.

`MinStorageTracker.ResidualBillableWindow` computes the remaining billable
duration when a piece is deleted before the 90-day minimum expires, so
the billing pipeline can charge correctly.

## Worker Jobs

`internal/worker/jobs.go` — five jobs, each on its own ticker.

### PromotionJob

- **Interval**: `promote_interval_ms` (default 30s).
- **Batch**: `promote_batch_size` (default 100), oldest `CACHED` first.
- **Parallelism**: `promote_parallelism` (default 4) concurrent promotes
  via a bounded worker pool.
- **Source**: prefers `blob_placements` (`ListCachedPlacements`) when
  available; falls back to `file_versions` (`ListCachedVersions`) for
  backward compat.
- **Metrics**: `promotedTotal`, `failedTotal`, `batchCount`.
- Runs once immediately on startup so a manual `drive-worker` invocation
  promotes pending blobs without waiting for the first tick.

### RepairJob

- **Interval**: `repair_interval_ms` (default 5 min).
- **Sample**: `repair_sample_count` (default 10) random durable blobs
  (`ORDER BY random()`).
- **Action**: re-reads each sampled blob from the durable store and
  compares SHA-256. On mismatch, logs an alert (in a full implementation
  it would mark the blob for re-promotion).
- **Metrics**: `checkedTotal`, `mismatchTotal`, `scanCount`.

### PurgeJob

- **Interval**: 1 hour.
- **Action**: lists in-progress multipart uploads via `BlobInventory`.
  Currently logs the count for observability but does **not** abort —
  `ListMultipartUploads` doesn't return creation time, so aborting would
  risk killing legitimate active uploads. A future implementation
  cross-references `upload_sessions` to find truly orphaned uploads.

### BackupJob

- **Interval**: 24 hours (runs once on startup, then daily).
- **Action**: logs the backup intent for observability. The actual
  `pg_dump` + WAL sync runs via host cron or `deploy/sme/backup.sh`.

### GuardrailRollupJob

- **Interval**: 5 min.
- **Action**:
  - Rolls up cache stats (`entries`, `bytes_used`, `hits`, `misses`,
    `evictions`, `hit_ratio`). Warns if hit ratio drops below 0.9 with
    >100 total requests.
  - Monitors CACHED queue depth (`CountCachedPlacements`). Warns when
    depth exceeds `queue_depth_alert`, suggesting scaling workers or
    increasing parallelism.
- **Metrics**: `queueDepthSamples`, `queueDepthAlerts`.

## Backup & DR

### Nightly Backup

`deploy/sme/backup.sh`:

1. `pg_dump --clean --if-exists --no-owner --no-privileges` inside the
   postgres container.
2. `gzip -9` compress.
3. Upload to `s3://$WASABI_BUCKET/backups/postgres/kdrive-<ts>.sql.gz`
   via the `aws` CLI with `--endpoint-url` pointed at Wasabi.
4. Sync the WAL archive volume to Wasabi as
   `backups/postgres/wal-<ts>.tar` for PITR.
5. Prune backups older than `RETENTION_DAYS` (default 14).

Credentials are exported into the environment for the `aws` invocation
only — never written to disk.

### WAL Archiving

Postgres is configured (in the compose `command`) with:

- `wal_level=replica`, `archive_mode=on`
- `archive_command=cp %p /var/lib/postgresql/wal-archive/%f`
- `archive_timeout=900` (15 min) — forces a segment switch even with low
  traffic.
- `max_wal_senders=3`, `checkpoint_timeout=300`, `max_wal_size=4GB`,
  `min_wal_size=1GB`, `--wal-segsize=64`.

This enables point-in-time recovery with ~15 min RPO from the nightly
base backup + WAL replay.

### Erasure Ledger

The `erasure_ledger` table is the one DR safety net kept from v1 (plan
§5, ADR-021):

- Append-only, monotonic `erasure_generation` per `(entity_type,
  entity_id)`.
- After any restore, replay the ledger to ensure deleted rows stay
  deleted — a restored "deleted" row cannot come back from a stale
  backup.
- `entity_type`: `file_version`, `upload_session`, `quota_reservation`.
- `erasure_type`: `SOFT_DELETE`, `HARD_PURGE`, `QUOTA_RELEASE`.

### Retention

- Backup retention: 14 days (configurable via `backup_retention_days`).
- Wasabi: no Object Lock in phase 1 (bucket versioning only).

## Production Topology

```
                    ┌─────────────┐
                    │   Traefik   │ (TLS, Let's Encrypt TLS-ALPN-01)
                    │  :443/:80   │ (HTTP→HTTPS redirect)
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
                   │  Postgres   │ (WAL archive, PITR, ~15 min RPO)
                   │  :5432      │
                   └─────────────┘

  Wasabi (durable origin) ←── gateway (promote/read) + worker (backup)
```

- Single Linode VM pool.
- 2 gateway replicas with **independent L1 caches** (separate Docker
  volumes: `gateway-1-cache`, `gateway-2-cache`). A cache miss on one
  replica does not warm the other — each replica's singleflight is
  independent.
- 1 worker process.
- 1 Postgres instance (no HA in v1; WAL archiving + nightly `pg_dump`
  provide PITR).
- Traefik for TLS + zero-downtime deploys (`upgrade.sh` rolls replicas
  one at a time).

## Configuration & Lifecycle

### Config Loading

`internal/config/config.go` loads JSON config for both binaries. The
loader:

- Returns a built-in dev config when no path is given (and
  `$DRIVE_GATEWAY_CONFIG` / `$DRIVE_WORKER_CONFIG` is unset).
- Validates all tuning knobs at startup and fails fast on negative values
  or missing production-required fields.
- Does **not** expand environment variables itself — the compose
  entrypoint renders templates with `envsubst` at container start so
  secrets never land in the image.

### Gateway Lifecycle

1. Load config.
2. `server.NewGateway`:
   - Build `BlobStore` (Wasabi or `local_fs_dev`).
   - Build `Cache` (disk or memory).
   - If `PostgresDSN` set: open pool, `metadata.New`, `AutoMigrate`
     (30s timeout), build `PostgresStatusStore`, wire pipeline with it.
   - Else: wire pipeline with in-memory `StatusStore`.
3. Build HTTP handler: `/healthz`, `/readyz`, `/metrics`, and (if
   Postgres configured) the Drive `/v1/*` REST API routes. Wrap with
   `withWebHeaders` (COOP/COEP for the WASM web sample).
4. `ListenAndServe` on `:8080` with a 10s `ReadHeaderTimeout`.
5. On `SIGINT`/`SIGTERM`: `Shutdown` (30s), then `srv.Close` (pipeline →
   cache → DB pool).

### Worker Lifecycle

1. Load config.
2. `worker.New`:
   - In dev (no Postgres): no jobs, idle.
   - In production: build store, cache, `PostgresStatusStore`, pipeline;
     build the five jobs with configured tuning knobs.
3. `Start`: launch each job in its own goroutine, wait for ctx cancel.
4. On `SIGINT`/`SIGTERM`: `Stop` (cancel ctx, wait 15s for jobs), then
   `Close` (DB pool).

## Observability

### Metrics (`/metrics`)

Prometheus text format, exported by `internal/server/metrics.go`:

- `kdrive_cache_*` — entries, bytes_used, bytes_limit, hits, misses,
  evictions, hit_ratio.
- `kdrive_promote_*` — attempts, successes, failures, skips,
  duration_avg_ms.

Worker metrics are emitted as structured logs (the worker has no HTTP
endpoint).

### Readiness

`/readyz` checks Postgres (`Ping`) and the durable store. For Wasabi it
issues a `Head` on a known non-existent probe key — `nil` error or
`ErrNotFound` both mean reachable; any other error (network, auth,
timeout) means not ready. 5-second deadline.

### Logs

Both binaries use `log/slog` JSON at `INFO`. Key streams: gateway
startup/migration/readiness, request errors; worker per-job batch
summaries, queue-depth warnings, checksum-mismatch alerts.

## Failure Modes & Recovery

| Failure | Detection | Recovery |
| --- | --- | --- |
| Gateway crash after cache PUT, before promote | `blob_placements` row stays `CACHED` | Next `PromotionJob` sweep promotes it |
| Worker crash mid-Wasabi-PUT | `blob_placements` stays `CACHED` | Next sweep HEADs Wasabi; verifies or retries |
| Wasabi PUT timeout (ambiguous) | `ErrAmbiguousTimeout` | Caller MUST HEAD + verify before retrying (§13.3) |
| Wasabi down | Circuit breaker (optional) opens after N failures | Probe after 30s; fails-fast until then |
| Postgres down | `/readyz` fails; gateway returns 503 | Postgres restart; WAL replay for PITR |
| Lost backup | Nightly `backup.sh` fails | Previous night's backup still in retention window |
| Stale backup restore | Erasure ledger replay | Deleted rows whose latest ledger entry is newer than the backup stay deleted |
| Cache eviction of CACHED blob | Prevented by `NonEvictable=true` | N/A — only durable blobs are evictable |
| Singleflight thundering herd | `inflight` / `promoteInflight` `sync.Map` | One GET/PUT per blob per replica |
| L1 cache miss storm | Per-replica singleflight | One Wasabi GET per blob per replica; async cache fill |

## Architecture Defaults (Plan §10)

- **No WORM / Object Lock in phase 1** — Wasabi bucket versioning only.
- **Single Postgres instance** (no HA) acceptable for v1; WAL archiving
  + nightly `pg_dump` provide PITR with ~15 min RPO.
- **Erasure ledger** is the one DR safety net kept from v1 (ADR-021).
- **L1 disk cache on NVMe** — 400 GiB per replica default, LRU with
  hot-pin.
- **Circuit breaker optional** — off by default; opens after N
  consecutive Wasabi failures and probes after 30 seconds.
- **Object keys are opaque** — no tenant/user/group/file/folder IDs
  (§3 invariant 14).
- **KChat SHA-256 is authoritative** — provider ETags never used
  (§13.3).
- **Two gateway replicas** with independent L1 caches behind Traefik.
- **One worker process** — sufficient for v1 single-VM-pool scale.
