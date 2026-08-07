-- M3: Trimmed Postgres schema for KChat Drive.
-- Drops the multi-provider placement/allocation tables from v1; keeps
-- the provider-count-agnostic correctness tables (outbox,
-- provider_write_intents, upload_sessions, quota_reservations,
-- erasure_ledger).
-- See plan §5.

CREATE TABLE IF NOT EXISTS tenants (
    id              TEXT PRIMARY KEY,
    pool_id         TEXT NOT NULL,
    privacy_mode    TEXT NOT NULL DEFAULT 'secured',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Guardrail configuration is stored as JSONB so the
    -- wasabiguardrails.Guardrails struct can round-trip without a
    -- schema migration per tuning change.
    guardrails      JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS files (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL REFERENCES tenants(id),
    parent_folder_id TEXT,
    name_encrypted  BYTEA NOT NULL,
    -- The encrypted name is opaque to the storage layer; the gateway
    -- decrypts it with the tenant's device key before display.
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS files_tenant_parent_idx
    ON files (tenant_id, parent_folder_id)
    WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS file_versions (
    id                  TEXT PRIMARY KEY,
    file_id             TEXT NOT NULL REFERENCES files(id),
    tenant_id           TEXT NOT NULL REFERENCES tenants(id),
    -- blob_key is the opaque immutable object key in the BlobStore.
    -- It carries no tenant/user/file/folder identifiers (§3
    -- invariant 14).
    blob_key            TEXT NOT NULL,
    blob_version_id     TEXT,
    size_bytes          BIGINT NOT NULL,
    checksum_sha256     TEXT NOT NULL,
    encryption_mode     TEXT NOT NULL,
    commit_state        TEXT NOT NULL DEFAULT 'CACHED',
    -- commit_state: CACHED | COMMITTED_DURABLE
    cached_at           TIMESTAMPTZ,
    durable_at          TIMESTAMPTZ,
    retention_mode      TEXT NOT NULL DEFAULT 'NONE',
    retain_until        TIMESTAMPTZ,
    legal_hold          BOOLEAN NOT NULL DEFAULT false,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at          TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS file_versions_file_idx
    ON file_versions (file_id)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS file_versions_commit_state_idx
    ON file_versions (commit_state)
    WHERE commit_state = 'CACHED';

-- provider_write_intents: the erasure-fence check before every PUT.
-- A row is inserted before the PUT is issued and adopted only after
-- the result + generation recheck confirms the write succeeded. This
-- is cheap insurance against orphaned Wasabi objects and is
-- provider-count-agnostic (plan §2, §5).
CREATE TABLE IF NOT EXISTS provider_write_intents (
    id                  BIGSERIAL PRIMARY KEY,
    blob_key            TEXT NOT NULL,
    idempotency_token   TEXT NOT NULL,
    expected_length     BIGINT NOT NULL,
    checksum_sha256     TEXT NOT NULL,
    state               TEXT NOT NULL DEFAULT 'PROPOSED',
    -- state: PROPOSED | ADOPTED | ABANDONED
    generation          INTEGER NOT NULL DEFAULT 1,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    adopted_at          TIMESTAMPTZ,
    abandoned_at        TIMESTAMPTZ,
    UNIQUE (blob_key, idempotency_token)
);

CREATE INDEX IF NOT EXISTS provider_write_intents_state_idx
    ON provider_write_intents (state)
    WHERE state = 'PROPOSED';

-- upload_sessions: in-progress multipart uploads.
CREATE TABLE IF NOT EXISTS upload_sessions (
    id                  TEXT PRIMARY KEY,
    tenant_id           TEXT NOT NULL REFERENCES tenants(id),
    file_id             TEXT NOT NULL REFERENCES files(id),
    blob_key            TEXT NOT NULL,
    upload_id           TEXT NOT NULL,
    -- The parts array is updated as parts are uploaded.
    parts               JSONB NOT NULL DEFAULT '[]'::jsonb,
    expected_size       BIGINT NOT NULL DEFAULT 0,
    checksum_sha256     TEXT,
    state               TEXT NOT NULL DEFAULT 'UPLOADING',
    -- state: UPLOADING | COMPLETING | COMPLETED | ABORTED
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS upload_sessions_state_idx
    ON upload_sessions (state)
    WHERE state = 'UPLOADING';

-- quota_reservations: atomic quota checks before a write.
CREATE TABLE IF NOT EXISTS quota_reservations (
    id                  BIGSERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL REFERENCES tenants(id),
    file_version_id     TEXT NOT NULL REFERENCES file_versions(id),
    bytes_reserved      BIGINT NOT NULL,
    state               TEXT NOT NULL DEFAULT 'HELD',
    -- state: HELD | COMMITTED | RELEASED
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    committed_at        TIMESTAMPTZ,
    released_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS quota_reservations_tenant_state_idx
    ON quota_reservations (tenant_id, state)
    WHERE state = 'HELD';

-- erasure_ledger: append-only table replayed after any restore so a
-- resurrected "deleted" row can't come back from a stale backup
-- (plan §5, ADR-021). This is the one DR safety net kept from v1.
CREATE TABLE IF NOT EXISTS erasure_ledger (
    id                  BIGSERIAL PRIMARY KEY,
    entity_type         TEXT NOT NULL,
    -- entity_type: file_version | upload_session | quota_reservation
    entity_id           TEXT NOT NULL,
    tenant_id           TEXT NOT NULL,
    erasure_type        TEXT NOT NULL,
    -- erasure_type: SOFT_DELETE | HARD_PURGE | QUOTA_RELEASE
    erasure_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- erasure_generation is monotonic per (entity_type, entity_id);
    -- a restore must not resurrect any row whose latest
    -- erasure_ledger entry is newer than the backup.
    erasure_generation  INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS erasure_ledger_entity_idx
    ON erasure_ledger (entity_type, entity_id);

-- outbox: transactional outbox for events the worker processes
-- asynchronously (promote, repair, purge, backup).
CREATE TABLE IF NOT EXISTS outbox (
    id                  BIGSERIAL PRIMARY KEY,
    event_type          TEXT NOT NULL,
    -- event_type: PROMOTE_BLOB | REPAIR_BLOB | PURGE_BLOB |
    --             BACKUP_REQUEST | GUARDRAIL_ROLLUP
    payload             JSONB NOT NULL,
    state               TEXT NOT NULL DEFAULT 'PENDING',
    -- state: PENDING | IN_PROGRESS | DONE | FAILED
    attempts            INTEGER NOT NULL DEFAULT 0,
    max_attempts        INTEGER NOT NULL DEFAULT 5,
    last_error          TEXT,
    available_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_state_available_idx
    ON outbox (state, available_at)
    WHERE state = 'PENDING';

-- Schema version tracking for future migrations.
CREATE TABLE IF NOT EXISTS schema_version (
    version             INTEGER PRIMARY KEY,
    applied_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO schema_version (version) VALUES (1)
    ON CONFLICT (version) DO NOTHING;
