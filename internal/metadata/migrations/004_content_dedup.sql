-- M5: Content deduplication tables (KDRV1).
--
-- Enables intra-tenant content dedup by tracking content entries and
-- their chunks separately from file versions. Multiple file versions
-- (across Secured/Advanced/Max modes) can reference the same content
-- entry, and the gateway can skip uploading chunks that already exist.
--
-- Security properties:
--   - content_id is HMAC(tenant_pepper, plaintext_hash) — opaque without pepper
--   - Cross-tenant dedup impossible (different peppers → different content_ids)
--   - blob_key is content-addressed: "blob_{content_id}_{chunk_hash}"

CREATE TABLE IF NOT EXISTS content_entries (
    content_id          TEXT PRIMARY KEY,
    tenant_id           TEXT NOT NULL REFERENCES tenants(id),
    plaintext_size      BIGINT NOT NULL,
    chunk_count         BIGINT NOT NULL,
    chunk_size          BIGINT NOT NULL,
    chunk_plan_root     TEXT NOT NULL,
    pepper_generation   INTEGER NOT NULL DEFAULT 1,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(tenant_id, content_id)
);

CREATE INDEX IF NOT EXISTS content_entries_tenant_idx
    ON content_entries(tenant_id);

CREATE TABLE IF NOT EXISTS content_chunks (
    content_id          TEXT NOT NULL REFERENCES content_entries(content_id),
    chunk_index         INTEGER NOT NULL,
    chunk_content_hash  TEXT NOT NULL,
    blob_key            TEXT NOT NULL,
    plaintext_len       BIGINT NOT NULL,
    ciphertext_len      BIGINT NOT NULL,
    PRIMARY KEY(content_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS content_chunks_hash_idx
    ON content_chunks(chunk_content_hash);

-- Add content_id and protocol to file_versions for KDRV1 support.
ALTER TABLE file_versions
    ADD COLUMN IF NOT EXISTS content_id TEXT,
    ADD COLUMN IF NOT EXISTS protocol TEXT NOT NULL DEFAULT 'KDRV1';

-- Add tenant_id to blob_placements for tenant-scoped dedup checks.
-- Existing rows get '' (empty) as a placeholder; new rows must set it.
ALTER TABLE blob_placements
    ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS blob_placements_tenant_hash_idx
    ON blob_placements(tenant_id, checksum_sha256);

INSERT INTO schema_version (version) VALUES (4)
    ON CONFLICT (version) DO NOTHING;
