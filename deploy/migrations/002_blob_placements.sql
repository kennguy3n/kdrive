-- M4: Separate blob-level placement tracking from file_versions.
--
-- The blob_placements table tracks where a blob's bytes currently live
-- (CACHED vs COMMITTED_DURABLE) keyed by the opaque blob_key. This
-- decouples placement state from file_versions so that when dedup is
-- implemented, multiple file_versions can reference the same blob_key
-- without the UPDATE-by-blob_key problem that existed when placement
-- columns lived on file_versions.
--
-- Key design decisions:
--   - blob_key is the PRIMARY KEY: one row per unique blob.
--   - commit_state, blob_version_id, size_bytes, checksum_sha256,
--     durable_at, retention_mode, retain_until, legal_hold move here
--     from file_versions.
--   - file_versions retains blob_key as a reference (no FK for now;
--     a FK can be added once all writers populate blob_placements
--     first).
--   - updated_at tracks the last placement change for observability.
--
-- See plan §5 and the dedup-readiness review.

CREATE TABLE IF NOT EXISTS blob_placements (
    blob_key            TEXT PRIMARY KEY,
    commit_state        TEXT NOT NULL DEFAULT 'CACHED',
    -- commit_state: CACHED | COMMITTED_DURABLE
    blob_version_id     TEXT,
    size_bytes          BIGINT NOT NULL,
    checksum_sha256     TEXT NOT NULL,
    cached_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    durable_at          TIMESTAMPTZ,
    retention_mode      TEXT NOT NULL DEFAULT 'NONE',
    retain_until        TIMESTAMPTZ,
    legal_hold          BOOLEAN NOT NULL DEFAULT false,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS blob_placements_commit_state_idx
    ON blob_placements (commit_state)
    WHERE commit_state = 'CACHED';

-- Backfill blob_placements from existing file_versions data (for
-- deployments that already ran 001_init.sql). ON CONFLICT handles
-- duplicate blob_keys from multiple file_versions sharing one blob.
INSERT INTO blob_placements
    (blob_key, commit_state, blob_version_id, size_bytes,
     checksum_sha256, cached_at, durable_at, retention_mode,
     retain_until, legal_hold, created_at, updated_at)
SELECT DISTINCT ON (blob_key)
       blob_key, commit_state, blob_version_id, size_bytes,
       checksum_sha256, COALESCE(cached_at, created_at), durable_at,
       retention_mode, retain_until, legal_hold, created_at, now()
  FROM file_versions
 WHERE deleted_at IS NULL
   AND blob_key NOT IN (SELECT blob_key FROM blob_placements)
 ORDER BY blob_key, created_at DESC;

INSERT INTO schema_version (version) VALUES (2)
    ON CONFLICT (version) DO NOTHING;
