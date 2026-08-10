-- M6: KDRV1 upload sessions table.
--
-- The existing upload_sessions table tracks blob-level multi-part uploads
-- (upload_id, parts, expected_size). The KDRV1 upload flow needs to store
-- different metadata: chunk_plan, manifest, header, wrapped_dek, and
-- registered chunks. This migration creates a dedicated table for that.
--
-- Data is transient: sessions are created at upload:initiate and consumed
-- at upload:commit. A cleanup worker can purge sessions older than 24h.

CREATE TABLE IF NOT EXISTS kdrv1_upload_sessions (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL REFERENCES tenants(id),
    node_id         TEXT NOT NULL,
    folder_id       TEXT NOT NULL DEFAULT '',
    chunk_plan      JSONB NOT NULL DEFAULT '{}'::jsonb,
    manifest        JSONB NOT NULL DEFAULT '{}'::jsonb,
    header          JSONB NOT NULL DEFAULT '{}'::jsonb,
    wrapped_dek     TEXT NOT NULL DEFAULT '',
    wrap_nonce      TEXT NOT NULL DEFAULT '',
    chunks          JSONB NOT NULL DEFAULT '[]'::jsonb,
    state           TEXT NOT NULL DEFAULT 'UPLOADING',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS kdrv1_upload_sessions_state_idx
    ON kdrv1_upload_sessions (state)
    WHERE state = 'UPLOADING';

CREATE INDEX IF NOT EXISTS kdrv1_upload_sessions_tenant_idx
    ON kdrv1_upload_sessions (tenant_id);

INSERT INTO schema_version (version) VALUES (5)
    ON CONFLICT (version) DO NOTHING;
