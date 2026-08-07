-- 003: Drive demo tables — extends tenants and adds the minimal
-- schema needed for the React web sample to demonstrate the 3 privacy
-- modes (Secured / Advanced / Max) against the live Go gateway.
-- See plan-6a223e8bbfb25e00.md Part C.

-- Extend tenants with demo classification.
ALTER TABLE tenants
    ADD COLUMN IF NOT EXISTS tenant_type TEXT NOT NULL DEFAULT 'b2b',
    ADD COLUMN IF NOT EXISTS bucket_name TEXT NOT NULL DEFAULT '';

-- Folders: hierarchical folder tree per tenant. Root folders have
-- parent_folder_id = NULL. The name is opaque ciphertext (the gateway
-- never sees plaintext folder names).
CREATE TABLE IF NOT EXISTS folders (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL REFERENCES tenants(id),
    parent_folder_id TEXT REFERENCES folders(id),
    name_encrypted  BYTEA NOT NULL,
    privacy_mode    TEXT NOT NULL DEFAULT 'secured',
    -- privacy_mode: secured | advanced | max
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS folders_tenant_parent_idx
    ON folders (tenant_id, parent_folder_id)
    WHERE deleted_at IS NULL;

-- Nodes: files within folders. Replaces the simpler 'files' table for
-- the demo (files stays for backward compat; nodes is the demo table).
CREATE TABLE IF NOT EXISTS nodes (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL REFERENCES tenants(id),
    folder_id       TEXT NOT NULL REFERENCES folders(id),
    name_encrypted  BYTEA NOT NULL,
    mime_type       TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS nodes_folder_idx
    ON nodes (folder_id)
    WHERE deleted_at IS NULL;

-- Encryption domains: per-domain key generation tracking.
CREATE TABLE IF NOT EXISTS encryption_domains (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL REFERENCES tenants(id),
    folder_id       TEXT REFERENCES folders(id),
    privacy_mode    TEXT NOT NULL DEFAULT 'secured',
    generation      INTEGER NOT NULL DEFAULT 1,
    prev_generation INTEGER,
    prev_key_envelope BYTEA,
    -- prev_key_envelope: AEAD(DomainKey[g], DomainKey[g-1]) for backward chain
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    rotated_at      TIMESTAMPTZ
);

-- Key envelopes: encrypted key material stored server-side.
-- The gateway stores only ciphertext; it never holds plaintext keys.
CREATE TABLE IF NOT EXISTS key_envelopes (
    id              TEXT PRIMARY KEY,
    domain_id       TEXT REFERENCES encryption_domains(id),
    version_id      TEXT,
    tenant_id       TEXT NOT NULL REFERENCES tenants(id),
    envelope_type   TEXT NOT NULL,
    -- envelope_type: hpke | mls_transport | domain_wrap | share_grant_wrap | recovery
    ciphertext      BYTEA NOT NULL,
    nonce           BYTEA NOT NULL,
    encapsulated_key BYTEA,
    -- HPKE encap key (empty for MLS transport)
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS key_envelopes_domain_idx
    ON key_envelopes (domain_id);

CREATE INDEX IF NOT EXISTS key_envelopes_version_idx
    ON key_envelopes (version_id);

-- Share grants: per-share grant for Max mode.
CREATE TABLE IF NOT EXISTS share_grants (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL REFERENCES tenants(id),
    node_id         TEXT REFERENCES nodes(id),
    grantor_user_id TEXT NOT NULL,
    grantee_user_id TEXT NOT NULL,
    generation      INTEGER NOT NULL DEFAULT 1,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    key_envelope_id TEXT REFERENCES key_envelopes(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS share_grants_node_idx
    ON share_grants (node_id)
    WHERE is_active = true;

CREATE INDEX IF NOT EXISTS share_grants_grantee_idx
    ON share_grants (grantee_user_id)
    WHERE is_active = true;

-- Access context snapshots: minimal ACL snapshot for the demo.
CREATE TABLE IF NOT EXISTS access_context_snapshots (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL REFERENCES tenants(id),
    node_id         TEXT REFERENCES nodes(id),
    revision        INTEGER NOT NULL DEFAULT 1,
    snapshot_hash   BYTEA NOT NULL,
    acl_ciphertext  BYTEA NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS access_context_node_idx
    ON access_context_snapshots (node_id, revision);

-- Demo seed data: 1 B2C zone + 3 B2B tenants.
INSERT INTO tenants (id, pool_id, privacy_mode, tenant_type, bucket_name)
VALUES ('tenant_b2c', 'pool_b2c', 'max', 'b2c', 'b2c-zone')
ON CONFLICT (id) DO NOTHING;

INSERT INTO tenants (id, pool_id, privacy_mode, tenant_type, bucket_name)
VALUES ('tenant_acme', 'pool_acme', 'secured', 'b2b', 'acme')
ON CONFLICT (id) DO NOTHING;

INSERT INTO tenants (id, pool_id, privacy_mode, tenant_type, bucket_name)
VALUES ('tenant_globex', 'pool_globex', 'secured', 'b2b', 'globex')
ON CONFLICT (id) DO NOTHING;

INSERT INTO tenants (id, pool_id, privacy_mode, tenant_type, bucket_name)
VALUES ('tenant_initech', 'pool_initech', 'secured', 'b2b', 'initech')
ON CONFLICT (id) DO NOTHING;

-- Root folders for each B2B tenant (3 modes each).
-- Folder names are encrypted placeholders (16-byte zero prefix for demo).
INSERT INTO folders (id, tenant_id, parent_folder_id, name_encrypted, privacy_mode)
SELECT 'folder_acme_secured', 'tenant_acme', NULL, decode('0000000000000000000000000000000053656375726564', 'hex'), 'secured'
WHERE NOT EXISTS (SELECT 1 FROM folders WHERE id = 'folder_acme_secured');

INSERT INTO folders (id, tenant_id, parent_folder_id, name_encrypted, privacy_mode)
SELECT 'folder_acme_advanced', 'tenant_acme', NULL, decode('00000000000000000000000000000000416476616e636564', 'hex'), 'advanced'
WHERE NOT EXISTS (SELECT 1 FROM folders WHERE id = 'folder_acme_advanced');

INSERT INTO folders (id, tenant_id, parent_folder_id, name_encrypted, privacy_mode)
SELECT 'folder_acme_max', 'tenant_acme', NULL, decode('000000000000000000000000000000004d6178', 'hex'), 'max'
WHERE NOT EXISTS (SELECT 1 FROM folders WHERE id = 'folder_acme_max');

INSERT INTO folders (id, tenant_id, parent_folder_id, name_encrypted, privacy_mode)
SELECT 'folder_globex_secured', 'tenant_globex', NULL, decode('0000000000000000000000000000000053656375726564', 'hex'), 'secured'
WHERE NOT EXISTS (SELECT 1 FROM folders WHERE id = 'folder_globex_secured');

INSERT INTO folders (id, tenant_id, parent_folder_id, name_encrypted, privacy_mode)
SELECT 'folder_globex_advanced', 'tenant_globex', NULL, decode('00000000000000000000000000000000416476616e636564', 'hex'), 'advanced'
WHERE NOT EXISTS (SELECT 1 FROM folders WHERE id = 'folder_globex_advanced');

INSERT INTO folders (id, tenant_id, parent_folder_id, name_encrypted, privacy_mode)
SELECT 'folder_globex_max', 'tenant_globex', NULL, decode('000000000000000000000000000000004d6178', 'hex'), 'max'
WHERE NOT EXISTS (SELECT 1 FROM folders WHERE id = 'folder_globex_max');

INSERT INTO folders (id, tenant_id, parent_folder_id, name_encrypted, privacy_mode)
SELECT 'folder_initech_secured', 'tenant_initech', NULL, decode('0000000000000000000000000000000053656375726564', 'hex'), 'secured'
WHERE NOT EXISTS (SELECT 1 FROM folders WHERE id = 'folder_initech_secured');

INSERT INTO folders (id, tenant_id, parent_folder_id, name_encrypted, privacy_mode)
SELECT 'folder_initech_advanced', 'tenant_initech', NULL, decode('00000000000000000000000000000000416476616e636564', 'hex'), 'advanced'
WHERE NOT EXISTS (SELECT 1 FROM folders WHERE id = 'folder_initech_advanced');

INSERT INTO folders (id, tenant_id, parent_folder_id, name_encrypted, privacy_mode)
SELECT 'folder_initech_max', 'tenant_initech', NULL, decode('000000000000000000000000000000004d6178', 'hex'), 'max'
WHERE NOT EXISTS (SELECT 1 FROM folders WHERE id = 'folder_initech_max');

-- B2C root folder (Max only).
INSERT INTO folders (id, tenant_id, parent_folder_id, name_encrypted, privacy_mode)
SELECT 'folder_b2c_personal', 'tenant_b2c', NULL, decode('000000000000000000000000000000004d792046696c6573', 'hex'), 'max'
WHERE NOT EXISTS (SELECT 1 FROM folders WHERE id = 'folder_b2c_personal');

INSERT INTO schema_version (version) VALUES (3)
    ON CONFLICT (version) DO NOTHING;
