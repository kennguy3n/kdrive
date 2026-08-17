// Package metadata — Drive table accessors. These methods extend Store
// with CRUD for the tables introduced in migration 003_drive_demo.sql:
// folders, nodes, encryption_domains, key_envelopes, share_grants,
// access_context_snapshots.
package metadata

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// --- Folder ---

// Folder is a folder row from the drive schema.
type Folder struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	ParentFolderID string    `json:"parent_folder_id"`
	NameEncrypted  []byte    `json:"name_encrypted"`
	PrivacyMode    string    `json:"privacy_mode"`
	CreatedAt      time.Time `json:"created_at"`
}

// CreateFolder inserts a folder.
func (s *Store) CreateFolder(ctx context.Context, f Folder) error {
	var parentID interface{}
	if f.ParentFolderID != "" {
		parentID = f.ParentFolderID
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO folders (id, tenant_id, parent_folder_id, name_encrypted, privacy_mode)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (id) DO NOTHING`,
		f.ID, f.TenantID, parentID, f.NameEncrypted, f.PrivacyMode)
	return err
}

// ListFolders returns child folders of parentFolderID (or root folders
// if parentFolderID is empty) for the given tenant. Results are capped
// at 1000 rows to prevent unbounded result sets.
func (s *Store) ListFolders(ctx context.Context, tenantID, parentFolderID string) ([]Folder, error) {
	q := `SELECT id, tenant_id, COALESCE(parent_folder_id, ''), name_encrypted, privacy_mode, created_at
	      FROM folders WHERE tenant_id = $1 AND deleted_at IS NULL`
	args := []interface{}{tenantID}
	if parentFolderID == "" {
		q += ` AND parent_folder_id IS NULL`
	} else {
		q += ` AND parent_folder_id = $2`
		args = append(args, parentFolderID)
	}
	q += ` ORDER BY created_at LIMIT 1000`

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Folder
	for rows.Next() {
		var f Folder
		if err := rows.Scan(&f.ID, &f.TenantID, &f.ParentFolderID, &f.NameEncrypted, &f.PrivacyMode, &f.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// GetFolder fetches a folder by ID.
func (s *Store) GetFolder(ctx context.Context, id string) (*Folder, error) {
	var row *sql.Row
	if s.getFolder != nil {
		row = s.getFolder.QueryRowContext(ctx, id)
	} else {
		row = s.db.QueryRowContext(ctx,
			`SELECT id, tenant_id, COALESCE(parent_folder_id, ''), name_encrypted, privacy_mode, created_at
		 FROM folders WHERE id = $1 AND deleted_at IS NULL`, id)
	}
	var f Folder
	if err := row.Scan(&f.ID, &f.TenantID, &f.ParentFolderID, &f.NameEncrypted, &f.PrivacyMode, &f.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &f, nil
}

// --- Node ---

// Node is a file node in the drive schema.
type Node struct {
	ID            string    `json:"id"`
	TenantID      string    `json:"tenant_id"`
	FolderID      string    `json:"folder_id"`
	NameEncrypted []byte    `json:"name_encrypted"`
	MimeType      string    `json:"mime_type"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// CreateNode inserts a file node.
func (s *Store) CreateNode(ctx context.Context, n Node) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO nodes (id, tenant_id, folder_id, name_encrypted, mime_type)
		 VALUES ($1, $2, $3, $4, $5)`,
		n.ID, n.TenantID, n.FolderID, n.NameEncrypted, n.MimeType)
	return err
}

// ListNodes returns files in a folder. Results are capped at 1000
// rows to prevent unbounded result sets.
func (s *Store) ListNodes(ctx context.Context, folderID, tenantID string) ([]Node, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, tenant_id, folder_id, name_encrypted, mime_type, created_at, updated_at
		 FROM nodes WHERE folder_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
		 ORDER BY created_at LIMIT 1000`,
		folderID, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Node
	for rows.Next() {
		var n Node
		if err := rows.Scan(&n.ID, &n.TenantID, &n.FolderID, &n.NameEncrypted, &n.MimeType, &n.CreatedAt, &n.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// GetNode fetches a node by ID.
func (s *Store) GetNode(ctx context.Context, id string) (*Node, error) {
	var row *sql.Row
	if s.getNode != nil {
		row = s.getNode.QueryRowContext(ctx, id)
	} else {
		row = s.db.QueryRowContext(ctx,
			`SELECT id, tenant_id, folder_id, name_encrypted, mime_type, created_at, updated_at
		 FROM nodes WHERE id = $1 AND deleted_at IS NULL`, id)
	}
	var n Node
	if err := row.Scan(&n.ID, &n.TenantID, &n.FolderID, &n.NameEncrypted, &n.MimeType, &n.CreatedAt, &n.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &n, nil
}

// --- EncryptionDomain ---

// EncryptionDomain is an encryption domain row.
type EncryptionDomain struct {
	ID              string       `json:"id"`
	TenantID        string       `json:"tenant_id"`
	FolderID        string       `json:"folder_id"`
	PrivacyMode     string       `json:"privacy_mode"`
	Generation      int          `json:"generation"`
	PrevGeneration  *int         `json:"prev_generation"`
	PrevKeyEnvelope []byte       `json:"prev_key_envelope"`
	CreatedAt       time.Time    `json:"created_at"`
	RotatedAt       sql.NullTime `json:"rotated_at"`
}

// CreateEncryptionDomain inserts a new encryption domain.
func (s *Store) CreateEncryptionDomain(ctx context.Context, d EncryptionDomain) error {
	var folderID interface{}
	if d.FolderID != "" {
		folderID = d.FolderID
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO encryption_domains (id, tenant_id, folder_id, privacy_mode, generation)
		 VALUES ($1, $2, $3, $4, $5)`,
		d.ID, d.TenantID, folderID, d.PrivacyMode, d.Generation)
	return err
}

// GetEncryptionDomain fetches an encryption domain by ID.
func (s *Store) GetEncryptionDomain(ctx context.Context, id, tenantID string) (*EncryptionDomain, error) {
	var row *sql.Row
	if s.getEncryptionDomain != nil {
		row = s.getEncryptionDomain.QueryRowContext(ctx, id, tenantID)
	} else {
		row = s.db.QueryRowContext(ctx,
			`SELECT id, tenant_id, COALESCE(folder_id, ''), privacy_mode, generation,
			        prev_generation, prev_key_envelope, created_at, rotated_at
		 FROM encryption_domains WHERE id = $1 AND tenant_id = $2`, id, tenantID)
	}
	var d EncryptionDomain
	if err := row.Scan(&d.ID, &d.TenantID, &d.FolderID, &d.PrivacyMode, &d.Generation,
		&d.PrevGeneration, &d.PrevKeyEnvelope, &d.CreatedAt, &d.RotatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &d, nil
}

// RotateEncryptionDomain increments the generation and stores the
// previous key envelope.
func (s *Store) RotateEncryptionDomain(ctx context.Context, id, tenantID string, prevKeyEnvelope []byte) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var curGen int
	if err := tx.QueryRowContext(ctx,
		`SELECT generation FROM encryption_domains WHERE id = $1 AND tenant_id = $2 FOR UPDATE`,
		id, tenantID).Scan(&curGen); err != nil {
		return err
	}
	newGen := curGen + 1
	if _, err := tx.ExecContext(ctx,
		`UPDATE encryption_domains
		    SET generation = $3, prev_generation = $4, prev_key_envelope = $5, rotated_at = now()
		  WHERE id = $1 AND tenant_id = $2`,
		id, tenantID, newGen, curGen, prevKeyEnvelope); err != nil {
		return err
	}
	return tx.Commit()
}

// --- KeyEnvelope ---

// KeyEnvelope is a key envelope row (ciphertext only).
type KeyEnvelope struct {
	ID              string    `json:"id"`
	DomainID        string    `json:"domain_id"`
	VersionID       string    `json:"version_id"`
	TenantID        string    `json:"tenant_id"`
	EnvelopeType    string    `json:"envelope_type"`
	Ciphertext      []byte    `json:"ciphertext"`
	Nonce           []byte    `json:"nonce"`
	EncapsulatedKey []byte    `json:"encapsulated_key"`
	Metadata        []byte    `json:"metadata"` // JSON
	CreatedAt       time.Time `json:"created_at"`
}

// CreateKeyEnvelope inserts a key envelope.
func (s *Store) CreateKeyEnvelope(ctx context.Context, e KeyEnvelope) error {
	var domainID, versionID interface{}
	if e.DomainID != "" {
		domainID = e.DomainID
	}
	if e.VersionID != "" {
		versionID = e.VersionID
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO key_envelopes (id, domain_id, version_id, tenant_id, envelope_type, ciphertext, nonce, encapsulated_key, metadata)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		e.ID, domainID, versionID, e.TenantID, e.EnvelopeType, e.Ciphertext, e.Nonce, e.EncapsulatedKey, e.Metadata)
	return err
}

// ListEnvelopesByDomain returns key envelopes for a domain. Results
// are capped at 1000 rows.
func (s *Store) ListEnvelopesByDomain(ctx context.Context, domainID, tenantID string) ([]KeyEnvelope, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, COALESCE(domain_id, ''), COALESCE(version_id, ''), tenant_id, envelope_type,
		        ciphertext, nonce, COALESCE(encapsulated_key, ''::bytea), metadata, created_at
		 FROM key_envelopes WHERE domain_id = $1 AND tenant_id = $2
		 ORDER BY created_at LIMIT 1000`,
		domainID, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []KeyEnvelope
	for rows.Next() {
		var e KeyEnvelope
		if err := rows.Scan(&e.ID, &e.DomainID, &e.VersionID, &e.TenantID, &e.EnvelopeType,
			&e.Ciphertext, &e.Nonce, &e.EncapsulatedKey, &e.Metadata, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ListEnvelopesByVersion returns key envelopes for a version.
func (s *Store) ListEnvelopesByVersion(ctx context.Context, versionID string) ([]KeyEnvelope, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, COALESCE(domain_id, ''), COALESCE(version_id, ''), tenant_id, envelope_type,
		        ciphertext, nonce, COALESCE(encapsulated_key, ''::bytea), metadata, created_at
		 FROM key_envelopes WHERE version_id = $1 ORDER BY created_at`, versionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []KeyEnvelope
	for rows.Next() {
		var e KeyEnvelope
		if err := rows.Scan(&e.ID, &e.DomainID, &e.VersionID, &e.TenantID, &e.EnvelopeType,
			&e.Ciphertext, &e.Nonce, &e.EncapsulatedKey, &e.Metadata, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// --- ShareGrant ---

// ShareGrant is a share grant row for Max mode.
type ShareGrant struct {
	ID            string       `json:"id"`
	TenantID      string       `json:"tenant_id"`
	NodeID        string       `json:"node_id"`
	GrantorUserID string       `json:"grantor_user_id"`
	GranteeUserID string       `json:"grantee_user_id"`
	Generation    int          `json:"generation"`
	IsActive      bool         `json:"is_active"`
	KeyEnvelopeID string       `json:"key_envelope_id"`
	CreatedAt     time.Time    `json:"created_at"`
	RevokedAt     sql.NullTime `json:"revoked_at"`
}

// CreateShareGrant inserts a share grant.
func (s *Store) CreateShareGrant(ctx context.Context, g ShareGrant) error {
	var nodeID, keyEnvID interface{}
	if g.NodeID != "" {
		nodeID = g.NodeID
	}
	if g.KeyEnvelopeID != "" {
		keyEnvID = g.KeyEnvelopeID
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO share_grants (id, tenant_id, node_id, grantor_user_id, grantee_user_id, generation, is_active, key_envelope_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		g.ID, g.TenantID, nodeID, g.GrantorUserID, g.GranteeUserID, g.Generation, g.IsActive, keyEnvID)
	return err
}

// ListActiveShareGrants returns active grants for a grantee
// (tenant-scoped). Results are capped at 1000 rows.
func (s *Store) ListActiveShareGrants(ctx context.Context, granteeUserID, tenantID string) ([]ShareGrant, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, tenant_id, COALESCE(node_id, ''), grantor_user_id, grantee_user_id,
		        generation, is_active, COALESCE(key_envelope_id, ''), created_at, revoked_at
		 FROM share_grants WHERE grantee_user_id = $1 AND tenant_id = $2 AND is_active = true
		 ORDER BY created_at DESC LIMIT 1000`,
		granteeUserID, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ShareGrant
	for rows.Next() {
		var g ShareGrant
		if err := rows.Scan(&g.ID, &g.TenantID, &g.NodeID, &g.GrantorUserID, &g.GranteeUserID,
			&g.Generation, &g.IsActive, &g.KeyEnvelopeID, &g.CreatedAt, &g.RevokedAt); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// RevokeShareGrant marks a share grant as revoked (tenant-scoped).
func (s *Store) RevokeShareGrant(ctx context.Context, id, tenantID string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE share_grants SET is_active = false, revoked_at = now()
		  WHERE id = $1 AND tenant_id = $2 AND is_active = true`, id, tenantID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// --- AccessContextSnapshot ---

// AccessContextSnapshot is an ACL snapshot row.
type AccessContextSnapshot struct {
	ID            string    `json:"id"`
	TenantID      string    `json:"tenant_id"`
	NodeID        string    `json:"node_id"`
	Revision      int       `json:"revision"`
	SnapshotHash  []byte    `json:"snapshot_hash"`
	ACLCiphertext []byte    `json:"acl_ciphertext"`
	CreatedAt     time.Time `json:"created_at"`
}

// CreateAccessContextSnapshot inserts a snapshot.
func (s *Store) CreateAccessContextSnapshot(ctx context.Context, a AccessContextSnapshot) error {
	var nodeID interface{}
	if a.NodeID != "" {
		nodeID = a.NodeID
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO access_context_snapshots (id, tenant_id, node_id, revision, snapshot_hash, acl_ciphertext)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		a.ID, a.TenantID, nodeID, a.Revision, a.SnapshotHash, a.ACLCiphertext)
	return err
}

// GetLatestAccessContext returns the latest snapshot for a node (tenant-scoped).
func (s *Store) GetLatestAccessContext(ctx context.Context, nodeID, tenantID string) (*AccessContextSnapshot, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, COALESCE(node_id, ''), revision, snapshot_hash, acl_ciphertext, created_at
		 FROM access_context_snapshots WHERE node_id = $1 AND tenant_id = $2 ORDER BY revision DESC LIMIT 1`,
		nodeID, tenantID)
	var a AccessContextSnapshot
	if err := row.Scan(&a.ID, &a.TenantID, &a.NodeID, &a.Revision, &a.SnapshotHash, &a.ACLCiphertext, &a.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &a, nil
}

// --- Tenant extensions ---

// DriveTenant is a tenant with the drive-specific columns
// (tenant_type, bucket_name) added by migration 003.
type DriveTenant struct {
	ID          string `json:"id"`
	PoolID      string `json:"pool_id"`
	PrivacyMode string `json:"privacy_mode"`
	TenantType  string `json:"tenant_type"`
	BucketName  string `json:"bucket_name"`
}

// GetDriveTenant fetches a tenant with the drive columns.
func (s *Store) GetDriveTenant(ctx context.Context, id string) (*DriveTenant, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, pool_id, privacy_mode, tenant_type, bucket_name FROM tenants WHERE id = $1`, id)
	var t DriveTenant
	if err := row.Scan(&t.ID, &t.PoolID, &t.PrivacyMode, &t.TenantType, &t.BucketName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &t, nil
}

// ListDriveTenants returns all tenants.
func (s *Store) ListDriveTenants(ctx context.Context) ([]DriveTenant, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, pool_id, privacy_mode, tenant_type, bucket_name FROM tenants ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DriveTenant
	for rows.Next() {
		var t DriveTenant
		if err := rows.Scan(&t.ID, &t.PoolID, &t.PrivacyMode, &t.TenantType, &t.BucketName); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
