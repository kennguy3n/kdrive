// Package metadata — KDRV1 upload session store methods.
//
// These methods extend Store with CRUD for the kdrv1_upload_sessions
// table introduced in migration 005. The KDRV1 upload flow uses these
// sessions to track chunk_plan, manifest, header, and registered chunks
// between the upload:initiate and upload:commit calls.
package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// KDRV1UploadSession is a row from the kdrv1_upload_sessions table.
type KDRV1UploadSession struct {
	ID         string
	TenantID   string
	NodeID     string
	FolderID   string
	ChunkPlan  json.RawMessage
	Manifest   json.RawMessage
	Header     json.RawMessage
	WrappedDEK string
	WrapNonce  string
	Chunks     json.RawMessage // JSON array of chunk objects
	State      string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// CreateKDRV1UploadSession inserts a new KDRV1 upload session.
func (s *Store) CreateKDRV1UploadSession(ctx context.Context, sess KDRV1UploadSession) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO kdrv1_upload_sessions
		   (id, tenant_id, node_id, folder_id, chunk_plan, manifest,
		    header, wrapped_dek, wrap_nonce, chunks, state)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		sess.ID, sess.TenantID, sess.NodeID, sess.FolderID,
		sess.ChunkPlan, sess.Manifest, sess.Header,
		sess.WrappedDEK, sess.WrapNonce, sess.Chunks, sess.State)
	return err
}

// GetKDRV1UploadSession returns the upload session with the given ID,
// scoped to the given tenant. Returns ErrNotFound if not found or if
// the session belongs to a different tenant.
func (s *Store) GetKDRV1UploadSession(ctx context.Context, id, tenantID string) (*KDRV1UploadSession, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, node_id, folder_id, chunk_plan, manifest,
		        header, wrapped_dek, wrap_nonce, chunks, state, created_at, updated_at
		 FROM kdrv1_upload_sessions
		 WHERE id = $1 AND tenant_id = $2`,
		id, tenantID)
	var sess KDRV1UploadSession
	if err := row.Scan(&sess.ID, &sess.TenantID, &sess.NodeID, &sess.FolderID,
		&sess.ChunkPlan, &sess.Manifest, &sess.Header,
		&sess.WrappedDEK, &sess.WrapNonce, &sess.Chunks,
		&sess.State, &sess.CreatedAt, &sess.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &sess, nil
}

// AppendKDRV1UploadSessionChunk atomically appends a single chunk to the
// session's chunks JSON array using a transaction with SELECT FOR UPDATE.
// This prevents lost updates when multiple chunk uploads arrive concurrently.
// The tenantID parameter enforces tenant isolation: if the session belongs
// to a different tenant, ErrNotFound is returned (avoiding information leak).
func (s *Store) AppendKDRV1UploadSessionChunk(ctx context.Context, id, tenantID string, chunk json.RawMessage) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	// Lock the row for the duration of the transaction, scoped by tenant
	var existingChunks []byte
	err = tx.QueryRowContext(ctx,
		`SELECT chunks FROM kdrv1_upload_sessions WHERE id = $1 AND tenant_id = $2 FOR UPDATE`,
		id, tenantID).Scan(&existingChunks)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}

	// Parse existing chunks, append the new one, and marshal back
	var chunks []json.RawMessage
	if len(existingChunks) > 0 && string(existingChunks) != "[]" {
		if err := json.Unmarshal(existingChunks, &chunks); err != nil {
			return err
		}
	}
	chunks = append(chunks, chunk)
	merged, err := json.Marshal(chunks)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx,
		`UPDATE kdrv1_upload_sessions
		 SET chunks = $3, updated_at = now()
		 WHERE id = $1 AND tenant_id = $2`,
		id, tenantID, merged)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// UpdateKDRV1UploadSessionState updates the state of a session,
// scoped to the given tenant for isolation.
func (s *Store) UpdateKDRV1UploadSessionState(ctx context.Context, id, tenantID, state string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE kdrv1_upload_sessions
		 SET state = $3, updated_at = now()
		 WHERE id = $1 AND tenant_id = $2`,
		id, tenantID, state)
	return err
}

// DeleteKDRV1UploadSession removes a session (called after commit),
// scoped to the given tenant for isolation.
func (s *Store) DeleteKDRV1UploadSession(ctx context.Context, id, tenantID string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM kdrv1_upload_sessions WHERE id = $1 AND tenant_id = $2`,
		id, tenantID)
	return err
}
