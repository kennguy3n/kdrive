// Package metadata — Content deduplication store methods.
//
// These methods extend Store with CRUD for the content_entries and
// content_chunks tables introduced in migration 004_content_dedup.sql.
// They enable intra-tenant content dedup: multiple file versions can
// reference the same content entry, and the gateway can skip uploading
// chunks that already exist.
package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// --- ContentEntry ---

// ContentEntry is a row from the content_entries table.
type ContentEntry struct {
	ContentID        string
	TenantID         string
	PlaintextSize    int64
	ChunkCount       int64
	ChunkSize        int64
	ChunkPlanRoot    string
	PepperGeneration int
	CreatedAt        time.Time
}

// ContentChunk is a row from the content_chunks table.
type ContentChunk struct {
	ChunkIndex       int
	ChunkContentHash string
	BlobKey          string
	PlaintextLen     int64
	CiphertextLen    int64
}

// ChunkCheckResultEntry is the result of checking a single chunk hash.
type ChunkCheckResultEntry struct {
	Hash    string
	Exists  bool
	BlobKey string // empty if !Exists
}

// GetContentEntry returns the content entry for the given content_id,
// scoped to the given tenant. Returns ErrNotFound if not found or if
// the entry belongs to a different tenant.
func (s *Store) GetContentEntry(ctx context.Context, contentID, tenantID string) (*ContentEntry, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT content_id, tenant_id, plaintext_size, chunk_count, chunk_size,
		        chunk_plan_root, pepper_generation, created_at
		 FROM content_entries
		 WHERE content_id = $1 AND tenant_id = $2`,
		contentID, tenantID)
	var e ContentEntry
	if err := row.Scan(&e.ContentID, &e.TenantID, &e.PlaintextSize, &e.ChunkCount,
		&e.ChunkSize, &e.ChunkPlanRoot, &e.PepperGeneration, &e.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &e, nil
}

// GetContentEntryByContentID returns the content entry for the given content_id
// regardless of tenant. Used to check if a content_id is owned by any tenant.
// Returns ErrNotFound if not found.
func (s *Store) GetContentEntryByContentID(ctx context.Context, contentID string) (*ContentEntry, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT content_id, tenant_id, plaintext_size, chunk_count, chunk_size,
		        chunk_plan_root, pepper_generation, created_at
		 FROM content_entries
		 WHERE content_id = $1`,
		contentID)
	var e ContentEntry
	if err := row.Scan(&e.ContentID, &e.TenantID, &e.PlaintextSize, &e.ChunkCount,
		&e.ChunkSize, &e.ChunkPlanRoot, &e.PepperGeneration, &e.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &e, nil
}

// CreateContentEntry inserts a new content entry. This is idempotent:
// if the content_id already exists for this tenant, it returns nil
// without modifying the existing row.
func (s *Store) CreateContentEntry(ctx context.Context, e ContentEntry) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO content_entries
		   (content_id, tenant_id, plaintext_size, chunk_count, chunk_size,
		    chunk_plan_root, pepper_generation)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (content_id) DO NOTHING`,
		e.ContentID, e.TenantID, e.PlaintextSize, e.ChunkCount, e.ChunkSize,
		e.ChunkPlanRoot, e.PepperGeneration)
	return err
}

// GetContentChunks returns all chunks for the given content_id.
// Note: content_id is HMAC(tenant_pepper, plaintext_hash), so it is already
// tenant-scoped by construction (different tenants produce different content_ids).
// The tenant_id parameter provides defense-in-depth.
func (s *Store) GetContentChunks(ctx context.Context, contentID, tenantID string) ([]ContentChunk, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT cc.chunk_index, cc.chunk_content_hash, cc.blob_key, cc.plaintext_len, cc.ciphertext_len
		 FROM content_chunks cc
		 JOIN content_entries ce ON cc.content_id = ce.content_id
		 WHERE cc.content_id = $1 AND ce.tenant_id = $2
		 ORDER BY cc.chunk_index`,
		contentID, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var chunks []ContentChunk
	for rows.Next() {
		var c ContentChunk
		if err := rows.Scan(&c.ChunkIndex, &c.ChunkContentHash, &c.BlobKey,
			&c.PlaintextLen, &c.CiphertextLen); err != nil {
			return nil, err
		}
		chunks = append(chunks, c)
	}
	return chunks, rows.Err()
}

// CreateContentChunks inserts all chunks for a content entry.
// Chunks are inserted idempotently (ON CONFLICT DO NOTHING).
// Uses a prepared statement executed per row within a transaction
// for efficiency and safety (avoids dynamic SQL string building).
func (s *Store) CreateContentChunks(ctx context.Context, contentID string, chunks []ContentChunk) error {
	if len(chunks) == 0 {
		return nil
	}
	// Cap batch size to prevent excessively long transactions.
	const maxChunksPerCall = 10000
	if len(chunks) > maxChunksPerCall {
		return fmt.Errorf("metadata: CreateContentChunks: %d chunks exceeds limit of %d", len(chunks), maxChunksPerCall)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	// Prepare the single-row INSERT once and reuse it for every chunk.
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO content_chunks
		   (content_id, chunk_index, chunk_content_hash, blob_key,
		    plaintext_len, ciphertext_len)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (content_id, chunk_index) DO NOTHING`)
	if err != nil {
		return err
	}
	defer func() {
		if stmt != nil {
			stmt.Close() //nolint:errcheck
		}
	}()

	for _, c := range chunks {
		if _, err := stmt.ExecContext(ctx,
			contentID, c.ChunkIndex, c.ChunkContentHash, c.BlobKey,
			c.PlaintextLen, c.CiphertextLen); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// CheckChunkHashes checks which chunk hashes already exist for the given
// content_id (scoped to the tenant). Returns a result entry per input hash.
// Uses ANY() to filter at the database level instead of loading all chunks.
func (s *Store) CheckChunkHashes(ctx context.Context, contentID, tenantID string, hashes []string) ([]ChunkCheckResultEntry, error) {
	// Chunk-level dedup searches across ALL content_chunks for this tenant,
	// not just the given content_id. This allows a new file version to reuse
	// chunks from a different content_id (e.g., chunk 1 is identical between
	// two versions of a file but the overall content_id differs because the
	// plaintext hash includes the modified chunk 2).
	//
	// The content_id parameter is retained for API compatibility but the
	// query is scoped to tenant_id only.
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT chunk_content_hash, blob_key
		 FROM content_chunks
		 WHERE content_id IN (
		     SELECT content_id FROM content_entries WHERE tenant_id = $1
		 ) AND chunk_content_hash = ANY($2)`,
		tenantID, pq.Array(hashes))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	knownHashes := map[string]string{} // hash → blob_key
	for rows.Next() {
		var hash, blobKey string
		if err := rows.Scan(&hash, &blobKey); err != nil {
			return nil, err
		}
		knownHashes[hash] = blobKey
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	results := make([]ChunkCheckResultEntry, len(hashes))
	for i, h := range hashes {
		blobKey, exists := knownHashes[h]
		results[i] = ChunkCheckResultEntry{
			Hash:    h,
			Exists:  exists,
			BlobKey: blobKey,
		}
	}
	return results, nil
}
