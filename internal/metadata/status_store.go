// Package metadata — PostgresStatusStore implements blobio.StatusStore
// against the blob_placements table. It is the production status store
// so that blob placement status survives gateway restarts.
//
// The blob_placements table is keyed by blob_key (PRIMARY KEY), so
// multiple file_versions can reference the same blob_key once dedup
// is implemented. The UPDATE no longer touches file_versions, avoiding
// the cross-version contamination that existed when placement columns
// lived on file_versions.
package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/kchat/drive/internal/blobio"
	"github.com/kchat/drive/pkg/blobstore"
)

// PostgresStatusStore is a blobio.StatusStore backed by the
// blob_placements table.
type PostgresStatusStore struct {
	db *sql.DB
}

// NewStatusStore returns a Postgres-backed StatusStore.
func NewStatusStore(db *sql.DB) *PostgresStatusStore {
	return &PostgresStatusStore{db: db}
}

// Get returns the BlobStatus for blobID (looked up by blob_key), or
// nil if no row exists.
func (s *PostgresStatusStore) Get(ctx context.Context, blobID string) (*blobio.BlobStatus, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT blob_key, commit_state, size_bytes, checksum_sha256,
		        blob_version_id, cached_at, durable_at,
		        retention_mode, retain_until, legal_hold
		   FROM blob_placements
		  WHERE blob_key = $1`, blobID)
	var (
		blobKey       string
		commitState   string
		sizeBytes     int64
		checksum      string
		blobVersionID sql.NullString
		cachedAt      sql.NullTime
		durableAt     sql.NullTime
		retentionMode string
		retainUntil   sql.NullTime
		legalHold     bool
	)
	if err := row.Scan(&blobKey, &commitState, &sizeBytes, &checksum,
		&blobVersionID, &cachedAt, &durableAt,
		&retentionMode, &retainUntil, &legalHold); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("metadata: status get: %w", err)
	}
	status := &blobio.BlobStatus{
		BlobID:         blobKey,
		CommitState:    blobio.CommitState(commitState),
		Size:           sizeBytes,
		ChecksumSHA256: checksum,
		VersionID:      blobVersionID.String,
	}
	if cachedAt.Valid {
		status.CachedAt = cachedAt.Time
	}
	if durableAt.Valid {
		status.DurableAt = durableAt.Time
	}
	status.Retention = blobstore.RetentionSpec{
		Mode:      blobstore.RetentionMode(retentionMode),
		LegalHold: legalHold,
	}
	if retainUntil.Valid {
		status.Retention.RetainUntil = retainUntil.Time
	}
	return status, nil
}

// Put upserts the blob placement status. It inserts a new
// blob_placements row when the blob is first cached, and updates it
// when the commit state changes (e.g. CACHED → COMMITTED_DURABLE).
// The upsert is atomic via ON CONFLICT(blob_key) DO UPDATE.
func (s *PostgresStatusStore) Put(ctx context.Context, status *blobio.BlobStatus) error {
	var retainUntil any
	if !status.Retention.RetainUntil.IsZero() {
		retainUntil = status.Retention.RetainUntil
	}
	var durableAt any
	if !status.DurableAt.IsZero() {
		durableAt = status.DurableAt
	}
	var cachedAt any
	if !status.CachedAt.IsZero() {
		cachedAt = status.CachedAt
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO blob_placements
		   (blob_key, commit_state, blob_version_id, size_bytes,
		    checksum_sha256, cached_at, durable_at, retention_mode,
		    retain_until, legal_hold, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())
		 ON CONFLICT (blob_key) DO UPDATE
		   SET commit_state    = EXCLUDED.commit_state,
		       blob_version_id = EXCLUDED.blob_version_id,
		       size_bytes      = EXCLUDED.size_bytes,
		       checksum_sha256 = EXCLUDED.checksum_sha256,
		       durable_at      = EXCLUDED.durable_at,
		       retention_mode  = EXCLUDED.retention_mode,
		       retain_until    = EXCLUDED.retain_until,
		       legal_hold      = EXCLUDED.legal_hold,
		       updated_at      = now()`,
		status.BlobID, string(status.CommitState),
		nullString(status.VersionID),
		status.Size, status.ChecksumSHA256,
		cachedAt, durableAt,
		string(status.Retention.Mode), retainUntil,
		status.Retention.LegalHold)
	if err != nil {
		return fmt.Errorf("metadata: status put: %w", err)
	}
	return nil
}

// --- Methods for worker jobs and backpressure monitoring ---

// CachedPlacement is a row from blob_placements in CACHED state.
type CachedPlacement struct {
	BlobKey        string
	SizeBytes      int64
	ChecksumSHA256 string
	CachedAt       time.Time
}

// ListCachedPlacements returns up to limit blob_placements in CACHED
// state, oldest first. The PromotionJob uses this to find blobs that
// need promoting to Wasabi.
func (s *PostgresStatusStore) ListCachedPlacements(ctx context.Context, limit int) ([]CachedPlacement, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT blob_key, size_bytes, checksum_sha256, cached_at
		   FROM blob_placements
		  WHERE commit_state = 'CACHED'
		  ORDER BY cached_at
		  LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("metadata: list cached placements: %w", err)
	}
	defer rows.Close()
	var out []CachedPlacement
	for rows.Next() {
		var p CachedPlacement
		if err := rows.Scan(&p.BlobKey, &p.SizeBytes, &p.ChecksumSHA256, &p.CachedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// DurablePlacement is a row from blob_placements in COMMITTED_DURABLE
// state, sampled for repair verification.
type DurablePlacement struct {
	BlobKey        string
	BlobVersionID  string
	SizeBytes      int64
	ChecksumSHA256 string
	DurableAt      time.Time
}

// SampleDurablePlacements returns up to limit random durable blob
// placements for repair verification. Uses ORDER BY random() for
// sampling; on very large tables this could be replaced with
// TABLESAMPLE SYSTEM for better performance.
func (s *PostgresStatusStore) SampleDurablePlacements(ctx context.Context, limit int) ([]DurablePlacement, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT blob_key, COALESCE(blob_version_id, ''), size_bytes,
		        checksum_sha256, durable_at
		   FROM blob_placements
		  WHERE commit_state = 'COMMITTED_DURABLE'
		  ORDER BY random()
		  LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("metadata: sample durable placements: %w", err)
	}
	defer rows.Close()
	var out []DurablePlacement
	for rows.Next() {
		var p DurablePlacement
		var durableAt sql.NullTime
		if err := rows.Scan(&p.BlobKey, &p.BlobVersionID, &p.SizeBytes,
			&p.ChecksumSHA256, &durableAt); err != nil {
			return nil, err
		}
		if durableAt.Valid {
			p.DurableAt = durableAt.Time
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// CountCachedPlacements returns the number of blobs in CACHED state.
// Used for backpressure monitoring and queue-depth alerting.
func (s *PostgresStatusStore) CountCachedPlacements(ctx context.Context) (int64, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM blob_placements WHERE commit_state = 'CACHED'`)
	var n int64
	if err := row.Scan(&n); err != nil {
		return 0, fmt.Errorf("metadata: count cached placements: %w", err)
	}
	return n, nil
}

// MarkDurable updates a blob's placement to COMMITTED_DURABLE with the
// provider-verified version ID and durable timestamp. This is a
// targeted update used by the promote path after a successful PUT.
func (s *PostgresStatusStore) MarkDurable(ctx context.Context, blobKey, blobVersionID string, size int64, checksum string, durableAt time.Time) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE blob_placements
		    SET commit_state    = 'COMMITTED_DURABLE',
		        blob_version_id = $2,
		        size_bytes      = $3,
		        checksum_sha256 = $4,
		        durable_at      = $5,
		        updated_at      = now()
		  WHERE blob_key = $1`,
		blobKey, nullString(blobVersionID), size, checksum, durableAt)
	if err != nil {
		return fmt.Errorf("metadata: mark durable: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

var _ blobio.StatusStore = (*PostgresStatusStore)(nil)
