// Package metadata is the Postgres-backed metadata store for KChat
// Drive. It wraps the trimmed schema from deploy/migrations/001_init.sql
// and exposes the operations the gateway and worker need.
//
// The store keeps: tenants, files, file_versions, upload_sessions,
// quota_reservations, provider_write_intents, the outbox, and the
// erasure_ledger. It drops the multi-provider placement/allocation
// tables from v1 (plan §5).
package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// Store is the Postgres metadata store.
type Store struct {
	db *sql.DB
}

// Open opens a Postgres connection pool from a DSN and tunes it for
// the worker/gateway workload. The pool limits are conservative for
// a single-VM-pool deployment with 2 gateway replicas + 1 worker.
func Open(dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("metadata: open: %w", err)
	}
	// Pool tuning: the worker needs few connections; the gateway
	// needs more for concurrent requests. These limits are safe for
	// a single Postgres instance with max_connections=100.
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return db, nil
}

// New returns a Store backed by the given database.
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// DB returns the underlying *sql.DB so callers can close the
// connection pool on shutdown.
func (s *Store) DB() *sql.DB { return s.db }

// Ping verifies the database connection.
func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Migrate applies the schema migrations in order.
func (s *Store) Migrate(ctx context.Context, migrations []Migration) error {
	for _, m := range migrations {
		if err := m.Apply(ctx, s.db); err != nil {
			return fmt.Errorf("metadata: migration %d: %w", m.Version, err)
		}
	}
	return nil
}

// AutoMigrate applies all embedded migrations. This is the
// convenience method the gateway calls on startup.
func (s *Store) AutoMigrate(ctx context.Context) error {
	return s.Migrate(ctx, EmbeddedMigrations())
}

// Migration is a single schema migration.
type Migration struct {
	Version int
	SQL     string
}

// Apply runs the migration SQL and records the version.
func (m Migration) Apply(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
		// Ignore "already exists" errors so migrations are idempotent.
		if !isAlreadyExists(err) {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO schema_version (version) VALUES ($1) ON CONFLICT (version) DO NOTHING`,
		m.Version); err != nil {
		return err
	}
	return tx.Commit()
}

func isAlreadyExists(err error) bool {
	if pqErr, ok := err.(*pq.Error); ok {
		return pqErr.Code == "42P07" // duplicate_table
	}
	return false
}

// --- Tenant ---

// Tenant is a KChat Drive tenant row.
type Tenant struct {
	ID          string
	PoolID      string
	PrivacyMode string
	Guardrails  []byte // JSON
	CreatedAt   time.Time
}

// CreateTenant inserts a tenant.
func (s *Store) CreateTenant(ctx context.Context, t Tenant) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO tenants (id, pool_id, privacy_mode, guardrails) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (id) DO NOTHING`,
		t.ID, t.PoolID, t.PrivacyMode, t.Guardrails)
	return err
}

// GetTenant fetches a tenant by ID.
func (s *Store) GetTenant(ctx context.Context, id string) (*Tenant, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, pool_id, privacy_mode, guardrails, created_at FROM tenants WHERE id = $1`, id)
	var t Tenant
	if err := row.Scan(&t.ID, &t.PoolID, &t.PrivacyMode, &t.Guardrails, &t.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &t, nil
}

// --- FileVersion ---

// FileVersion is a file version row.
type FileVersion struct {
	ID             string
	FileID         string
	TenantID       string
	BlobKey        string
	BlobVersionID  string
	SizeBytes      int64
	ChecksumSHA256 string
	EncryptionMode string
	CommitState    string
	CachedAt       sql.NullTime
	DurableAt      sql.NullTime
	RetentionMode  string
	RetainUntil    sql.NullTime
	LegalHold      bool
	CreatedAt      time.Time
	DeletedAt      sql.NullTime
}

// CreateFileVersion inserts a file version.
func (s *Store) CreateFileVersion(ctx context.Context, v FileVersion) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO file_versions
		   (id, file_id, tenant_id, blob_key, blob_version_id, size_bytes,
		    checksum_sha256, encryption_mode, commit_state, cached_at,
		    retention_mode, retain_until, legal_hold)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		v.ID, v.FileID, v.TenantID, v.BlobKey, v.BlobVersionID, v.SizeBytes,
		v.ChecksumSHA256, v.EncryptionMode, v.CommitState, v.CachedAt,
		v.RetentionMode, v.RetainUntil, v.LegalHold)
	return err
}

// MarkFileVersionDurable flips a file version to COMMITTED_DURABLE.
func (s *Store) MarkFileVersionDurable(ctx context.Context, id, blobVersionID string, durableAt time.Time) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE file_versions
		    SET commit_state = 'COMMITTED_DURABLE',
		        blob_version_id = $2,
		        durable_at = $3
		  WHERE id = $1 AND commit_state = 'CACHED'`,
		id, blobVersionID, durableAt)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotCachable
	}
	return nil
}

// ListCachedVersions returns file versions still in CACHED state
// (pending promotion to Wasabi).
func (s *Store) ListCachedVersions(ctx context.Context, limit int) ([]FileVersion, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, file_id, tenant_id, blob_key, blob_version_id, size_bytes,
		        checksum_sha256, encryption_mode, commit_state, cached_at,
		        durable_at, retention_mode, retain_until, legal_hold,
		        created_at, deleted_at
		   FROM file_versions
		  WHERE commit_state = 'CACHED' AND deleted_at IS NULL
		  ORDER BY cached_at
		  LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FileVersion
	for rows.Next() {
		var v FileVersion
		if err := rows.Scan(&v.ID, &v.FileID, &v.TenantID, &v.BlobKey, &v.BlobVersionID,
			&v.SizeBytes, &v.ChecksumSHA256, &v.EncryptionMode, &v.CommitState, &v.CachedAt,
			&v.DurableAt, &v.RetentionMode, &v.RetainUntil, &v.LegalHold,
			&v.CreatedAt, &v.DeletedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// --- Outbox ---

// OutboxEvent is a row in the outbox table.
type OutboxEvent struct {
	ID          int64
	EventType   string
	Payload     []byte
	State       string
	Attempts    int
	MaxAttempts int
	LastError   string
	AvailableAt time.Time
	CreatedAt   time.Time
}

// EnqueueOutbox inserts a pending outbox event.
func (s *Store) EnqueueOutbox(ctx context.Context, eventType string, payload []byte) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO outbox (event_type, payload) VALUES ($1, $2)`,
		eventType, payload)
	return err
}

// ClaimOutboxEvents atomically claims up to limit pending events.
// Claimed events are marked IN_PROGRESS.
func (s *Store) ClaimOutboxEvents(ctx context.Context, limit int) ([]OutboxEvent, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx,
		`SELECT id, event_type, payload, state, attempts, max_attempts,
		        last_error, available_at, created_at
		   FROM outbox
		  WHERE state = 'PENDING' AND available_at <= now()
		  ORDER BY id
		  LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	var events []OutboxEvent
	ids := make([]int64, 0, limit)
	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(&e.ID, &e.EventType, &e.Payload, &e.State, &e.Attempts,
			&e.MaxAttempts, &e.LastError, &e.AvailableAt, &e.CreatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		events = append(events, e)
		ids = append(ids, e.ID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx,
			`UPDATE outbox SET state = 'IN_PROGRESS' WHERE id = $1`, id); err != nil {
			return nil, err
		}
	}
	return events, tx.Commit()
}

// CompleteOutboxEvent marks an event DONE.
func (s *Store) CompleteOutboxEvent(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE outbox SET state = 'DONE', completed_at = now() WHERE id = $1`, id)
	return err
}

// FailOutboxEvent increments attempts and either requeues (if under
// max) or marks FAILED.
func (s *Store) FailOutboxEvent(ctx context.Context, id int64, lastError string, backoff time.Duration) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE outbox
		    SET attempts = attempts + 1,
		        last_error = $2,
		        state = CASE WHEN attempts + 1 >= max_attempts THEN 'FAILED' ELSE 'PENDING' END,
		        available_at = now() + ($3 || ' seconds')::interval
		  WHERE id = $1`,
		id, lastError, fmt.Sprintf("%.0f", backoff.Seconds()))
	return err
}

// --- Erasure Ledger ---

// ErasureEntry is a row in the append-only erasure ledger.
type ErasureEntry struct {
	ID                int64
	EntityType        string
	EntityID          string
	TenantID          string
	ErasureType       string
	ErasureAt         time.Time
	ErasureGeneration int
}

// RecordErasure appends an entry to the erasure ledger. The
// erasure_generation is computed as the current max + 1 for the
// (entity_type, entity_id) pair so a restore can detect resurrected
// rows by comparing the row's generation against the ledger.
func (s *Store) RecordErasure(ctx context.Context, entityType, entityID, tenantID, erasureType string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO erasure_ledger (entity_type, entity_id, tenant_id, erasure_type, erasure_generation)
		 VALUES ($1, $2, $3, $4,
		     COALESCE(
		         (SELECT MAX(erasure_generation) + 1
		            FROM erasure_ledger
		           WHERE entity_type = $1 AND entity_id = $2), 1))`,
		entityType, entityID, tenantID, erasureType)
	return err
}

// LatestErasureGeneration returns the highest erasure_generation for
// the given entity, or 0 if no erasure has been recorded.
func (s *Store) LatestErasureGeneration(ctx context.Context, entityType, entityID string) (int, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(erasure_generation), 0)
		   FROM erasure_ledger
		  WHERE entity_type = $1 AND entity_id = $2`,
		entityType, entityID)
	var gen int
	if err := row.Scan(&gen); err != nil {
		return 0, err
	}
	return gen, nil
}

// --- Provider Write Intents ---

// WriteIntent is a row in provider_write_intents.
type WriteIntent struct {
	ID               int64
	BlobKey          string
	IdempotencyToken string
	ExpectedLength   int64
	ChecksumSHA256   string
	State            string
	Generation       int
	CreatedAt        time.Time
}

// CreateWriteIntent inserts a PROPOSED write intent. Returns
// ErrConflict if a row with the same (blob_key, idempotency_token)
// already exists.
func (s *Store) CreateWriteIntent(ctx context.Context, intent WriteIntent) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO provider_write_intents
		   (blob_key, idempotency_token, expected_length, checksum_sha256)
		 VALUES ($1, $2, $3, $4)`,
		intent.BlobKey, intent.IdempotencyToken, intent.ExpectedLength, intent.ChecksumSHA256)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return ErrConflict
		}
		return err
	}
	return nil
}

// AdoptWriteIntent flips a write intent to ADOPTED after the PUT is
// verified.
func (s *Store) AdoptWriteIntent(ctx context.Context, blobKey, idempotencyToken string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE provider_write_intents
		    SET state = 'ADOPTED', adopted_at = now()
		  WHERE blob_key = $1 AND idempotency_token = $2 AND state = 'PROPOSED'`,
		blobKey, idempotencyToken)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// AbandonWriteIntent flips a write intent to ABANDONED.
func (s *Store) AbandonWriteIntent(ctx context.Context, blobKey, idempotencyToken string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE provider_write_intents
		    SET state = 'ABANDONED', abandoned_at = now()
		  WHERE blob_key = $1 AND idempotency_token = $2`,
		blobKey, idempotencyToken)
	return err
}

// Normalized errors.
var (
	ErrNotFound    = errors.New("metadata: not found")
	ErrConflict    = errors.New("metadata: conflict")
	ErrNotCachable = errors.New("metadata: not in cacheable state")
)
