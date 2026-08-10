// Package metadata_test — integration tests for content_store.go and
// upload_session_store.go. Requires a live Postgres instance.
//
// Run with: go test -v ./internal/metadata/ -run TestContentStore -tags integration
//
// The tests auto-skip if Postgres is not reachable.
package metadata_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/kchat/drive/internal/metadata"
	_ "github.com/lib/pq"
)

// testStore opens a connection to the test Postgres, applies migrations,
// and returns a Store. Skips the test if Postgres is not available.
func testStore(t *testing.T) *metadata.Store {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		dsn = "host=localhost port=5432 user=postgres password=postgres dbname=kdrive_test sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("Postgres not available: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Skipf("Postgres not reachable: %v", err)
	}
	// Clean and re-create schema
	_, _ = db.ExecContext(ctx, `DROP SCHEMA IF EXISTS public CASCADE`)
	_, _ = db.ExecContext(ctx, `CREATE SCHEMA public`)
	store := metadata.New(db)
	if err := store.Migrate(ctx, metadata.EmbeddedMigrations()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// Create a test tenant
	_, err = db.ExecContext(ctx,
		`INSERT INTO tenants (id, pool_id) VALUES ('test-tenant', 'test-pool')
		 ON CONFLICT (id) DO NOTHING`)
	if err != nil {
		t.Fatalf("create test tenant: %v", err)
	}
	// Create a second tenant for isolation tests
	_, err = db.ExecContext(ctx,
		`INSERT INTO tenants (id, pool_id) VALUES ('other-tenant', 'test-pool')
		 ON CONFLICT (id) DO NOTHING`)
	if err != nil {
		t.Fatalf("create other tenant: %v", err)
	}
	return store
}

func TestContentStore_CreateAndGetEntry(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	entry := metadata.ContentEntry{
		ContentID:        "content-001",
		TenantID:         "test-tenant",
		PlaintextSize:    1024,
		ChunkCount:       4,
		ChunkSize:        256,
		ChunkPlanRoot:    "abc123root",
		PepperGeneration: 1,
	}

	// Create
	if err := store.CreateContentEntry(ctx, entry); err != nil {
		t.Fatalf("CreateContentEntry: %v", err)
	}

	// Get — should succeed for same tenant
	got, err := store.GetContentEntry(ctx, "content-001", "test-tenant")
	if err != nil {
		t.Fatalf("GetContentEntry: %v", err)
	}
	if got.ContentID != "content-001" {
		t.Errorf("ContentID = %s, want content-001", got.ContentID)
	}
	if got.TenantID != "test-tenant" {
		t.Errorf("TenantID = %s, want test-tenant", got.TenantID)
	}
	if got.PlaintextSize != 1024 {
		t.Errorf("PlaintextSize = %d, want 1024", got.PlaintextSize)
	}

	// Get — should fail for different tenant
	_, err = store.GetContentEntry(ctx, "content-001", "other-tenant")
	if err != metadata.ErrNotFound {
		t.Errorf("GetContentEntry for other tenant: err = %v, want ErrNotFound", err)
	}

	// Get by content ID (any tenant)
	anyTenant, err := store.GetContentEntryByContentID(ctx, "content-001")
	if err != nil {
		t.Fatalf("GetContentEntryByContentID: %v", err)
	}
	if anyTenant.TenantID != "test-tenant" {
		t.Errorf("TenantID = %s, want test-tenant", anyTenant.TenantID)
	}
}

func TestContentStore_CreateEntryIdempotent(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	entry := metadata.ContentEntry{
		ContentID:        "content-idem",
		TenantID:         "test-tenant",
		PlaintextSize:    512,
		ChunkCount:       2,
		ChunkSize:        256,
		ChunkPlanRoot:    "root-idem",
		PepperGeneration: 1,
	}

	// First insert
	if err := store.CreateContentEntry(ctx, entry); err != nil {
		t.Fatalf("first CreateContentEntry: %v", err)
	}
	// Second insert — should be idempotent (no error, no change)
	if err := store.CreateContentEntry(ctx, entry); err != nil {
		t.Fatalf("second CreateContentEntry: %v", err)
	}

	got, err := store.GetContentEntry(ctx, "content-idem", "test-tenant")
	if err != nil {
		t.Fatalf("GetContentEntry after idempotent insert: %v", err)
	}
	if got.PlaintextSize != 512 {
		t.Errorf("PlaintextSize after idempotent insert = %d, want 512", got.PlaintextSize)
	}
}

func TestContentStore_CreateAndGetChunks(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	// Create content entry first
	entry := metadata.ContentEntry{
		ContentID:        "content-chunks",
		TenantID:         "test-tenant",
		PlaintextSize:    1024,
		ChunkCount:       3,
		ChunkSize:        341,
		ChunkPlanRoot:    "root-chunks",
		PepperGeneration: 1,
	}
	if err := store.CreateContentEntry(ctx, entry); err != nil {
		t.Fatalf("CreateContentEntry: %v", err)
	}

	// Create chunks
	chunks := []metadata.ContentChunk{
		{ChunkIndex: 0, ChunkContentHash: "hash-0", BlobKey: "blob_0", PlaintextLen: 341, CiphertextLen: 357},
		{ChunkIndex: 1, ChunkContentHash: "hash-1", BlobKey: "blob_1", PlaintextLen: 341, CiphertextLen: 357},
		{ChunkIndex: 2, ChunkContentHash: "hash-2", BlobKey: "blob_2", PlaintextLen: 342, CiphertextLen: 358},
	}
	if err := store.CreateContentChunks(ctx, "content-chunks", chunks); err != nil {
		t.Fatalf("CreateContentChunks: %v", err)
	}

	// Get chunks
	got, err := store.GetContentChunks(ctx, "content-chunks", "test-tenant")
	if err != nil {
		t.Fatalf("GetContentChunks: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len(chunks) = %d, want 3", len(got))
	}
	if got[0].ChunkContentHash != "hash-0" || got[2].ChunkContentHash != "hash-2" {
		t.Errorf("chunks out of order: %s, %s", got[0].ChunkContentHash, got[2].ChunkContentHash)
	}

	// Get chunks for wrong tenant — should return empty (tenant-scoped JOIN)
	gotOther, err := store.GetContentChunks(ctx, "content-chunks", "other-tenant")
	if err != nil {
		t.Errorf("GetContentChunks for other tenant: err = %v, want nil", err)
	}
	if len(gotOther) != 0 {
		t.Errorf("GetContentChunks for other tenant: len = %d, want 0 (tenant isolation)", len(gotOther))
	}
}

func TestContentStore_CreateChunksIdempotent(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	entry := metadata.ContentEntry{
		ContentID:        "content-idem-chunks",
		TenantID:         "test-tenant",
		PlaintextSize:    256,
		ChunkCount:       1,
		ChunkSize:        256,
		ChunkPlanRoot:    "root-idem-chunks",
		PepperGeneration: 1,
	}
	if err := store.CreateContentEntry(ctx, entry); err != nil {
		t.Fatalf("CreateContentEntry: %v", err)
	}

	chunks := []metadata.ContentChunk{
		{ChunkIndex: 0, ChunkContentHash: "hash-x", BlobKey: "blob_x", PlaintextLen: 256, CiphertextLen: 272},
	}
	// First insert
	if err := store.CreateContentChunks(ctx, "content-idem-chunks", chunks); err != nil {
		t.Fatalf("first CreateContentChunks: %v", err)
	}
	// Second insert — idempotent
	if err := store.CreateContentChunks(ctx, "content-idem-chunks", chunks); err != nil {
		t.Fatalf("second CreateContentChunks: %v", err)
	}
	got, err := store.GetContentChunks(ctx, "content-idem-chunks", "test-tenant")
	if err != nil {
		t.Fatalf("GetContentChunks after idempotent insert: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("len(chunks) after idempotent insert = %d, want 1", len(got))
	}
}

func TestContentStore_CreateChunksBulk(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	entry := metadata.ContentEntry{
		ContentID:        "content-bulk",
		TenantID:         "test-tenant",
		PlaintextSize:    1200 * 256,
		ChunkCount:       1200,
		ChunkSize:        256,
		ChunkPlanRoot:    "root-bulk",
		PepperGeneration: 1,
	}
	if err := store.CreateContentEntry(ctx, entry); err != nil {
		t.Fatalf("CreateContentEntry: %v", err)
	}

	// Create 1200 chunks — tests batched INSERT (batchSize=500)
	chunks := make([]metadata.ContentChunk, 1200)
	for i := range chunks {
		chunks[i] = metadata.ContentChunk{
			ChunkIndex:       i,
			ChunkContentHash: fmt.Sprintf("bulk-hash-%d", i),
			BlobKey:          fmt.Sprintf("blob_bulk_%d", i),
			PlaintextLen:     256,
			CiphertextLen:    272,
		}
	}
	if err := store.CreateContentChunks(ctx, "content-bulk", chunks); err != nil {
		t.Fatalf("CreateContentChunks bulk: %v", err)
	}

	got, err := store.GetContentChunks(ctx, "content-bulk", "test-tenant")
	if err != nil {
		t.Fatalf("GetContentChunks: %v", err)
	}
	if len(got) != 1200 {
		t.Errorf("len(chunks) = %d, want 1200", len(got))
	}
	// Verify ordering
	if got[0].ChunkIndex != 0 || got[1199].ChunkIndex != 1199 {
		t.Errorf("chunk ordering wrong: first=%d, last=%d", got[0].ChunkIndex, got[1199].ChunkIndex)
	}
}

func TestContentStore_CheckChunkHashes(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	entry := metadata.ContentEntry{
		ContentID:        "content-check",
		TenantID:         "test-tenant",
		PlaintextSize:    768,
		ChunkCount:       3,
		ChunkSize:        256,
		ChunkPlanRoot:    "root-check",
		PepperGeneration: 1,
	}
	if err := store.CreateContentEntry(ctx, entry); err != nil {
		t.Fatalf("CreateContentEntry: %v", err)
	}

	chunks := []metadata.ContentChunk{
		{ChunkIndex: 0, ChunkContentHash: "hash-a", BlobKey: "blob_a", PlaintextLen: 256, CiphertextLen: 272},
		{ChunkIndex: 1, ChunkContentHash: "hash-b", BlobKey: "blob_b", PlaintextLen: 256, CiphertextLen: 272},
		{ChunkIndex: 2, ChunkContentHash: "hash-c", BlobKey: "blob_c", PlaintextLen: 256, CiphertextLen: 272},
	}
	if err := store.CreateContentChunks(ctx, "content-check", chunks); err != nil {
		t.Fatalf("CreateContentChunks: %v", err)
	}

	// Check mixed existing/non-existing hashes
	results, err := store.CheckChunkHashes(ctx, "content-check", "test-tenant",
		[]string{"hash-a", "hash-b", "hash-zzz"})
	if err != nil {
		t.Fatalf("CheckChunkHashes: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(results))
	}
	if !results[0].Exists || results[0].BlobKey != "blob_a" {
		t.Errorf("results[0]: exists=%v, blobKey=%s, want true/blob_a", results[0].Exists, results[0].BlobKey)
	}
	if !results[1].Exists || results[1].BlobKey != "blob_b" {
		t.Errorf("results[1]: exists=%v, blobKey=%s, want true/blob_b", results[1].Exists, results[1].BlobKey)
	}
	if results[2].Exists {
		t.Errorf("results[2]: exists=true, want false (hash-zzz doesn't exist)")
	}

	// Check for wrong tenant — all should be false
	results, err = store.CheckChunkHashes(ctx, "content-check", "other-tenant",
		[]string{"hash-a"})
	if err != nil {
		t.Fatalf("CheckChunkHashes for other tenant: %v", err)
	}
	if len(results) != 1 || results[0].Exists {
		t.Errorf("CheckChunkHashes for other tenant: should return all false")
	}
}

func TestContentStore_TenantIsolation(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	// Tenant A creates content
	entryA := metadata.ContentEntry{
		ContentID:        "content-iso-a",
		TenantID:         "test-tenant",
		PlaintextSize:    100,
		ChunkCount:       1,
		ChunkSize:        100,
		ChunkPlanRoot:    "root-a",
		PepperGeneration: 1,
	}
	if err := store.CreateContentEntry(ctx, entryA); err != nil {
		t.Fatalf("CreateContentEntry: %v", err)
	}

	// Tenant B tries to get tenant A's content — should get ErrNotFound
	_, err := store.GetContentEntry(ctx, "content-iso-a", "other-tenant")
	if err != metadata.ErrNotFound {
		t.Errorf("cross-tenant GetContentEntry: err = %v, want ErrNotFound", err)
	}

	// Tenant B tries to check chunks — should return all false
	results, err := store.CheckChunkHashes(ctx, "content-iso-a", "other-tenant",
		[]string{"any-hash"})
	if err != nil {
		t.Fatalf("CheckChunkHashes for other tenant: %v", err)
	}
	if len(results) != 1 || results[0].Exists {
		t.Errorf("cross-tenant CheckChunkHashes should return all false")
	}
}

// --- Upload Session Store Tests ---

func TestUploadSessionStore_CreateAndGet(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	chunkPlan := json.RawMessage(`{"chunks":[]}`)
	manifest := json.RawMessage(`{"version_id":"v1"}`)
	header := json.RawMessage(`{"h":"1"}`)

	sess := metadata.KDRV1UploadSession{
		ID:         "sess-001",
		TenantID:   "test-tenant",
		NodeID:     "node-001",
		FolderID:   "folder-001",
		ChunkPlan:  chunkPlan,
		Manifest:   manifest,
		Header:     header,
		WrappedDEK: "wrapped-dek-hex",
		WrapNonce:  "wrap-nonce-hex",
		Chunks:     json.RawMessage(`[]`),
		State:      "open",
	}

	if err := store.CreateKDRV1UploadSession(ctx, sess); err != nil {
		t.Fatalf("CreateKDRV1UploadSession: %v", err)
	}

	// Get for same tenant — should succeed
	got, err := store.GetKDRV1UploadSession(ctx, "sess-001", "test-tenant")
	if err != nil {
		t.Fatalf("GetKDRV1UploadSession: %v", err)
	}
	if got.ID != "sess-001" {
		t.Errorf("ID = %s, want sess-001", got.ID)
	}
	if got.State != "open" {
		t.Errorf("State = %s, want open", got.State)
	}

	// Get for wrong tenant — should get ErrNotFound
	_, err = store.GetKDRV1UploadSession(ctx, "sess-001", "other-tenant")
	if err != metadata.ErrNotFound {
		t.Errorf("GetKDRV1UploadSession for wrong tenant: err = %v, want ErrNotFound", err)
	}
}

func TestUploadSessionStore_AppendChunk(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	sess := metadata.KDRV1UploadSession{
		ID:         "sess-append",
		TenantID:   "test-tenant",
		NodeID:     "node-append",
		FolderID:   "folder-append",
		ChunkPlan:  json.RawMessage(`{"chunks":[]}`),
		Manifest:   json.RawMessage(`{}`),
		Header:     json.RawMessage(`{}`),
		WrappedDEK: "dek",
		WrapNonce:  "nonce",
		Chunks:     json.RawMessage(`[]`),
		State:      "open",
	}
	if err := store.CreateKDRV1UploadSession(ctx, sess); err != nil {
		t.Fatalf("CreateKDRV1UploadSession: %v", err)
	}

	// Append chunk 0
	chunk0 := json.RawMessage(`{"index":0,"blob_key":"blob_0"}`)
	if err := store.AppendKDRV1UploadSessionChunk(ctx, "sess-append", "test-tenant", chunk0); err != nil {
		t.Fatalf("AppendKDRV1UploadSessionChunk 0: %v", err)
	}

	// Append chunk 1
	chunk1 := json.RawMessage(`{"index":1,"blob_key":"blob_1"}`)
	if err := store.AppendKDRV1UploadSessionChunk(ctx, "sess-append", "test-tenant", chunk1); err != nil {
		t.Fatalf("AppendKDRV1UploadSessionChunk 1: %v", err)
	}

	// Verify both chunks are in the session
	got, err := store.GetKDRV1UploadSession(ctx, "sess-append", "test-tenant")
	if err != nil {
		t.Fatalf("GetKDRV1UploadSession: %v", err)
	}
	var chunks []json.RawMessage
	if err := json.Unmarshal(got.Chunks, &chunks); err != nil {
		t.Fatalf("unmarshal chunks: %v", err)
	}
	if len(chunks) != 2 {
		t.Errorf("len(chunks) = %d, want 2", len(chunks))
	}

	// Wrong tenant — should get ErrNotFound
	chunk2 := json.RawMessage(`{"index":2}`)
	err = store.AppendKDRV1UploadSessionChunk(ctx, "sess-append", "other-tenant", chunk2)
	if err != metadata.ErrNotFound {
		t.Errorf("AppendKDRV1UploadSessionChunk for wrong tenant: err = %v, want ErrNotFound", err)
	}
}

func TestUploadSessionStore_UpdateStateAndDelete(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	sess := metadata.KDRV1UploadSession{
		ID:         "sess-state",
		TenantID:   "test-tenant",
		NodeID:     "node-state",
		FolderID:   "folder-state",
		ChunkPlan:  json.RawMessage(`{}`),
		Manifest:   json.RawMessage(`{}`),
		Header:     json.RawMessage(`{}`),
		WrappedDEK: "dek",
		WrapNonce:  "nonce",
		Chunks:     json.RawMessage(`[]`),
		State:      "open",
	}
	if err := store.CreateKDRV1UploadSession(ctx, sess); err != nil {
		t.Fatalf("CreateKDRV1UploadSession: %v", err)
	}

	// Update state
	if err := store.UpdateKDRV1UploadSessionState(ctx, "sess-state", "test-tenant", "committed"); err != nil {
		t.Fatalf("UpdateKDRV1UploadSessionState: %v", err)
	}
	got, err := store.GetKDRV1UploadSession(ctx, "sess-state", "test-tenant")
	if err != nil {
		t.Fatalf("GetKDRV1UploadSession after state update: %v", err)
	}
	if got.State != "committed" {
		t.Errorf("State = %s, want committed", got.State)
	}

	// Wrong tenant update — should not change state (affects 0 rows, no error)
	err = store.UpdateKDRV1UploadSessionState(ctx, "sess-state", "other-tenant", "aborted")
	if err != nil {
		t.Errorf("UpdateKDRV1UploadSessionState for wrong tenant: err = %v, want nil", err)
	}
	got, err = store.GetKDRV1UploadSession(ctx, "sess-state", "test-tenant")
	if err != nil {
		t.Fatalf("GetKDRV1UploadSession after wrong-tenant update: %v", err)
	}
	if got.State != "committed" {
		t.Errorf("State after wrong-tenant update = %s, want committed (unchanged)", got.State)
	}

	// Delete
	if err := store.DeleteKDRV1UploadSession(ctx, "sess-state", "test-tenant"); err != nil {
		t.Fatalf("DeleteKDRV1UploadSession: %v", err)
	}
	_, err = store.GetKDRV1UploadSession(ctx, "sess-state", "test-tenant")
	if err != metadata.ErrNotFound {
		t.Errorf("GetKDRV1UploadSession after delete: err = %v, want ErrNotFound", err)
	}
}
