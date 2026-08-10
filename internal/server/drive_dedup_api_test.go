package server

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// newTestGateway builds a minimal Gateway for unit tests (no Postgres, no blob store).
func newTestGateway(t *testing.T) *Gateway {
	t.Helper()
	return &Gateway{
		logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
	}
}

func TestContentCheck_NotFound(t *testing.T) {
	gw := newTestGateway(t)
	d := newDriveAPI(gw)

	body := `{"content_id":"nonexistent"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/content:check", bytes.NewBufferString(body))
	req.Header.Set("X-Demo-Tenant", "test-tenant")
	w := httptest.NewRecorder()

	d.handleContentCheck(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)

	if resp["exists"] != false {
		t.Fatalf("expected exists=false, got %v", resp["exists"])
	}
}

func TestContentRegisterAndCheck(t *testing.T) {
	gw := newTestGateway(t)
	d := newDriveAPI(gw)

	// Register content
	regBody := `{
		"content_id": "test-content-1",
		"plaintext_size": 1024,
		"chunk_count": 1,
		"chunk_size": 4194304,
		"chunk_plan_root": "abc123",
		"chunks": [
			{
				"chunk_index": 0,
				"chunk_content_hash": "deadbeef",
				"blob_key": "blob_test-content-1_deadbeef",
				"plaintext_len": 1024,
				"ciphertext_len": 1040
			}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/v1/content:register", bytes.NewBufferString(regBody))
	req.Header.Set("X-Demo-Tenant", "test-tenant")
	w := httptest.NewRecorder()
	d.handleContentRegister(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("register: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Check content — should exist
	checkBody := `{"content_id":"test-content-1"}`
	req2 := httptest.NewRequest(http.MethodPost, "/v1/content:check", bytes.NewBufferString(checkBody))
	req2.Header.Set("X-Demo-Tenant", "test-tenant")
	w2 := httptest.NewRecorder()
	d.handleContentCheck(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("check: expected 200, got %d", w2.Code)
	}

	var resp map[string]any
	json.NewDecoder(w2.Body).Decode(&resp)

	if resp["exists"] != true {
		t.Fatalf("expected exists=true, got %v", resp["exists"])
	}
	if resp["chunk_count"] != float64(1) {
		t.Fatalf("expected chunk_count=1, got %v", resp["chunk_count"])
	}
}

func TestContentCheck_TenantIsolation(t *testing.T) {
	gw := newTestGateway(t)
	d := newDriveAPI(gw)

	// Register content for tenant A
	regBody := `{
		"content_id": "tenant-a-content",
		"plaintext_size": 100,
		"chunk_count": 1,
		"chunk_size": 4194304,
		"chunk_plan_root": "root",
		"chunks": []
	}`
	req := httptest.NewRequest(http.MethodPost, "/v1/content:register", bytes.NewBufferString(regBody))
	req.Header.Set("X-Demo-Tenant", "tenant-a")
	w := httptest.NewRecorder()
	d.handleContentRegister(w, req)

	// Tenant B should not see tenant A's content
	checkBody := `{"content_id":"tenant-a-content"}`
	req2 := httptest.NewRequest(http.MethodPost, "/v1/content:check", bytes.NewBufferString(checkBody))
	req2.Header.Set("X-Demo-Tenant", "tenant-b")
	w2 := httptest.NewRecorder()
	d.handleContentCheck(w2, req2)

	var resp map[string]any
	json.NewDecoder(w2.Body).Decode(&resp)

	if resp["exists"] != false {
		t.Fatalf("tenant B should not see tenant A's content: exists=%v", resp["exists"])
	}
}

func TestContentCheckChunks(t *testing.T) {
	gw := newTestGateway(t)
	d := newDriveAPI(gw)

	// Register content with 2 chunks
	regBody := `{
		"content_id": "chunk-test",
		"plaintext_size": 2048,
		"chunk_count": 2,
		"chunk_size": 4194304,
		"chunk_plan_root": "root",
		"chunks": [
			{"chunk_index": 0, "chunk_content_hash": "hash0", "blob_key": "blob_0", "plaintext_len": 1024, "ciphertext_len": 1040},
			{"chunk_index": 1, "chunk_content_hash": "hash1", "blob_key": "blob_1", "plaintext_len": 1024, "ciphertext_len": 1040}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/v1/content:register", bytes.NewBufferString(regBody))
	req.Header.Set("X-Demo-Tenant", "test-tenant")
	w := httptest.NewRecorder()
	d.handleContentRegister(w, req)

	// Check chunks — hash0 exists, hash2 does not
	checkBody := `{"content_id":"chunk-test","chunk_hashes":["hash0","hash2"]}`
	req2 := httptest.NewRequest(http.MethodPost, "/v1/content:checkChunks", bytes.NewBufferString(checkBody))
	req2.Header.Set("X-Demo-Tenant", "test-tenant")
	w2 := httptest.NewRecorder()
	d.handleContentCheckChunks(w2, req2)

	var resp map[string]any
	json.NewDecoder(w2.Body).Decode(&resp)

	results := resp["results"].([]any)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	r0 := results[0].(map[string]any)
	if r0["exists"] != true {
		t.Fatalf("hash0 should exist: %v", r0["exists"])
	}
	if r0["blob_key"] != "blob_0" {
		t.Fatalf("expected blob_0, got %v", r0["blob_key"])
	}

	r1 := results[1].(map[string]any)
	if r1["exists"] != false {
		t.Fatalf("hash2 should not exist: %v", r1["exists"])
	}
}

func TestUploadCommitDedup(t *testing.T) {
	gw := newTestGateway(t)
	d := newDriveAPI(gw)

	// Register content first
	regBody := `{
		"content_id": "commit-test",
		"plaintext_size": 100,
		"chunk_count": 1,
		"chunk_size": 4194304,
		"chunk_plan_root": "root",
		"chunks": [
			{"chunk_index": 0, "chunk_content_hash": "chash", "blob_key": "blob_commit-test_chash", "plaintext_len": 100, "ciphertext_len": 116}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/v1/content:register", bytes.NewBufferString(regBody))
	req.Header.Set("X-Demo-Tenant", "test-tenant")
	w := httptest.NewRecorder()
	d.handleContentRegister(w, req)

	// Commit dedup version
	commitBody := `{
		"protocol": "KDRV1",
		"content_id": "commit-test",
		"node_id": "node-1",
		"manifest": "{}",
		"manifest_nonce_hex": "aabbccdd",
		"manifest_ciphertext_sha256": "mhash",
		"header": "{}",
		"wrapped_dek_hex": "deadbeef",
		"wrap_nonce_hex": "11223344",
		"wrapped_content_key_hex": "cafebabe",
		"content_wrap_nonce_hex": "55667788",
		"reused_blob_keys": ["blob_commit-test_chash"],
		"new_blob_keys": []
	}`
	req2 := httptest.NewRequest(http.MethodPost, "/v1/uploads:commitDedup", bytes.NewBufferString(commitBody))
	req2.Header.Set("X-Demo-Tenant", "test-tenant")
	w2 := httptest.NewRecorder()
	d.handleUploadCommitDedup(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w2.Code, w2.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w2.Body).Decode(&resp)

	if resp["committed"] != true {
		t.Fatalf("expected committed=true, got %v", resp["committed"])
	}
	if resp["deduped_chunks"] != float64(1) {
		t.Fatalf("expected deduped_chunks=1, got %v", resp["deduped_chunks"])
	}
	if resp["new_chunks"] != float64(0) {
		t.Fatalf("expected new_chunks=0, got %v", resp["new_chunks"])
	}
}

func TestUploadCommitDedup_ReusedBlobNotFound(t *testing.T) {
	gw := newTestGateway(t)
	d := newDriveAPI(gw)

	// Register content
	regBody := `{
		"content_id": "fail-test",
		"plaintext_size": 100,
		"chunk_count": 1,
		"chunk_size": 4194304,
		"chunk_plan_root": "root",
		"chunks": [
			{"chunk_index": 0, "chunk_content_hash": "chash", "blob_key": "blob_real", "plaintext_len": 100, "ciphertext_len": 116}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/v1/content:register", bytes.NewBufferString(regBody))
	req.Header.Set("X-Demo-Tenant", "test-tenant")
	w := httptest.NewRecorder()
	d.handleContentRegister(w, req)

	// Try to commit with a non-existent reused blob key
	commitBody := `{
		"protocol": "KDRV1",
		"content_id": "fail-test",
		"reused_blob_keys": ["blob_nonexistent"],
		"new_blob_keys": []
	}`
	req2 := httptest.NewRequest(http.MethodPost, "/v1/uploads:commitDedup", bytes.NewBufferString(commitBody))
	req2.Header.Set("X-Demo-Tenant", "test-tenant")
	w2 := httptest.NewRecorder()
	d.handleUploadCommitDedup(w2, req2)

	if w2.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-existent blob, got %d", w2.Code)
	}
}

func TestContentRegister_Idempotent(t *testing.T) {
	gw := newTestGateway(t)
	d := newDriveAPI(gw)

	regBody := `{
		"content_id": "idempotent-content-1",
		"plaintext_size": 1024,
		"chunk_count": 1,
		"chunk_size": 4194304,
		"chunk_plan_root": "abc123",
		"chunks": [
			{
				"chunk_index": 0,
				"chunk_content_hash": "deadbeef",
				"blob_key": "blob_idempotent-content-1_deadbeef",
				"plaintext_len": 1024,
				"ciphertext_len": 1040
			}
		]
	}`

	// First registration
	req := httptest.NewRequest(http.MethodPost, "/v1/content:register", bytes.NewBufferString(regBody))
	req.Header.Set("X-Demo-Tenant", "test-tenant")
	w := httptest.NewRecorder()
	d.handleContentRegister(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("first register: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp1 map[string]any
	json.NewDecoder(w.Body).Decode(&resp1)
	if resp1["already_existed"] != false {
		t.Fatalf("first register: expected already_existed=false, got %v", resp1["already_existed"])
	}

	// Second registration (same tenant, same content_id) — should be idempotent
	req2 := httptest.NewRequest(http.MethodPost, "/v1/content:register", bytes.NewBufferString(regBody))
	req2.Header.Set("X-Demo-Tenant", "test-tenant")
	w2 := httptest.NewRecorder()
	d.handleContentRegister(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("second register: expected 200, got %d: %s", w2.Code, w2.Body.String())
	}
	var resp2 map[string]any
	json.NewDecoder(w2.Body).Decode(&resp2)
	if resp2["already_existed"] != true {
		t.Fatalf("second register: expected already_existed=true, got %v", resp2["already_existed"])
	}
}

func TestContentRegister_CrossTenantForbidden(t *testing.T) {
	gw := newTestGateway(t)
	d := newDriveAPI(gw)

	regBody := `{
		"content_id": "cross-tenant-content",
		"plaintext_size": 1024,
		"chunk_count": 1,
		"chunk_size": 4194304,
		"chunk_plan_root": "abc123",
		"chunks": [
			{
				"chunk_index": 0,
				"chunk_content_hash": "deadbeef",
				"blob_key": "blob_cross-tenant-content_deadbeef",
				"plaintext_len": 1024,
				"ciphertext_len": 1040
			}
		]
	}`

	// Register with tenant A
	req := httptest.NewRequest(http.MethodPost, "/v1/content:register", bytes.NewBufferString(regBody))
	req.Header.Set("X-Demo-Tenant", "tenant-a")
	w := httptest.NewRecorder()
	d.handleContentRegister(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("register tenant-a: expected 200, got %d", w.Code)
	}

	// Try to re-register with tenant B — should be forbidden
	req2 := httptest.NewRequest(http.MethodPost, "/v1/content:register", bytes.NewBufferString(regBody))
	req2.Header.Set("X-Demo-Tenant", "tenant-b")
	w2 := httptest.NewRecorder()
	d.handleContentRegister(w2, req2)
	if w2.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant register: expected 403, got %d: %s", w2.Code, w2.Body.String())
	}
}
