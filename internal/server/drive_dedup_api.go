// Package server — KDRV1 content deduplication API handlers.
//
// These endpoints enable intra-tenant content dedup:
//
//	POST /v1/content:check       — file-level dedup check
//	POST /v1/content:checkChunks  — chunk-level dedup check
//	POST /v1/content:register     — register new content entry
//	POST /v1/uploads:commitDedup  — commit KDRV1 version with dedup info
//
// The gateway stores only ciphertext + opaque content_ids. It cannot
// compute content_ids (requires tenant_pepper) or decrypt content.
//
// When Postgres is available (g.metaDB != nil), content entries and
// chunks are persisted to the content_entries / content_chunks tables.
// In dev mode without Postgres, an in-memory store is used as fallback.
package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/kchat/drive/internal/metadata"
	"github.com/kchat/drive/pkg/blobstore"
)

// hexDecodeString is a thin wrapper around hex.DecodeString for use in handlers.
func hexDecodeString(s string) ([]byte, error) {
	return hex.DecodeString(s)
}

// --- In-memory fallback (dev mode without Postgres) ---

type memContentEntry struct {
	ContentID        string
	TenantID         string
	PlaintextSize    int64
	ChunkCount       int64
	ChunkSize        int64
	ChunkPlanRoot    string
	BlobKeys         []string
	CiphertextHashes []string
	CreatedAt        time.Time
}

type memContentChunk struct {
	ChunkIndex       int
	ChunkContentHash string
	BlobKey          string
	PlaintextLen     int64
	CiphertextLen    int64
}

// putMemContent inserts a content entry and its chunks into the in-memory
// stores, evicting a random entry if the store has reached its size limit.
// Caller must NOT hold memContentMu.
//
// NOTE: Eviction is currently random (map iteration order is randomized
// in Go). A proper LRU would track access order, but that adds
// complexity for a dev-mode fallback. Left as a known limitation.
func (d *driveAPI) putMemContent(key string, entry *memContentEntry, chunks []memContentChunk) {
	d.memContentMu.Lock()
	defer d.memContentMu.Unlock()
	if len(d.memContentStore) >= d.memContentStoreMax {
		// Evict a random entry (map iteration order is randomized in Go).
		for k := range d.memContentStore {
			delete(d.memContentStore, k)
			delete(d.memContentChunks, k)
			break
		}
	}
	// The chunks map can grow larger than the entry store because
	// eviction above only trims to memContentStoreMax. Add a hard cap
	// at 10x the store max (100000) and evict random entries to keep
	// memory bounded in dev mode.
	if d.memContentStoreMax > 0 && len(d.memContentChunks) > d.memContentStoreMax*10 {
		for k := range d.memContentChunks {
			delete(d.memContentChunks, k)
			delete(d.memContentStore, k)
			break
		}
	}
	d.memContentStore[key] = entry
	d.memContentChunks[key] = chunks
}

// getMemContent retrieves a content entry from the in-memory store.
// Caller must NOT hold memContentMu. The returned entry is a snapshot;
// callers should not mutate it.
func (d *driveAPI) getMemContent(key string) (*memContentEntry, bool) {
	d.memContentMu.RLock()
	defer d.memContentMu.RUnlock()
	entry, ok := d.memContentStore[key]
	return entry, ok
}

// getMemContentChunks retrieves the chunks for a content entry from the
// in-memory store. Caller must NOT hold memContentMu.
func (d *driveAPI) getMemContentChunks(key string) ([]memContentChunk, bool) {
	d.memContentMu.RLock()
	defer d.memContentMu.RUnlock()
	chunks, ok := d.memContentChunks[key]
	return chunks, ok
}

// contentChunkEntry is the JSON shape for chunk registration requests.
type contentChunkEntry struct {
	ChunkIndex       int    `json:"chunk_index"`
	ChunkContentHash string `json:"chunk_content_hash"`
	BlobKey          string `json:"blob_key"`
	PlaintextLen     int64  `json:"plaintext_len"`
	CiphertextLen    int64  `json:"ciphertext_len"`
}

// --- Handlers ---

func (d *driveAPI) handleContentCheck(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	_, tenantID := getUserTenant(r)
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Demo-Tenant header")
		return
	}

	var body struct {
		ContentID string `json:"content_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}

	// Postgres path
	if d.gw.metaDB != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		entry, err := d.gw.metaDB.GetContentEntry(ctx, body.ContentID, tenantID)
		if err != nil {
			if errors.Is(err, metadata.ErrNotFound) {
				writeJSON(w, http.StatusOK, map[string]any{"exists": false})
				return
			}
			d.gw.logger.Error("drive: content check", slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		chunks, err := d.gw.metaDB.GetContentChunks(ctx, body.ContentID, tenantID)
		if err != nil {
			d.gw.logger.Error("drive: content chunks", slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		blobKeys := make([]string, 0, len(chunks))
		ciphertextHashes := make([]string, 0, len(chunks))
		for _, c := range chunks {
			blobKeys = append(blobKeys, c.BlobKey)
			ciphertextHashes = append(ciphertextHashes, c.ChunkContentHash)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"exists":            true,
			"blob_keys":         blobKeys,
			"ciphertext_hashes": ciphertextHashes,
			"chunk_count":       entry.ChunkCount,
			"plaintext_size":    entry.PlaintextSize,
		})
		return
	}

	// In-memory fallback (dev mode)
	entry, ok := d.getMemContent(body.ContentID)

	if !ok || entry.TenantID != tenantID {
		writeJSON(w, http.StatusOK, map[string]any{"exists": false})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"exists":            true,
		"blob_keys":         entry.BlobKeys,
		"ciphertext_hashes": entry.CiphertextHashes,
		"chunk_count":       entry.ChunkCount,
		"plaintext_size":    entry.PlaintextSize,
	})
}

func (d *driveAPI) handleContentCheckChunks(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	_, tenantID := getUserTenant(r)
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Demo-Tenant header")
		return
	}

	var body struct {
		ContentID   string   `json:"content_id"`
		ChunkHashes []string `json:"chunk_hashes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}

	// Postgres path
	if d.gw.metaDB != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		results, err := d.gw.metaDB.CheckChunkHashes(ctx, body.ContentID, tenantID, body.ChunkHashes)
		if err != nil {
			d.gw.logger.Error("drive: check chunks", slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		out := make([]map[string]any, 0, len(results))
		for _, r := range results {
			entry := map[string]any{
				"hash":   r.Hash,
				"exists": r.Exists,
			}
			if r.Exists {
				entry["blob_key"] = r.BlobKey
			} else {
				entry["blob_key"] = nil
			}
			out = append(out, entry)
		}
		writeJSON(w, http.StatusOK, map[string]any{"results": out})
		return
	}

	// In-memory fallback (dev mode)
	// Chunk-level dedup searches across ALL content chunks for this tenant,
	// not just the given content_id. This allows a new file version to reuse
	// chunks from a different content_id.
	knownHashes := map[string]string{} // hash → blob_key
	d.memContentMu.RLock()
	for cid, entry := range d.memContentStore {
		if entry.TenantID != tenantID {
			continue
		}
		for _, c := range d.memContentChunks[cid] {
			knownHashes[c.ChunkContentHash] = c.BlobKey
		}
	}
	d.memContentMu.RUnlock()

	results := make([]map[string]any, 0, len(body.ChunkHashes))
	for _, hash := range body.ChunkHashes {
		blobKey, exists := knownHashes[hash]
		entry := map[string]any{
			"hash":   hash,
			"exists": exists,
		}
		if exists {
			entry["blob_key"] = blobKey
		} else {
			entry["blob_key"] = nil
		}
		results = append(results, entry)
	}

	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func (d *driveAPI) handleContentRegister(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	_, tenantID := getUserTenant(r)
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Demo-Tenant header")
		return
	}

	var body struct {
		ContentID     string              `json:"content_id"`
		PlaintextSize int64               `json:"plaintext_size"`
		ChunkCount    int64               `json:"chunk_count"`
		ChunkSize     int64               `json:"chunk_size"`
		ChunkPlanRoot string              `json:"chunk_plan_root"`
		Chunks        []contentChunkEntry `json:"chunks"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}

	// Postgres path
	if d.gw.metaDB != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		// Check if already registered (idempotent)
		existing, err := d.gw.metaDB.GetContentEntry(ctx, body.ContentID, tenantID)
		if err != nil && !errors.Is(err, metadata.ErrNotFound) {
			d.gw.logger.Error("drive: content register check", slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		if existing != nil {
			// Already registered by this tenant — idempotent success
			writeJSON(w, http.StatusOK, map[string]any{
				"content_id":      body.ContentID,
				"registered":      true,
				"already_existed": true,
			})
			return
		}

		// Check if owned by another tenant
		anyTenant, err := d.gw.metaDB.GetContentEntryByContentID(ctx, body.ContentID)
		if err == nil && anyTenant != nil && anyTenant.TenantID != tenantID {
			writeError(w, http.StatusForbidden, "content_id owned by another tenant")
			return
		}

		// Insert content entry
		err = d.gw.metaDB.CreateContentEntry(ctx, metadata.ContentEntry{
			ContentID:        body.ContentID,
			TenantID:         tenantID,
			PlaintextSize:    body.PlaintextSize,
			ChunkCount:       body.ChunkCount,
			ChunkSize:        body.ChunkSize,
			ChunkPlanRoot:    body.ChunkPlanRoot,
			PepperGeneration: 1,
		})
		if err != nil {
			d.gw.logger.Error("drive: create content entry", slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		// Insert chunks
		dbChunks := make([]metadata.ContentChunk, 0, len(body.Chunks))
		for _, c := range body.Chunks {
			dbChunks = append(dbChunks, metadata.ContentChunk{
				ChunkIndex:       c.ChunkIndex,
				ChunkContentHash: c.ChunkContentHash,
				BlobKey:          c.BlobKey,
				PlaintextLen:     c.PlaintextLen,
				CiphertextLen:    c.CiphertextLen,
			})
		}
		if err := d.gw.metaDB.CreateContentChunks(ctx, body.ContentID, dbChunks); err != nil {
			d.gw.logger.Error("drive: create content chunks", slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		d.gw.logger.Info("drive: registered content",
			slog.String("content_id", body.ContentID),
			slog.Int("chunks", len(body.Chunks)),
		)

		writeJSON(w, http.StatusOK, map[string]any{
			"content_id":      body.ContentID,
			"registered":      true,
			"already_existed": false,
		})
		return
	}

	// In-memory fallback (dev mode)
	if existing, ok := d.getMemContent(body.ContentID); ok {
		if existing.TenantID != tenantID {
			writeError(w, http.StatusForbidden, "content_id owned by another tenant")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"content_id":      body.ContentID,
			"registered":      true,
			"already_existed": true,
		})
		return
	}

	blobKeys := make([]string, 0, len(body.Chunks))
	ciphertextHashes := make([]string, 0, len(body.Chunks))
	memChunks := make([]memContentChunk, 0, len(body.Chunks))
	for _, c := range body.Chunks {
		blobKeys = append(blobKeys, c.BlobKey)
		ciphertextHashes = append(ciphertextHashes, c.ChunkContentHash)
		memChunks = append(memChunks, memContentChunk{
			ChunkIndex:       c.ChunkIndex,
			ChunkContentHash: c.ChunkContentHash,
			BlobKey:          c.BlobKey,
			PlaintextLen:     c.PlaintextLen,
			CiphertextLen:    c.CiphertextLen,
		})
	}

	d.putMemContent(body.ContentID, &memContentEntry{
		ContentID:        body.ContentID,
		TenantID:         tenantID,
		PlaintextSize:    body.PlaintextSize,
		ChunkCount:       body.ChunkCount,
		ChunkSize:        body.ChunkSize,
		ChunkPlanRoot:    body.ChunkPlanRoot,
		BlobKeys:         blobKeys,
		CiphertextHashes: ciphertextHashes,
		CreatedAt:        time.Now(),
	}, memChunks)

	d.gw.logger.Info("drive: registered content",
		slog.String("content_id", body.ContentID),
		slog.Int("chunks", len(body.Chunks)),
	)

	writeJSON(w, http.StatusOK, map[string]any{
		"content_id":      body.ContentID,
		"registered":      true,
		"already_existed": false,
	})
}

func (d *driveAPI) handleUploadCommitDedup(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	_, tenantID := getUserTenant(r)
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Demo-Tenant header")
		return
	}

	var body struct {
		Protocol              string          `json:"protocol"`
		ContentID             string          `json:"content_id"`
		NodeID                string          `json:"node_id"`
		Manifest              json.RawMessage `json:"manifest"`
		ManifestNonce         string          `json:"manifest_nonce_hex"`
		ManifestCiphertextSHA string          `json:"manifest_ciphertext_sha256"`
		Header                json.RawMessage `json:"header"`
		WrappedDEK            string          `json:"wrapped_dek_hex"`
		WrapNonce             string          `json:"wrap_nonce_hex"`
		WrappedContentKey     string          `json:"wrapped_content_key_hex"`
		ContentWrapNonce      string          `json:"content_wrap_nonce_hex"`
		ReusedBlobKeys        []string        `json:"reused_blob_keys"`
		NewBlobKeys           []string        `json:"new_blob_keys"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}

	if body.Protocol != "KDRV1" {
		writeError(w, http.StatusBadRequest, "expected protocol KDRV1")
		return
	}

	// Verify all reused blob keys exist.
	// Postgres path
	if d.gw.metaDB != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		entry, err := d.gw.metaDB.GetContentEntry(ctx, body.ContentID, tenantID)
		if err != nil {
			if errors.Is(err, metadata.ErrNotFound) {
				writeError(w, http.StatusBadRequest, "content_id not registered")
				return
			}
			d.gw.logger.Error("drive: commit dedup content check", slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		chunks, err := d.gw.metaDB.GetContentChunks(ctx, body.ContentID, tenantID)
		if err != nil {
			d.gw.logger.Error("drive: commit dedup chunks", slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		knownBlobs := map[string]bool{}
		for _, c := range chunks {
			knownBlobs[c.BlobKey] = true
		}

		for _, k := range body.ReusedBlobKeys {
			if !knownBlobs[k] {
				writeError(w, http.StatusBadRequest, "reused blob key not found")
				return
			}
		}

		_ = entry // entry verified to exist and belong to tenant
	} else {
		// In-memory fallback (dev mode)
		entry, ok := d.getMemContent(body.ContentID)
		if !ok || entry.TenantID != tenantID {
			writeError(w, http.StatusBadRequest, "content_id not registered")
			return
		}
		knownBlobs := map[string]bool{}
		for _, k := range entry.BlobKeys {
			knownBlobs[k] = true
		}

		for _, k := range body.ReusedBlobKeys {
			if !knownBlobs[k] {
				writeError(w, http.StatusBadRequest, "reused blob key not found")
				return
			}
		}
	}

	// Verify new blob keys were actually uploaded to the blob store.
	// Without this check a client could commit a version whose chunks
	// were never persisted, leaving the content undownloadable.
	if d.gw.store != nil && len(body.NewBlobKeys) > 0 {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		for _, k := range body.NewBlobKeys {
			_, err := d.gw.store.Head(ctx, blobstore.ObjectRef{Key: k})
			if err != nil {
				if errors.Is(err, blobstore.ErrNotFound) {
					writeError(w, http.StatusBadRequest, "new blob key not uploaded: "+k)
					return
				}
				d.gw.logger.Error("drive: commit dedup blob head", slog.Any("err", err))
				writeError(w, http.StatusInternalServerError, "internal error")
				return
			}
		}
	}

	versionID := generateID("version")

	d.gw.logger.Info("drive: committed KDRV1 version",
		slog.String("version_id", versionID),
		slog.String("content_id", body.ContentID),
		slog.Int("deduped_chunks", len(body.ReusedBlobKeys)),
		slog.Int("new_chunks", len(body.NewBlobKeys)),
	)

	writeJSON(w, http.StatusOK, map[string]any{
		"version_id":     versionID,
		"committed":      true,
		"deduped_chunks": len(body.ReusedBlobKeys),
		"new_chunks":     len(body.NewBlobKeys),
	})
}

// handleBlobUpload stores a single ciphertext blob under its blob_key.
// This is used by the KDRV1 dedup upload flow (dedupUpload NAPI callback).
//
//	POST /v1/blobs:upload
//	Body: { blob_key, ciphertext_hex, ciphertext_sha256, plaintext_len, ciphertext_len }
func (d *driveAPI) handleBlobUpload(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	_, tenantID := getUserTenant(r)
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Demo-Tenant header")
		return
	}

	var body struct {
		BlobKey          string `json:"blob_key"`
		CiphertextHex    string `json:"ciphertext_hex"`
		CiphertextSHA256 string `json:"ciphertext_sha256"`
		PlaintextLen     int64  `json:"plaintext_len"`
		CiphertextLen    int64  `json:"ciphertext_len"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if body.BlobKey == "" || body.CiphertextHex == "" {
		writeError(w, http.StatusBadRequest, "missing blob_key or ciphertext_hex")
		return
	}

	// Decode ciphertext
	ctBytes, err := hexDecodeString(body.CiphertextHex)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid ciphertext_hex")
		return
	}

	// Store ciphertext in the blob store
	if d.gw.store != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		putReq := blobstore.PutRequest{
			Key:            body.BlobKey,
			Body:           bytes.NewReader(ctBytes),
			ExpectedLength: int64(len(ctBytes)),
			ContentType:    "application/octet-stream",
		}
		// Only set checksum if provided (dev store may not validate it)
		if body.CiphertextSHA256 != "" {
			putReq.ChecksumSHA256 = body.CiphertextSHA256
		}
		_, err := d.gw.store.Put(ctx, putReq)
		if err != nil {
			d.gw.logger.Error("drive: blob upload", slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, "failed to store blob")
			return
		}
	}

	d.gw.logger.Info("drive: blob uploaded",
		slog.String("blob_key", body.BlobKey),
		slog.Int("ciphertext_len", len(ctBytes)),
	)

	writeJSON(w, http.StatusOK, map[string]any{
		"blob_key":       body.BlobKey,
		"stored":         true,
		"ciphertext_len": len(ctBytes),
	})
}
