// Package server — chat-storage gateway extensions.
// Adds endpoints for archive segments, search shards, backup manifests,
// and message delivery used by the Rust chat-storage SDK.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/kchat/drive/pkg/blobstore"
)

// chatStorageAPI wires the chat-storage-specific endpoints onto the mux.
type chatStorageAPI struct {
	gw     *Gateway
	logger *slog.Logger
}

func newChatStorageAPI(g *Gateway) *chatStorageAPI {
	return &chatStorageAPI{
		gw:     g,
		logger: g.logger.With("component", "chat-storage-api"),
	}
}

// requireAuth checks for authentication and returns the tenant ID.
//
// Accepts either:
//   - Authorization: Bearer <token> + X-Tenant-Id + X-User-Id (production)
//   - X-Demo-Tenant + X-Demo-User (backward compat for existing tests)
//
// Returns the tenant ID and false if authentication is missing.
func (c *chatStorageAPI) requireAuth(r *http.Request) (string, bool) {
	// Try production headers first
	tenant := r.Header.Get("X-Tenant-Id")
	user := r.Header.Get("X-User-Id")
	auth := r.Header.Get("Authorization")
	if tenant != "" && user != "" && auth != "" {
		return tenant, true
	}
	// Fall back to demo headers for backward compatibility
	tenant = r.Header.Get("X-Demo-Tenant")
	user = r.Header.Get("X-Demo-User")
	if tenant != "" && user != "" {
		return tenant, true
	}
	return "", false
}

// tenantPrefix returns the tenant-scoped blob key prefix.
func tenantPrefix(tenant string) string {
	return fmt.Sprintf("tenant/%s/", tenant)
}

// registerChatStorageRoutes wires all /v1/chat/ endpoints onto the mux.
func (c *chatStorageAPI) registerChatStorageRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/chat/messages/", c.handleMessages)
	mux.HandleFunc("/v1/chat/archive/segments/", c.handleArchiveSegment)
	mux.HandleFunc("/v1/chat/archive/manifests", c.handleArchiveManifests)
	mux.HandleFunc("/v1/chat/search/shards/", c.handleSearchShard)
	mux.HandleFunc("/v1/chat/backup/manifests", c.handleBackupManifests)
	mux.HandleFunc("/v1/chat/backup/segment/", c.handleBackupSegment)
	mux.HandleFunc("/v1/chat/media/", c.handleMediaBlob)
}

// sanitizeObjectID validates that an ID is safe for use as a blob path component.
// Rejects empty strings, path separators, and other dangerous characters.
func sanitizeObjectID(id string) bool {
	if id == "" || len(id) > 256 {
		return false
	}
	// Reject if contains path separators, null bytes, or relative path components
	for _, c := range id {
		if c == '/' || c == '\\' || c == 0 || c == '.' {
			return false
		}
	}
	return true
}

// --- Message delivery ---

// FetchResult is one page of messages plus the next cursor.
type FetchResult struct {
	Messages   []json.RawMessage `json:"messages"`
	NextCursor *string           `json:"next_cursor,omitempty"`
}

func (c *chatStorageAPI) handleMessages(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	tenant, ok := c.requireAuth(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}
	_ = tenant

	path := strings.TrimPrefix(r.URL.Path, "/v1/chat/messages/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 1 || parts[0] == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "conversation_id required"})
		return
	}
	conversationID := parts[0]
	if !sanitizeObjectID(conversationID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid conversation_id"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		afterCursor := r.URL.Query().Get("after")
		// In a full implementation, this would fetch from the delivery store.
		// For now, return an empty page with no cursor.
		result := FetchResult{
			Messages:   []json.RawMessage{},
			NextCursor: nil,
		}
		_ = conversationID
		_ = afterCursor
		writeJSON(w, http.StatusOK, result)

	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

// --- Archive segments ---

func (c *chatStorageAPI) handleArchiveSegment(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	tenant, ok := c.requireAuth(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}

	segmentID := strings.TrimPrefix(r.URL.Path, "/v1/chat/archive/segments/")
	if !sanitizeObjectID(segmentID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid segment_id"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	switch r.Method {
	case http.MethodPut, http.MethodPost:
		body, err := io.ReadAll(io.LimitReader(r.Body, 256<<20)) // 256 MB max
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
			return
		}
		objRef := fmt.Sprintf("%schat-archive/segments/%s", tenantPrefix(tenant), segmentID)
		_, err = c.gw.store.Put(ctx, blobstore.PutRequest{
			Key:            objRef,
			Body:           bytes.NewReader(body),
			ExpectedLength: int64(len(body)),
		})
		if err != nil {
			c.logger.Error("archive segment upload failed", "segment_id", segmentID, "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "upload failed"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"segment_id": segmentID})

	case http.MethodGet:
		objRef := fmt.Sprintf("%schat-archive/segments/%s", tenantPrefix(tenant), segmentID)
		rc, _, err := c.gw.store.Get(ctx, blobstore.GetRequest{
			Ref: blobstore.VersionedObjectRef{Key: objRef},
		})
		if err != nil {
			if errors.Is(err, blobstore.ErrNotFound) {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "segment not found"})
				return
			}
			c.logger.Error("archive segment download failed", "segment_id", segmentID, "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "download failed"})
			return
		}
		defer rc.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, rc)

	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

// --- Archive manifests ---

func (c *chatStorageAPI) handleArchiveManifests(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	tenant, ok := c.requireAuth(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	switch r.Method {
	case http.MethodPost, http.MethodPut:
		body, err := io.ReadAll(io.LimitReader(r.Body, 16<<20)) // 16 MB max
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
			return
		}
		// Generate a manifest ID from content hash or timestamp.
		manifestID := fmt.Sprintf("manifest-%d", time.Now().UnixNano())
		objKey := fmt.Sprintf("%schat-archive/manifests/%s", tenantPrefix(tenant), manifestID)
		_, err = c.gw.store.Put(ctx, blobstore.PutRequest{
			Key:            objKey,
			Body:           bytes.NewReader(body),
			ExpectedLength: int64(len(body)),
		})
		if err != nil {
			c.logger.Error("archive manifest upload failed", "manifest_id", manifestID, "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "upload failed"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"manifest_id": manifestID})

	case http.MethodGet:
		// List manifests after a given generation.
		// In a full implementation, this would query metadata for manifests
		// with generation > after. For now, return empty.
		after := r.URL.Query().Get("after")
		_ = after
		writeJSON(w, http.StatusOK, map[string][]json.RawMessage{"manifests": {}})

	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

// --- Search shards ---

func (c *chatStorageAPI) handleSearchShard(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	tenant, ok := c.requireAuth(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}

	shardKey := strings.TrimPrefix(r.URL.Path, "/v1/chat/search/shards/")
	if !sanitizeObjectID(shardKey) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid shard_key"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	switch r.Method {
	case http.MethodPut, http.MethodPost:
		body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20)) // 64 MB max
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
			return
		}
		objKey := fmt.Sprintf("%schat-search/shards/%s", tenantPrefix(tenant), shardKey)
		_, err = c.gw.store.Put(ctx, blobstore.PutRequest{
			Key:            objKey,
			Body:           bytes.NewReader(body),
			ExpectedLength: int64(len(body)),
		})
		if err != nil {
			c.logger.Error("search shard upload failed", "shard_key", shardKey, "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "upload failed"})
			return
		}
		w.WriteHeader(http.StatusNoContent)

	case http.MethodGet:
		objKey := fmt.Sprintf("%schat-search/shards/%s", tenantPrefix(tenant), shardKey)
		rc, _, err := c.gw.store.Get(ctx, blobstore.GetRequest{
			Ref: blobstore.VersionedObjectRef{Key: objKey},
		})
		if err != nil {
			if errors.Is(err, blobstore.ErrNotFound) {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "shard not found"})
				return
			}
			c.logger.Error("search shard download failed", "shard_key", shardKey, "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "download failed"})
			return
		}
		defer rc.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, rc)

	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

// --- Backup manifests ---

func (c *chatStorageAPI) handleBackupManifests(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	tenant, ok := c.requireAuth(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	switch r.Method {
	case http.MethodPost, http.MethodPut:
		body, err := io.ReadAll(io.LimitReader(r.Body, 16<<20)) // 16 MB max
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
			return
		}
		manifestID := fmt.Sprintf("backup-%d", time.Now().UnixNano())
		objKey := fmt.Sprintf("%schat-backup/manifests/%s", tenantPrefix(tenant), manifestID)
		_, err = c.gw.store.Put(ctx, blobstore.PutRequest{
			Key:            objKey,
			Body:           bytes.NewReader(body),
			ExpectedLength: int64(len(body)),
		})
		if err != nil {
			c.logger.Error("backup manifest upload failed", "manifest_id", manifestID, "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "upload failed"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"manifest_id": manifestID})

	case http.MethodGet:
		after := r.URL.Query().Get("after")
		_ = after
		writeJSON(w, http.StatusOK, map[string][]json.RawMessage{"manifests": {}})

	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

// --- Backup segments ---

func (c *chatStorageAPI) handleBackupSegment(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	tenant, ok := c.requireAuth(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}

	segmentID := strings.TrimPrefix(r.URL.Path, "/v1/chat/backup/segment/")
	if !sanitizeObjectID(segmentID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid segment_id"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	switch r.Method {
	case http.MethodPut, http.MethodPost:
		body, err := io.ReadAll(io.LimitReader(r.Body, 256<<20)) // 256 MB max
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
			return
		}
		objRef := fmt.Sprintf("%schat-backup/segments/%s", tenantPrefix(tenant), segmentID)
		_, err = c.gw.store.Put(ctx, blobstore.PutRequest{
			Key:            objRef,
			Body:           bytes.NewReader(body),
			ExpectedLength: int64(len(body)),
		})
		if err != nil {
			c.logger.Error("backup segment upload failed", "segment_id", segmentID, "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "upload failed"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"segment_id": segmentID})

	case http.MethodGet:
		objRef := fmt.Sprintf("%schat-backup/segments/%s", tenantPrefix(tenant), segmentID)
		rc, _, err := c.gw.store.Get(ctx, blobstore.GetRequest{
			Ref: blobstore.VersionedObjectRef{Key: objRef},
		})
		if err != nil {
			if errors.Is(err, blobstore.ErrNotFound) {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "segment not found"})
				return
			}
			c.logger.Error("backup segment download failed", "segment_id", segmentID, "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "download failed"})
			return
		}
		defer rc.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, rc)

	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

// --- Media blobs ---

func (c *chatStorageAPI) handleMediaBlob(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	tenant, ok := c.requireAuth(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}

	blobID := strings.TrimPrefix(r.URL.Path, "/v1/chat/media/")
	if !sanitizeObjectID(blobID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid blob_id"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	switch r.Method {
	case http.MethodPut, http.MethodPost:
		body, err := io.ReadAll(io.LimitReader(r.Body, 256<<20)) // 256 MB max
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
			return
		}
		objRef := fmt.Sprintf("%schat-media/%s", tenantPrefix(tenant), blobID)
		_, err = c.gw.store.Put(ctx, blobstore.PutRequest{
			Key:            objRef,
			Body:           bytes.NewReader(body),
			ExpectedLength: int64(len(body)),
		})
		if err != nil {
			c.logger.Error("media blob upload failed", "blob_id", blobID, "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "upload failed"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"blob_id": blobID})

	case http.MethodGet:
		objRef := fmt.Sprintf("%schat-media/%s", tenantPrefix(tenant), blobID)
		rc, _, err := c.gw.store.Get(ctx, blobstore.GetRequest{
			Ref: blobstore.VersionedObjectRef{Key: objRef},
		})
		if err != nil {
			if errors.Is(err, blobstore.ErrNotFound) {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "blob not found"})
				return
			}
			c.logger.Error("media blob download failed", "blob_id", blobID, "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "download failed"})
			return
		}
		defer rc.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, rc)

	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}
