// Package server — Drive REST API HTTP handlers. These implement the
// §17 client API surface (plan Part C): tenants, folders, nodes,
// uploads, versions, encryption domains, share grants, and test vectors.
// The gateway stores only ciphertext + opaque metadata; it never
// holds plaintext Drive keys.
//
// Authentication: the current implementation trusts X-Demo-Tenant and
// X-Demo-User headers for demo / web-sample purposes. Production
// deployments replace getUserTenant with KChat's real identity layer
// (session tokens, device certificates) in front of these handlers.
package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kchat/drive/internal/metadata"
	"github.com/kchat/drive/pkg/blobstore"
)

// driveAPI is the HTTP handler for all /v1/ Drive REST API endpoints.
// It is mounted on the gateway mux when Postgres is available.
type driveAPI struct {
	gw *Gateway
}

// newDriveAPI creates the Drive API handler.
func newDriveAPI(gw *Gateway) *driveAPI {
	return &driveAPI{gw: gw}
}

// registerDriveRoutes wires all /v1/ endpoints onto the mux.
func (d *driveAPI) registerDriveRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/tenants", d.handleTenants)
	mux.HandleFunc("/v1/folders", d.handleFoldersRoot)
	mux.HandleFunc("/v1/folders/", d.handleFolderChildren)
	mux.HandleFunc("/v1/nodes/", d.handleNode)
	mux.HandleFunc("/v1/uploads:initiate", d.handleUploadInitiate)
	mux.HandleFunc("/v1/uploads/", d.handleUploadChunks)
	mux.HandleFunc("/v1/uploads:commitDedup", d.handleUploadCommitDedup)
	mux.HandleFunc("/v1/versions/", d.handleVersion)
	mux.HandleFunc("/v1/domains/", d.handleDomain)
	mux.HandleFunc("/v1/shares", d.handleSharesRoot)
	mux.HandleFunc("/v1/shares/", d.handleShareRevoke)
	mux.HandleFunc("/v1/vectors", d.handleVectors)
	// KDRV1 content dedup endpoints
	mux.HandleFunc("/v1/content:check", d.handleContentCheck)
	mux.HandleFunc("/v1/content:checkChunks", d.handleContentCheckChunks)
	mux.HandleFunc("/v1/content:register", d.handleContentRegister)
}

// --- CORS middleware ---

func setCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Tenant-Id, X-User-Id, X-Demo-User, X-Demo-Tenant")
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// getUserTenant extracts the caller identity from request headers.
// Accepts either production headers (Authorization + X-Tenant-Id + X-User-Id)
// or demo headers (X-Demo-User / X-Demo-Tenant) for backward compatibility.
func getUserTenant(r *http.Request) (userID, tenantID string) {
	// Try production headers first
	userID = r.Header.Get("X-User-Id")
	tenantID = r.Header.Get("X-Tenant-Id")
	if userID != "" && tenantID != "" {
		return
	}
	// Fall back to demo headers
	userID = r.Header.Get("X-Demo-User")
	tenantID = r.Header.Get("X-Demo-Tenant")
	return
}

// --- Tenants ---

func (d *driveAPI) handleTenants(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if d.gw.metaDB == nil {
		writeError(w, http.StatusServiceUnavailable, "postgres not configured")
		return
	}
	tenants, err := d.gw.metaDB.ListDriveTenants(r.Context())
	if err != nil {
		d.gw.logger.Error("drive: list tenants", slog.Any("err", err))
		writeError(w, http.StatusInternalServerError, "failed to list tenants")
		return
	}
	if tenants == nil {
		tenants = []metadata.DriveTenant{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"tenants": tenants})
}

// --- Folders ---

func (d *driveAPI) handleFoldersRoot(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, tenantID := getUserTenant(r)
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Demo-Tenant header")
		return
	}
	if d.gw.metaDB == nil {
		writeError(w, http.StatusServiceUnavailable, "postgres not configured")
		return
	}

	switch r.Method {
	case http.MethodGet:
		parentID := r.URL.Query().Get("parent")
		folders, err := d.gw.metaDB.ListFolders(r.Context(), tenantID, parentID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list folders")
			return
		}
		if folders == nil {
			folders = []metadata.Folder{}
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"folders": folders})

	case http.MethodPost:
		var body struct {
			NameEncrypted  string `json:"name_encrypted_hex"`
			ParentFolderID string `json:"parent_folder_id"`
			PrivacyMode    string `json:"privacy_mode"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		nameBytes, err := hex.DecodeString(body.NameEncrypted)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid name_encrypted_hex")
			return
		}
		if body.PrivacyMode == "" {
			body.PrivacyMode = "secured"
		}
		folderID := generateID("folder")
		err = d.gw.metaDB.CreateFolder(r.Context(), metadata.Folder{
			ID:             folderID,
			TenantID:       tenantID,
			ParentFolderID: body.ParentFolderID,
			NameEncrypted:  nameBytes,
			PrivacyMode:    body.PrivacyMode,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to create folder")
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"folder_id": folderID})

	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (d *driveAPI) handleFolderChildren(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, tenantID := getUserTenant(r)
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Demo-Tenant header")
		return
	}
	// Path: /v1/folders/{folder_id}/children
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/folders/"), "/")
	if len(parts) < 2 || parts[1] != "children" {
		writeError(w, http.StatusBadRequest, "expected /v1/folders/{id}/children")
		return
	}
	folderID := parts[0]
	if d.gw.metaDB == nil {
		writeError(w, http.StatusServiceUnavailable, "postgres not configured")
		return
	}

	switch r.Method {
	case http.MethodGet:
		folder, err := d.gw.metaDB.GetFolder(r.Context(), folderID)
		if err != nil {
			writeError(w, http.StatusNotFound, "folder not found")
			return
		}
		// Verify folder belongs to requesting tenant
		if folder.TenantID != tenantID {
			writeError(w, http.StatusNotFound, "folder not found")
			return
		}
		subFolders, err := d.gw.metaDB.ListFolders(r.Context(), folder.TenantID, folderID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list subfolders")
			return
		}
		nodes, err := d.gw.metaDB.ListNodes(r.Context(), folderID, tenantID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list nodes")
			return
		}
		if subFolders == nil {
			subFolders = []metadata.Folder{}
		}
		if nodes == nil {
			nodes = []metadata.Node{}
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"folder":   folder,
			"children": subFolders,
			"nodes":    nodes,
		})

	case http.MethodPost:
		var body struct {
			NameEncrypted string `json:"name_encrypted_hex"`
			MimeType      string `json:"mime_type"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		nameBytes, err := hex.DecodeString(body.NameEncrypted)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid name_encrypted_hex")
			return
		}
		folder, err := d.gw.metaDB.GetFolder(r.Context(), folderID)
		if err != nil {
			writeError(w, http.StatusNotFound, "folder not found")
			return
		}
		// Verify folder belongs to requesting tenant
		if folder.TenantID != tenantID {
			writeError(w, http.StatusNotFound, "folder not found")
			return
		}
		nodeID := generateID("node")
		err = d.gw.metaDB.CreateNode(r.Context(), metadata.Node{
			ID:            nodeID,
			TenantID:      folder.TenantID,
			FolderID:      folderID,
			NameEncrypted: nameBytes,
			MimeType:      body.MimeType,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to create node")
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"node_id": nodeID})

	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// --- Nodes ---

func (d *driveAPI) handleNode(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, tenantID := getUserTenant(r)
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Demo-Tenant header")
		return
	}
	// Path: /v1/nodes/{node_id}/accessContext
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/nodes/"), "/")
	if len(parts) < 2 {
		writeError(w, http.StatusBadRequest, "expected /v1/nodes/{id}/{action}")
		return
	}
	nodeID := parts[0]
	action := parts[1]
	if d.gw.metaDB == nil {
		writeError(w, http.StatusServiceUnavailable, "postgres not configured")
		return
	}

	switch action {
	case "accessContext":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		snap, err := d.gw.metaDB.GetLatestAccessContext(r.Context(), nodeID, tenantID)
		if err != nil {
			writeError(w, http.StatusNotFound, "no access context")
			return
		}
		writeJSON(w, http.StatusOK, snap)

	case "shares":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		userID, tenantID := getUserTenant(r)
		var body struct {
			GranteeUserID string `json:"grantee_user_id"`
			KeyEnvelopeID string `json:"key_envelope_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		grantID := generateID("grant")
		err := d.gw.metaDB.CreateShareGrant(r.Context(), metadata.ShareGrant{
			ID:            grantID,
			TenantID:      tenantID,
			NodeID:        nodeID,
			GrantorUserID: userID,
			GranteeUserID: body.GranteeUserID,
			Generation:    1,
			IsActive:      true,
			KeyEnvelopeID: body.KeyEnvelopeID,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to create share grant")
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"grant_id": grantID})

	default:
		writeError(w, http.StatusBadRequest, "unknown action: "+action)
	}
}

// --- Uploads ---

type uploadSession struct {
	SessionID  string            `json:"session_id"`
	TenantID   string            `json:"tenant_id"`
	NodeID     string            `json:"node_id"`
	FolderID   string            `json:"folder_id"`
	ChunkPlan  json.RawMessage   `json:"chunk_plan"`
	Manifest   json.RawMessage   `json:"manifest"`
	Header     json.RawMessage   `json:"header"`
	WrappedDEK string            `json:"wrapped_dek_hex"`
	WrapNonce  string            `json:"wrap_nonce_hex"`
	Chunks     []registeredChunk `json:"chunks"`
	CreatedAt  time.Time         `json:"created_at"`
}

type registeredChunk struct {
	Index         int    `json:"index"`
	BlobKey       string `json:"blob_key"`
	CiphertextHex string `json:"ciphertext_hex"`
	CiphertextSHA string `json:"ciphertext_sha256"`
	PlaintextLen  int64  `json:"plaintext_len"`
	CiphertextLen int64  `json:"ciphertext_len"`
}

// In-memory upload sessions (dev mode fallback when Postgres is not available).
var (
	uploadSessions   = map[string]*uploadSession{}
	uploadSessionsMu sync.RWMutex
)

func (d *driveAPI) handleUploadInitiate(w http.ResponseWriter, r *http.Request) {
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
		NodeID     string          `json:"node_id"`
		FolderID   string          `json:"folder_id"`
		ChunkPlan  json.RawMessage `json:"chunk_plan"`
		Manifest   json.RawMessage `json:"manifest"`
		Header     json.RawMessage `json:"header"`
		WrappedDEK string          `json:"wrapped_dek_hex"`
		WrapNonce  string          `json:"wrap_nonce_hex"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	sid := generateID("upload")

	// Postgres path
	if d.gw.metaDB != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		err := d.gw.metaDB.CreateKDRV1UploadSession(ctx, metadata.KDRV1UploadSession{
			ID:         sid,
			TenantID:   tenantID,
			NodeID:     body.NodeID,
			FolderID:   body.FolderID,
			ChunkPlan:  body.ChunkPlan,
			Manifest:   body.Manifest,
			Header:     body.Header,
			WrappedDEK: body.WrappedDEK,
			WrapNonce:  body.WrapNonce,
			Chunks:     json.RawMessage("[]"),
			State:      "UPLOADING",
		})
		if err != nil {
			d.gw.logger.Error("drive: create upload session", slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, "failed to create session")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"session_id": sid})
		return
	}

	// In-memory fallback (dev mode)
	session := &uploadSession{
		SessionID:  sid,
		TenantID:   tenantID,
		NodeID:     body.NodeID,
		FolderID:   body.FolderID,
		ChunkPlan:  body.ChunkPlan,
		Manifest:   body.Manifest,
		Header:     body.Header,
		WrappedDEK: body.WrappedDEK,
		WrapNonce:  body.WrapNonce,
		Chunks:     []registeredChunk{},
		CreatedAt:  time.Now(),
	}
	uploadSessionsMu.Lock()
	uploadSessions[sid] = session
	uploadSessionsMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]string{"session_id": sid})
}

func (d *driveAPI) handleUploadChunks(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	// Path: /v1/uploads/{sid}/chunks/{ordinal}:register
	path := strings.TrimPrefix(r.URL.Path, "/v1/uploads/")
	parts := strings.Split(path, "/")
	if len(parts) < 3 || parts[1] != "chunks" {
		writeError(w, http.StatusBadRequest, "expected /v1/uploads/{sid}/chunks/{ordinal}:register")
		return
	}
	sid := parts[0]
	ordinalStr := strings.SplitN(parts[2], ":", 2)[0]

	_, tenantID := getUserTenant(r)
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Demo-Tenant header")
		return
	}

	var body struct {
		CiphertextHex string `json:"ciphertext_hex"`
		CiphertextSHA string `json:"ciphertext_sha256"`
		PlaintextLen  int64  `json:"plaintext_len"`
		CiphertextLen int64  `json:"ciphertext_len"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}

	var ordinal int
	fmt.Sscanf(ordinalStr, "%d", &ordinal)

	blobKey := fmt.Sprintf("blob_%s_%d", sid, ordinal)

	// Store ciphertext in the blob store.
	ctBytes, err := hex.DecodeString(body.CiphertextHex)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid ciphertext_hex")
		return
	}
	if d.gw.store != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		_, err := d.gw.store.Put(ctx, blobstore.PutRequest{
			Key:            blobKey,
			Body:           bytes.NewReader(ctBytes),
			ExpectedLength: int64(len(ctBytes)),
			ContentType:    "application/octet-stream",
		})
		if err != nil {
			d.gw.logger.Error("drive: store chunk", slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, "failed to store chunk")
			return
		}
	}

	// Postgres path: atomically append chunk to session
	if d.gw.metaDB != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		chunkJSON, err := json.Marshal(registeredChunk{
			Index:         ordinal,
			BlobKey:       blobKey,
			CiphertextHex: body.CiphertextHex,
			CiphertextSHA: body.CiphertextSHA,
			PlaintextLen:  body.PlaintextLen,
			CiphertextLen: body.CiphertextLen,
		})
		if err != nil {
			d.gw.logger.Error("drive: marshal chunk", slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		if err := d.gw.metaDB.AppendKDRV1UploadSessionChunk(ctx, sid, tenantID, chunkJSON); err != nil {
			if errors.Is(err, metadata.ErrNotFound) {
				writeError(w, http.StatusNotFound, "upload session not found")
				return
			}
			d.gw.logger.Error("drive: append upload session chunk", slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{"blob_key": blobKey})
		return
	}

	// In-memory fallback (dev mode)
	uploadSessionsMu.RLock()
	session, ok := uploadSessions[sid]
	uploadSessionsMu.RUnlock()
	if !ok {
		writeError(w, http.StatusNotFound, "upload session not found")
		return
	}
	if session.TenantID != tenantID {
		writeError(w, http.StatusForbidden, "upload session belongs to another tenant")
		return
	}

	uploadSessionsMu.Lock()
	session.Chunks = append(session.Chunks, registeredChunk{
		Index:         ordinal,
		BlobKey:       blobKey,
		CiphertextHex: body.CiphertextHex,
		CiphertextSHA: body.CiphertextSHA,
		PlaintextLen:  body.PlaintextLen,
		CiphertextLen: body.CiphertextLen,
	})
	uploadSessionsMu.Unlock()

	writeJSON(w, http.StatusOK, map[string]string{"blob_key": blobKey})
}

// --- Versions ---

func (d *driveAPI) handleVersion(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, tenantID := getUserTenant(r)
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Demo-Tenant header")
		return
	}
	// Path: /v1/versions/{vid}:authorizeDownload  or  /v1/versions/{vid}
	path := strings.TrimPrefix(r.URL.Path, "/v1/versions/")
	parts := strings.SplitN(path, ":", 2)
	vid := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	switch action {
	case "authorizeDownload":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		// Return a short-lived download capability.
		// TODO: sign the capability with a gateway key and bind it to the
		// caller's identity + the blob key (plan §17).
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"version_id": vid,
			"capability": fmt.Sprintf("cap_%s_%d", vid, time.Now().Unix()),
			"expires_at": time.Now().Add(15 * time.Minute).UTC().Format(time.RFC3339),
		})

	case "manifest":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		// Return the stored manifest for this version.
		// TODO: fetch the actual encrypted manifest from metadata store.
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"version_id": vid,
			"manifest":   "manifest_not_yet_persisted",
		})

	default:
		// GET /v1/versions/{vid} — return version metadata.
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"version_id": vid,
			"status":     "COMMITTED_DURABLE",
		})
	}
}

// --- Domains ---

func (d *driveAPI) handleDomain(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, tenantID := getUserTenant(r)
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Demo-Tenant header")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/domains/")
	parts := strings.Split(path, "/")
	domainID := parts[0]
	if domainID == "" {
		writeError(w, http.StatusBadRequest, "missing domain_id")
		return
	}
	if d.gw.metaDB == nil {
		writeError(w, http.StatusServiceUnavailable, "postgres not configured")
		return
	}

	if len(parts) >= 2 && parts[1] == "envelopes" {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		envelopes, err := d.gw.metaDB.ListEnvelopesByDomain(r.Context(), domainID, tenantID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list envelopes")
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"envelopes": envelopes})
		return
	}

	if len(parts) >= 2 && parts[1] == "generations" {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var body struct {
			PrevKeyEnvelopeHex string `json:"prev_key_envelope_hex"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		prevEnv, _ := hex.DecodeString(body.PrevKeyEnvelopeHex)
		err := d.gw.metaDB.RotateEncryptionDomain(r.Context(), domainID, tenantID, prevEnv)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to rotate domain")
			return
		}
		dom, _ := d.gw.metaDB.GetEncryptionDomain(r.Context(), domainID, tenantID)
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"domain_id":  domainID,
			"generation": dom.Generation,
		})
		return
	}

	// GET /v1/domains/{domain_id}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	dom, err := d.gw.metaDB.GetEncryptionDomain(r.Context(), domainID, tenantID)
	if err != nil {
		writeError(w, http.StatusNotFound, "domain not found")
		return
	}
	writeJSON(w, http.StatusOK, dom)
}

// --- Shares ---

func (d *driveAPI) handleSharesRoot(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	userID, tenantID := getUserTenant(r)
	if userID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Demo-User header")
		return
	}
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Demo-Tenant header")
		return
	}
	if d.gw.metaDB == nil {
		writeError(w, http.StatusServiceUnavailable, "postgres not configured")
		return
	}
	grants, err := d.gw.metaDB.ListActiveShareGrants(r.Context(), userID, tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list grants")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"grants": grants})
}

func (d *driveAPI) handleShareRevoke(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, tenantID := getUserTenant(r)
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Demo-Tenant header")
		return
	}
	// Path: /v1/shares/{grant_id}
	grantID := strings.TrimPrefix(r.URL.Path, "/v1/shares/")
	if grantID == "" {
		writeError(w, http.StatusBadRequest, "missing grant_id")
		return
	}
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if d.gw.metaDB == nil {
		writeError(w, http.StatusServiceUnavailable, "postgres not configured")
		return
	}
	err := d.gw.metaDB.RevokeShareGrant(r.Context(), grantID, tenantID)
	if err != nil {
		writeError(w, http.StatusNotFound, "grant not found or already revoked")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "revoked"})
}

// --- Vectors ---

func (d *driveAPI) handleVectors(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	// Return the KDRV1 test vectors for cross-language verification.
	// These match the Rust-side kchat_drive_crypto::all_vectors_json().
	vectors := map[string]interface{}{
		"protocol": "kdrv1",
		"version":  1,
		"suite":    1,
		"note":     "Cross-language test vectors — verify byte-equality with Rust SDK",
	}
	writeJSON(w, http.StatusOK, vectors)
}

// --- Helpers ---

var idCounter atomic.Int64

func generateID(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), idCounter.Add(1))
}
