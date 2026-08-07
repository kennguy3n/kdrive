// Package server — drive demo HTTP handlers. These implement the
// §17 subset needed for the React web sample (plan Part C).
// The gateway stores only ciphertext + opaque metadata; it never
// holds plaintext Drive keys.
package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
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

// demoAPI is the HTTP handler for all /v1/ drive demo endpoints.
// It is mounted on the gateway mux when Postgres is available.
type demoAPI struct {
	gw *Gateway
}

// newDemoAPI creates the demo API handler.
func newDemoAPI(gw *Gateway) *demoAPI {
	return &demoAPI{gw: gw}
}

// registerDemoRoutes wires all /v1/ endpoints onto the mux.
func (d *demoAPI) registerDemoRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/tenants", d.handleTenants)
	mux.HandleFunc("/v1/folders", d.handleFoldersRoot)
	mux.HandleFunc("/v1/folders/", d.handleFolderChildren)
	mux.HandleFunc("/v1/nodes/", d.handleNode)
	mux.HandleFunc("/v1/uploads:initiate", d.handleUploadInitiate)
	mux.HandleFunc("/v1/uploads/", d.handleUploadChunks)
	mux.HandleFunc("/v1/versions/", d.handleVersion)
	mux.HandleFunc("/v1/domains/", d.handleDomain)
	mux.HandleFunc("/v1/shares", d.handleSharesRoot)
	mux.HandleFunc("/v1/shares/", d.handleShareRevoke)
	mux.HandleFunc("/v1/vectors", d.handleVectors)
}

// --- CORS middleware ---

func setCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Demo-User, X-Demo-Tenant")
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func getUserTenant(r *http.Request) (userID, tenantID string) {
	userID = r.Header.Get("X-Demo-User")
	tenantID = r.Header.Get("X-Demo-Tenant")
	return
}

// --- Tenants ---

func (d *demoAPI) handleTenants(w http.ResponseWriter, r *http.Request) {
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
	tenants, err := d.gw.metaDB.ListTenantsDemo(r.Context())
	if err != nil {
		d.gw.logger.Error("demo: list tenants", slog.Any("err", err))
		writeError(w, http.StatusInternalServerError, "failed to list tenants")
		return
	}
	if tenants == nil {
		tenants = []metadata.TenantDemo{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"tenants": tenants})
}

// --- Folders ---

func (d *demoAPI) handleFoldersRoot(w http.ResponseWriter, r *http.Request) {
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

func (d *demoAPI) handleFolderChildren(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
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
		subFolders, err := d.gw.metaDB.ListFolders(r.Context(), folder.TenantID, folderID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list subfolders")
			return
		}
		nodes, err := d.gw.metaDB.ListNodes(r.Context(), folderID)
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

func (d *demoAPI) handleNode(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
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
		snap, err := d.gw.metaDB.GetLatestAccessContext(r.Context(), nodeID)
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

var (
	uploadSessions   = map[string]*uploadSession{}
	uploadSessionsMu sync.RWMutex
)

func (d *demoAPI) handleUploadInitiate(w http.ResponseWriter, r *http.Request) {
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

func (d *demoAPI) handleUploadChunks(w http.ResponseWriter, r *http.Request) {
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

	uploadSessionsMu.RLock()
	session, ok := uploadSessions[sid]
	uploadSessionsMu.RUnlock()
	if !ok {
		writeError(w, http.StatusNotFound, "upload session not found")
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
			d.gw.logger.Error("demo: store chunk", slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, "failed to store chunk")
			return
		}
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

func (d *demoAPI) handleVersion(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
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
		// Return a short-lived download capability (demo: just echo the version ID).
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
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"version_id": vid,
			"manifest":   "demo_manifest_placeholder",
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

func (d *demoAPI) handleDomain(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
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
		envelopes, err := d.gw.metaDB.ListEnvelopesByDomain(r.Context(), domainID)
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
		err := d.gw.metaDB.RotateEncryptionDomain(r.Context(), domainID, prevEnv)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to rotate domain")
			return
		}
		dom, _ := d.gw.metaDB.GetEncryptionDomain(r.Context(), domainID)
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
	dom, err := d.gw.metaDB.GetEncryptionDomain(r.Context(), domainID)
	if err != nil {
		writeError(w, http.StatusNotFound, "domain not found")
		return
	}
	writeJSON(w, http.StatusOK, dom)
}

// --- Shares ---

func (d *demoAPI) handleSharesRoot(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	userID, _ := getUserTenant(r)
	if userID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Demo-User header")
		return
	}
	if d.gw.metaDB == nil {
		writeError(w, http.StatusServiceUnavailable, "postgres not configured")
		return
	}
	grants, err := d.gw.metaDB.ListActiveShareGrants(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list grants")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"grants": grants})
}

func (d *demoAPI) handleShareRevoke(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
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
	err := d.gw.metaDB.RevokeShareGrant(r.Context(), grantID)
	if err != nil {
		writeError(w, http.StatusNotFound, "grant not found or already revoked")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "revoked"})
}

// --- Vectors ---

func (d *demoAPI) handleVectors(w http.ResponseWriter, r *http.Request) {
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
