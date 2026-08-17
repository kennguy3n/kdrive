// Package local_fs_dev implements the BlobStore and BlobInventory
// interfaces on top of a local filesystem. It exists for developer
// loopback and for the contract test suite, which must be runnable
// without cloud credentials.
//
// Objects are stored as files under a root directory. Metadata is kept
// in a sidecar JSON file next to each object so Head does not need to
// re-stat or re-hash the payload. The adapter is provider-version-
// aware: every Put records a synthetic version ID so PurgeAllVersions
// and DeleteVersion behave the same as against a versioned S3 bucket.
package local_fs_dev

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kchat/drive/pkg/blobstore"
)

// Provider is a filesystem-backed BlobStore used for dev and
// conformance tests.
type Provider struct {
	root string
	mu   sync.Mutex
	now  func() time.Time
}

// New returns a Provider rooted at root. The directory is created if
// it does not exist.
func New(root string) (*Provider, error) {
	if root == "" {
		return nil, errors.New("local_fs_dev: root path is required")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("local_fs_dev: create root %q: %w", root, err)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("local_fs_dev: resolve root %q: %w", root, err)
	}
	return &Provider{root: abs, now: time.Now}, nil
}

// NewWithClock returns a Provider with a custom clock for tests.
func NewWithClock(root string, clock func() time.Time) (*Provider, error) {
	p, err := New(root)
	if err != nil {
		return nil, err
	}
	if clock != nil {
		p.now = clock
	}
	return p, nil
}

// validateKey rejects keys that would escape the provider root via
// path traversal. It also rejects separators, empty IDs, and relative
// components. The gateway must only submit opaque, filesystem-safe IDs
// so this is a defence-in-depth check.
func validateKey(key string) error {
	if key == "" {
		return errors.New("local_fs_dev: key is required")
	}
	if strings.ContainsAny(key, `/\`) {
		return fmt.Errorf("local_fs_dev: key %q must not contain path separators", key)
	}
	if key == "." || key == ".." {
		return fmt.Errorf("local_fs_dev: key %q must not be a relative path component", key)
	}
	if strings.ContainsRune(key, 0) {
		return fmt.Errorf("local_fs_dev: key %q must not contain NUL bytes", key)
	}
	// Reject keys that collide with internal directories.
	if key == "_multipart" {
		return fmt.Errorf("local_fs_dev: key %q is reserved", key)
	}
	return nil
}

// sidecar holds the metadata persisted next to each object version.
type sidecar struct {
	Key             string                          `json:"key"`
	VersionID       string                          `json:"version_id"`
	SizeBytes       int64                           `json:"size_bytes"`
	ChecksumSHA256  string                          `json:"checksum_sha256"`
	ContentType     string                          `json:"content_type"`
	VersioningState blobstore.BucketVersioningState `json:"versioning_state"`
	Retention       blobstore.RetentionState        `json:"retention"`
	WriteTime       time.Time                       `json:"write_time"`
	IsDeleteMarker  bool                            `json:"is_delete_marker,omitempty"`
}

func (p *Provider) versionsDir(key string) string {
	return filepath.Join(p.root, key)
}

func (p *Provider) versionBodyPath(key, versionID string) string {
	return filepath.Join(p.versionsDir(key), versionID+".bin")
}

func (p *Provider) versionMetaPath(key, versionID string) string {
	return filepath.Join(p.versionsDir(key), versionID+".json")
}

// Put writes r to disk at {root}/{key}/{versionID}.bin and records a
// sidecar JSON next to it. A fresh synthetic version ID is generated
// for every Put so the adapter behaves like a versioned S3 bucket.
func (p *Provider) Put(ctx context.Context, req blobstore.PutRequest) (blobstore.PutResult, error) {
	if err := validateKey(req.Key); err != nil {
		return blobstore.PutResult{}, err
	}
	if req.Body == nil {
		return blobstore.PutResult{}, errors.New("local_fs_dev: body is required")
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if req.IfNoneMatch {
		if p.hasAnyVersionLocked(req.Key) {
			return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: %w: key %q already exists", blobstore.ErrPreconditionFailed, req.Key)
		}
	}

	if err := os.MkdirAll(p.versionsDir(req.Key), 0o755); err != nil {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: mkdir %q: %w", req.Key, err)
	}

	versionID := newVersionID()
	tmp, err := os.CreateTemp(p.versionsDir(req.Key), ".tmp-*")
	if err != nil {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: create temp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	hasher := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, hasher), req.Body)
	if err != nil {
		tmp.Close()
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: write %q: %w", req.Key, err)
	}
	if err := tmp.Close(); err != nil {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: close %q: %w", req.Key, err)
	}
	if req.ExpectedLength > 0 && n != req.ExpectedLength {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: %w: wrote %d want %d", blobstore.ErrChecksumMismatch, n, req.ExpectedLength)
	}
	sum := hex.EncodeToString(hasher.Sum(nil))
	if req.ChecksumSHA256 != "" && req.ChecksumSHA256 != sum {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: %w: got %s want %s", blobstore.ErrChecksumMismatch, sum, req.ChecksumSHA256)
	}
	bodyPath := p.versionBodyPath(req.Key, versionID)
	if err := os.Rename(tmpName, bodyPath); err != nil {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: finalize %q: %w", req.Key, err)
	}

	now := p.now()
	sc := sidecar{
		Key:             req.Key,
		VersionID:       versionID,
		SizeBytes:       n,
		ChecksumSHA256:  sum,
		ContentType:     req.ContentType,
		VersioningState: blobstore.VersioningEnabled,
		Retention: blobstore.RetentionState{
			Mode: effectiveRetentionMode(req.Retention),
		},
		WriteTime: now,
	}
	if req.Retention.Mode != blobstore.RetentionNone && !req.Retention.RetainUntil.IsZero() {
		sc.Retention.RetainUntil = req.Retention.RetainUntil
	}
	sc.Retention.LegalHold = req.Retention.LegalHold
	if err := writeSidecar(p.versionMetaPath(req.Key, versionID), sc); err != nil {
		_ = os.Remove(bodyPath)
		return blobstore.PutResult{}, err
	}

	return blobstore.PutResult{
		Key:             req.Key,
		VersionID:       versionID,
		VersioningState: blobstore.VersioningEnabled,
		Retention:       sc.Retention,
		WriteTime:       now,
		Size:            n,
		ChecksumSHA256:  sum,
		WriteResultHash: computeWriteResultHash(req.Key, versionID, blobstore.VersioningEnabled, n, sum),
	}, nil
}

// Head returns the metadata for the current (latest) version of key.
func (p *Provider) Head(ctx context.Context, ref blobstore.ObjectRef) (blobstore.ObjectMeta, error) {
	if err := validateKey(ref.Key); err != nil {
		return blobstore.ObjectMeta{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	versions, err := p.listVersionsLocked(ref.Key)
	if err != nil {
		return blobstore.ObjectMeta{}, err
	}
	current := latestVersion(versions)
	if current == nil || current.IsDeleteMarker {
		return blobstore.ObjectMeta{}, fmt.Errorf("local_fs_dev: %w: key %q", blobstore.ErrNotFound, ref.Key)
	}
	return sidecarToMeta(*current), nil
}

// Get returns a ReadCloser for the latest version (or a specific
// version when req.Ref.VersionID is set), honouring req.Range.
func (p *Provider) Get(ctx context.Context, req blobstore.GetRequest) (io.ReadCloser, blobstore.ObjectMeta, error) {
	if err := validateKey(req.Ref.Key); err != nil {
		return nil, blobstore.ObjectMeta{}, err
	}
	p.mu.Lock()
	var sc *sidecar
	if req.Ref.VersionID != "" {
		v, err := p.readSidecar(req.Ref.Key, req.Ref.VersionID)
		if err != nil {
			p.mu.Unlock()
			return nil, blobstore.ObjectMeta{}, err
		}
		sc = v
	} else {
		versions, err := p.listVersionsLocked(req.Ref.Key)
		if err != nil {
			p.mu.Unlock()
			return nil, blobstore.ObjectMeta{}, err
		}
		current := latestVersion(versions)
		if current == nil || current.IsDeleteMarker {
			p.mu.Unlock()
			return nil, blobstore.ObjectMeta{}, fmt.Errorf("local_fs_dev: %w: key %q", blobstore.ErrNotFound, req.Ref.Key)
		}
		sc = current
	}
	bodyPath := p.versionBodyPath(sc.Key, sc.VersionID)
	// Open the file while still holding the lock to prevent a TOCTOU
	// race where another goroutine deletes the version between unlock
	// and open. An open file descriptor stays valid on Unix even if
	// the file is unlinked after open.
	f, err := os.Open(bodyPath)
	p.mu.Unlock()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, blobstore.ObjectMeta{}, fmt.Errorf("local_fs_dev: %w: key %q", blobstore.ErrNotFound, req.Ref.Key)
		}
		return nil, blobstore.ObjectMeta{}, fmt.Errorf("local_fs_dev: open %q: %w", req.Ref.Key, err)
	}
	if req.Range == nil {
		return f, sidecarToMeta(*sc), nil
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, blobstore.ObjectMeta{}, fmt.Errorf("local_fs_dev: stat %q: %w", req.Ref.Key, err)
	}
	size := info.Size()
	start := req.Range.Start
	end := req.Range.End
	if end < 0 || end >= size {
		end = size - 1
	}
	if start < 0 || start > end {
		f.Close()
		return nil, blobstore.ObjectMeta{}, fmt.Errorf("local_fs_dev: invalid range [%d,%d] for %q size %d", req.Range.Start, req.Range.End, req.Ref.Key, size)
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		f.Close()
		return nil, blobstore.ObjectMeta{}, fmt.Errorf("local_fs_dev: seek %q: %w", req.Ref.Key, err)
	}
	return &limitedReadCloser{ReadCloser: f, R: io.LimitReader(f, end-start+1)}, sidecarToMeta(*sc), nil
}

type limitedReadCloser struct {
	io.ReadCloser
	R io.Reader
}

func (l *limitedReadCloser) Read(b []byte) (int, error) { return l.R.Read(b) }

// Delete places a delete marker on top of the version stack. The
// underlying versions remain until PurgeAllVersions or DeleteVersion
// removes them, mirroring S3 versioning semantics.
func (p *Provider) Delete(ctx context.Context, ref blobstore.ObjectRef) error {
	if err := validateKey(ref.Key); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.hasAnyVersionLocked(ref.Key) {
		return fmt.Errorf("local_fs_dev: %w: key %q", blobstore.ErrNotFound, ref.Key)
	}
	versionID := newVersionID()
	sc := sidecar{
		Key:             ref.Key,
		VersionID:       versionID,
		VersioningState: blobstore.VersioningEnabled,
		IsDeleteMarker:  true,
		WriteTime:       p.now(),
	}
	return writeSidecar(p.versionMetaPath(ref.Key, versionID), sc)
}

// PurgeAllVersions removes every version and delete marker for key.
func (p *Provider) PurgeAllVersions(ctx context.Context, ref blobstore.ObjectRef) error {
	if err := validateKey(ref.Key); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	dir := p.versionsDir(ref.Key)
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("local_fs_dev: stat %q: %w", ref.Key, err)
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("local_fs_dev: purge %q: %w", ref.Key, err)
	}
	return nil
}

// CreateMultipart initiates a multipart upload. Parts are staged
// under {root}/_multipart/{uploadID}/ and finalized on Complete.
func (p *Provider) CreateMultipart(ctx context.Context, req blobstore.MultipartRequest) (blobstore.MultipartUpload, error) {
	if err := validateKey(req.Key); err != nil {
		return blobstore.MultipartUpload{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	uploadID := newVersionID()
	dir := p.multipartDir(req.Key, uploadID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return blobstore.MultipartUpload{}, fmt.Errorf("local_fs_dev: mkdir multipart: %w", err)
	}
	meta := multipartMeta{
		Key:            req.Key,
		UploadID:       uploadID,
		ContentType:    req.ContentType,
		ChecksumSHA256: req.ChecksumSHA256,
		Retention:      req.Retention,
		StartedAt:      p.now(),
	}
	if err := writeJSON(filepath.Join(dir, "upload.json"), meta); err != nil {
		_ = os.RemoveAll(dir)
		return blobstore.MultipartUpload{}, err
	}
	return blobstore.MultipartUpload{Key: req.Key, UploadID: uploadID}, nil
}

// SignUploadPart returns a synthetic "signed" target pointing back at
// the local filesystem. Phase-1 product clients receive edge grants,
// not provider presigns; this is the seam tests use.
func (p *Provider) SignUploadPart(ctx context.Context, req blobstore.SignPartRequest) (blobstore.SignedRequest, error) {
	dir := p.multipartDir(req.Upload.Key, req.Upload.UploadID)
	if _, err := os.Stat(dir); err != nil {
		return blobstore.SignedRequest{}, fmt.Errorf("local_fs_dev: %w: upload %q", blobstore.ErrNotFound, req.Upload.UploadID)
	}
	return blobstore.SignedRequest{
		URL:       "file://" + filepath.Join(dir, fmt.Sprintf("part-%05d", req.PartNumber)),
		Headers:   map[string]string{},
		ExpiresAt: p.now().Add(15 * time.Minute),
	}, nil
}

// UploadPart writes one part to the staging directory. Used by the
// worker for server-side multipart promote.
func (p *Provider) UploadPart(ctx context.Context, req blobstore.UploadPartRequest) (blobstore.UploadedPart, error) {
	if err := validateKey(req.Upload.Key); err != nil {
		return blobstore.UploadedPart{}, err
	}
	if req.PartNumber < 1 {
		return blobstore.UploadedPart{}, errors.New("local_fs_dev: part number must be >= 1")
	}
	if req.Body == nil {
		return blobstore.UploadedPart{}, errors.New("local_fs_dev: part body is required")
	}
	dir := p.multipartDir(req.Upload.Key, req.Upload.UploadID)
	if _, err := os.Stat(dir); err != nil {
		return blobstore.UploadedPart{}, fmt.Errorf("local_fs_dev: %w: upload %q", blobstore.ErrNotFound, req.Upload.UploadID)
	}
	partPath := filepath.Join(dir, fmt.Sprintf("part-%05d", req.PartNumber))
	f, err := os.Create(partPath)
	if err != nil {
		return blobstore.UploadedPart{}, fmt.Errorf("local_fs_dev: create part: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	w := io.MultiWriter(f, h)
	n, err := io.Copy(w, req.Body)
	if err != nil {
		return blobstore.UploadedPart{}, fmt.Errorf("local_fs_dev: write part: %w", err)
	}
	return blobstore.UploadedPart{
		PartNumber:     req.PartNumber,
		ChecksumSHA256: hex.EncodeToString(h.Sum(nil)),
		Size:           n,
	}, nil
}

// CompleteMultipart assembles the staged parts into a new immutable
// version and removes the staging directory.
func (p *Provider) CompleteMultipart(ctx context.Context, req blobstore.CompleteRequest) (blobstore.PutResult, error) {
	if err := validateKey(req.Upload.Key); err != nil {
		return blobstore.PutResult{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	dir := p.multipartDir(req.Upload.Key, req.Upload.UploadID)
	metaPath := filepath.Join(dir, "upload.json")
	var mp multipartMeta
	if err := readJSON(metaPath, &mp); err != nil {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: %w: upload %q", blobstore.ErrNotFound, req.Upload.UploadID)
	}
	parts := make([]string, 0, len(req.Parts))
	for _, part := range req.Parts {
		parts = append(parts, filepath.Join(dir, fmt.Sprintf("part-%05d", part.PartNumber)))
	}
	if err := os.MkdirAll(p.versionsDir(req.Upload.Key), 0o755); err != nil {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: mkdir versions: %w", err)
	}
	versionID := newVersionID()
	out, err := os.CreateTemp(p.versionsDir(req.Upload.Key), ".tmp-*")
	if err != nil {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: temp: %w", err)
	}
	tmpName := out.Name()
	defer os.Remove(tmpName)
	hasher := sha256.New()
	var total int64
	for _, partPath := range parts {
		f, err := os.Open(partPath)
		if err != nil {
			out.Close()
			return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: open part: %w", err)
		}
		n, err := io.Copy(io.MultiWriter(out, hasher), f)
		f.Close()
		if err != nil {
			out.Close()
			return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: copy part: %w", err)
		}
		total += n
	}
	if err := out.Close(); err != nil {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: close: %w", err)
	}
	sum := hex.EncodeToString(hasher.Sum(nil))
	if mp.ChecksumSHA256 != "" && mp.ChecksumSHA256 != sum {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: %w: got %s want %s", blobstore.ErrChecksumMismatch, sum, mp.ChecksumSHA256)
	}
	bodyPath := p.versionBodyPath(req.Upload.Key, versionID)
	if err := os.Rename(tmpName, bodyPath); err != nil {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: finalize: %w", err)
	}
	now := p.now()
	sc := sidecar{
		Key:             req.Upload.Key,
		VersionID:       versionID,
		SizeBytes:       total,
		ChecksumSHA256:  sum,
		ContentType:     mp.ContentType,
		VersioningState: blobstore.VersioningEnabled,
		Retention: blobstore.RetentionState{
			Mode: effectiveRetentionMode(req.Retention),
		},
		WriteTime: now,
	}
	sc.Retention.LegalHold = req.Retention.LegalHold
	if req.Retention.Mode != blobstore.RetentionNone && !req.Retention.RetainUntil.IsZero() {
		sc.Retention.RetainUntil = req.Retention.RetainUntil
	}
	if err := writeSidecar(p.versionMetaPath(req.Upload.Key, versionID), sc); err != nil {
		_ = os.Remove(bodyPath)
		return blobstore.PutResult{}, err
	}
	_ = os.RemoveAll(dir)
	return blobstore.PutResult{
		Key:             req.Upload.Key,
		VersionID:       versionID,
		VersioningState: blobstore.VersioningEnabled,
		Retention:       sc.Retention,
		WriteTime:       now,
		Size:            total,
		ChecksumSHA256:  sum,
		WriteResultHash: computeWriteResultHash(req.Upload.Key, versionID, blobstore.VersioningEnabled, total, sum),
	}, nil
}

// AbortMultipart removes the staging directory.
func (p *Provider) AbortMultipart(ctx context.Context, upload blobstore.MultipartUpload) error {
	if err := validateKey(upload.Key); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	dir := p.multipartDir(upload.Key, upload.UploadID)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("local_fs_dev: abort multipart: %w", err)
	}
	return nil
}

// CopyWithinProvider copies one version to a new key.
func (p *Provider) CopyWithinProvider(ctx context.Context, req blobstore.CopyWithinProviderRequest) (blobstore.PutResult, error) {
	if err := validateKey(req.Src.Key); err != nil {
		return blobstore.PutResult{}, err
	}
	if err := validateKey(req.DstKey); err != nil {
		return blobstore.PutResult{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var srcSC *sidecar
	if req.Src.VersionID != "" {
		v, err := p.readSidecar(req.Src.Key, req.Src.VersionID)
		if err != nil {
			return blobstore.PutResult{}, err
		}
		srcSC = v
	} else {
		versions, err := p.listVersionsLocked(req.Src.Key)
		if err != nil {
			return blobstore.PutResult{}, err
		}
		srcSC = latestNonDeleteMarker(versions)
		if srcSC == nil {
			return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: %w: src %q", blobstore.ErrNotFound, req.Src.Key)
		}
	}
	srcBody, err := os.Open(p.versionBodyPath(srcSC.Key, srcSC.VersionID))
	if err != nil {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: open src: %w", err)
	}
	defer srcBody.Close()
	// Re-entrant Put would re-lock; inline the write.
	if err := os.MkdirAll(p.versionsDir(req.DstKey), 0o755); err != nil {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: mkdir dst: %w", err)
	}
	versionID := newVersionID()
	tmp, err := os.CreateTemp(p.versionsDir(req.DstKey), ".tmp-*")
	if err != nil {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: temp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	hasher := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, hasher), srcBody)
	if err != nil {
		tmp.Close()
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: copy: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: close: %w", err)
	}
	sum := hex.EncodeToString(hasher.Sum(nil))
	if sum != srcSC.ChecksumSHA256 {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: %w: copy checksum", blobstore.ErrChecksumMismatch)
	}
	bodyPath := p.versionBodyPath(req.DstKey, versionID)
	if err := os.Rename(tmpName, bodyPath); err != nil {
		return blobstore.PutResult{}, fmt.Errorf("local_fs_dev: finalize copy: %w", err)
	}
	now := p.now()
	dstSC := sidecar{
		Key:             req.DstKey,
		VersionID:       versionID,
		SizeBytes:       n,
		ChecksumSHA256:  sum,
		ContentType:     srcSC.ContentType,
		VersioningState: blobstore.VersioningEnabled,
		Retention:       blobstore.RetentionState{Mode: effectiveRetentionMode(req.Retention), LegalHold: req.Retention.LegalHold},
		WriteTime:       now,
	}
	if req.Retention.Mode != blobstore.RetentionNone && !req.Retention.RetainUntil.IsZero() {
		dstSC.Retention.RetainUntil = req.Retention.RetainUntil
	}
	if err := writeSidecar(p.versionMetaPath(req.DstKey, versionID), dstSC); err != nil {
		_ = os.Remove(bodyPath)
		return blobstore.PutResult{}, err
	}
	return blobstore.PutResult{
		Key:             req.DstKey,
		VersionID:       versionID,
		VersioningState: blobstore.VersioningEnabled,
		Retention:       dstSC.Retention,
		WriteTime:       now,
		Size:            n,
		ChecksumSHA256:  sum,
		WriteResultHash: computeWriteResultHash(req.DstKey, versionID, blobstore.VersioningEnabled, n, sum),
	}, nil
}

// GetRetention returns the retention state of a specific version.
func (p *Provider) GetRetention(ctx context.Context, ref blobstore.VersionedObjectRef) (blobstore.RetentionState, error) {
	if err := validateKey(ref.Key); err != nil {
		return blobstore.RetentionState{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	sc, err := p.readSidecar(ref.Key, ref.VersionID)
	if err != nil {
		return blobstore.RetentionState{}, err
	}
	return sc.Retention, nil
}

// UpdateRetention permits monotonic extension and a separately
// authorized clear. It never silently shortens a retention period.
func (p *Provider) UpdateRetention(ctx context.Context, req blobstore.RetentionUpdate) (blobstore.RetentionState, error) {
	if err := validateKey(req.Ref.Key); err != nil {
		return blobstore.RetentionState{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	sc, err := p.readSidecar(req.Ref.Key, req.Ref.VersionID)
	if err != nil {
		return blobstore.RetentionState{}, err
	}
	if sc.Retention.Mode == blobstore.RetentionCompliance {
		// Compliance mode: cannot be cleared, and the retain-until date
		// can only be extended forward, never shortened. This mirrors
		// S3 Object Lock compliance semantics.
		if req.ClearRetention {
			return blobstore.RetentionState{}, fmt.Errorf("local_fs_dev: %w: compliance lock cannot be cleared", blobstore.ErrRetentionConflict)
		}
		if !req.RetainUntil.IsZero() {
			if !sc.Retention.RetainUntil.IsZero() && req.RetainUntil.Before(sc.Retention.RetainUntil) {
				return blobstore.RetentionState{}, fmt.Errorf("local_fs_dev: %w: compliance retain-until cannot be shortened", blobstore.ErrRetentionConflict)
			}
			sc.Retention.RetainUntil = req.RetainUntil
		}
		// Mode cannot be downgraded from compliance.
		if req.Mode != blobstore.RetentionNone && req.Mode != blobstore.RetentionCompliance {
			return blobstore.RetentionState{}, fmt.Errorf("local_fs_dev: %w: compliance mode cannot be downgraded", blobstore.ErrRetentionConflict)
		}
	} else if req.ClearRetention {
		sc.Retention = blobstore.RetentionState{Mode: blobstore.RetentionNone}
	} else {
		if req.Mode != blobstore.RetentionNone {
			sc.Retention.Mode = req.Mode
		}
		if !req.RetainUntil.IsZero() {
			if !sc.Retention.RetainUntil.IsZero() && req.RetainUntil.Before(sc.Retention.RetainUntil) {
				return blobstore.RetentionState{}, fmt.Errorf("local_fs_dev: %w: cannot shorten retention", blobstore.ErrRetentionConflict)
			}
			sc.Retention.RetainUntil = req.RetainUntil
		}
	}
	if err := writeSidecar(p.versionMetaPath(req.Ref.Key, req.Ref.VersionID), *sc); err != nil {
		return blobstore.RetentionState{}, err
	}
	return sc.Retention, nil
}

// SetLegalHold toggles the legal-hold flag on a version. Removal
// requires case closure and dual approval (enforced upstream).
func (p *Provider) SetLegalHold(ctx context.Context, ref blobstore.VersionedObjectRef, enabled bool) (blobstore.RetentionState, error) {
	if err := validateKey(ref.Key); err != nil {
		return blobstore.RetentionState{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	sc, err := p.readSidecar(ref.Key, ref.VersionID)
	if err != nil {
		return blobstore.RetentionState{}, err
	}
	sc.Retention.LegalHold = enabled
	if err := writeSidecar(p.versionMetaPath(ref.Key, ref.VersionID), *sc); err != nil {
		return blobstore.RetentionState{}, err
	}
	return sc.Retention, nil
}

// Capabilities reports the local_fs_dev envelope. It mirrors a
// versioned S3 bucket so contract tests exercise the full surface.
func (p *Provider) Capabilities(ctx context.Context) blobstore.ProviderCapabilities {
	return blobstore.ProviderCapabilities{
		ProviderVersioning:        true,
		DeleteAllVersions:         true,
		ConditionalPut:            true,
		NativeSHA256Checksum:      true,
		S3ObjectLock:              true,
		PerVersionRetentionGet:    true,
		PerVersionRetentionExtend: true,
		PerVersionRetentionClear:  true,
		LegalHoldSetClear:         true,
		GovernanceBypassDenied:    true,
		ListAndAbortMultipart:     true,
		BucketVersioningState:     blobstore.VersioningEnabled,
		MaxObjectSize:             5 * 1024 * 1024 * 1024 * 1024,
		MaxPartSize:               5 * 1024 * 1024 * 1024,
		MaxParts:                  10000,
		MinPartBytes:              5 * 1024 * 1024,
		ChecksumOnComplete:        true,
	}
}

// --- BlobInventory ---

// ListObjects paginates object keys under prefix.
func (p *Provider) ListObjects(ctx context.Context, req blobstore.ListObjectsRequest) (blobstore.ObjectPage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entries, err := os.ReadDir(p.root)
	if err != nil {
		return blobstore.ObjectPage{}, fmt.Errorf("local_fs_dev: list root: %w", err)
	}
	var metas []blobstore.ObjectMeta
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		key := e.Name()
		if req.Prefix != "" && !strings.HasPrefix(key, req.Prefix) {
			continue
		}
		if req.Cursor != "" && key <= req.Cursor {
			continue
		}
		versions, err := p.listVersionsLocked(key)
		if err != nil {
			continue
		}
		latest := latestNonDeleteMarker(versions)
		if latest != nil {
			metas = append(metas, sidecarToMeta(*latest))
		}
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].Key < metas[j].Key })
	max := int(req.MaxKeys)
	if max <= 0 {
		max = 1000
	}
	if max > 1000 {
		max = 1000
	}
	if max > len(metas) {
		max = len(metas)
	}
	page := metas[:max]
	out := blobstore.ObjectPage{Objects: page}
	if max < len(metas) {
		out.NextCursor = page[len(page)-1].Key
		out.IsTruncated = true
	}
	return out, nil
}

// ListObjectVersions paginates versions of one object.
func (p *Provider) ListObjectVersions(ctx context.Context, req blobstore.ListVersionsRequest) (blobstore.VersionPage, error) {
	if err := validateKey(req.Key); err != nil {
		return blobstore.VersionPage{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	versions, err := p.listVersionsLocked(req.Key)
	if err != nil {
		return blobstore.VersionPage{}, err
	}
	metas := make([]blobstore.ObjectMeta, 0, len(versions))
	for _, v := range versions {
		metas = append(metas, sidecarToMeta(*v))
	}
	return blobstore.VersionPage{Versions: metas}, nil
}

// DeleteVersion removes a specific version.
func (p *Provider) DeleteVersion(ctx context.Context, ref blobstore.VersionedObjectRef) error {
	if err := validateKey(ref.Key); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := os.Remove(p.versionMetaPath(ref.Key, ref.VersionID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("local_fs_dev: delete version meta: %w", err)
	}
	if err := os.Remove(p.versionBodyPath(ref.Key, ref.VersionID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("local_fs_dev: delete version body: %w", err)
	}
	return nil
}

// ListMultipartUploads paginates in-progress multipart uploads. The
// cursor is the "key/uploadID" of the last upload returned in the
// previous page; uploads are sorted by (key, uploadID) and those
// <= the cursor are skipped.
func (p *Provider) ListMultipartUploads(ctx context.Context, req blobstore.ListUploadsRequest) (blobstore.UploadPage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	root := filepath.Join(p.root, "_multipart")
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return blobstore.UploadPage{}, nil
		}
		return blobstore.UploadPage{}, fmt.Errorf("local_fs_dev: list multipart: %w", err)
	}
	var uploads []blobstore.MultipartUpload
	for _, keyDir := range entries {
		if !keyDir.IsDir() {
			continue
		}
		key := keyDir.Name()
		if req.Prefix != "" && !strings.HasPrefix(key, req.Prefix) {
			continue
		}
		uploadDirs, err := os.ReadDir(filepath.Join(root, key))
		if err != nil {
			continue
		}
		for _, ud := range uploadDirs {
			if !ud.IsDir() {
				continue
			}
			uploads = append(uploads, blobstore.MultipartUpload{Key: key, UploadID: ud.Name()})
		}
	}
	// Sort by (key, uploadID) for stable pagination.
	sort.Slice(uploads, func(i, j int) bool {
		if uploads[i].Key != uploads[j].Key {
			return uploads[i].Key < uploads[j].Key
		}
		return uploads[i].UploadID < uploads[j].UploadID
	})
	// Filter out entries <= cursor.
	if req.Cursor != "" {
		filtered := uploads[:0]
		for _, u := range uploads {
			cursor := u.Key + "/" + u.UploadID
			if cursor > req.Cursor {
				filtered = append(filtered, u)
			}
		}
		uploads = filtered
	}
	// Apply MaxKeys limit.
	max := int(req.MaxKeys)
	if max > 0 && len(uploads) > max {
		page := uploads[:max]
		last := page[len(page)-1]
		return blobstore.UploadPage{
			Uploads:    page,
			NextCursor: last.Key + "/" + last.UploadID,
		}, nil
	}
	return blobstore.UploadPage{Uploads: uploads}, nil
}

// ListParts paginates parts of one multipart upload. The cursor is
// the part number after which to resume; parts are sorted by
// PartNumber and those <= the cursor are skipped.
func (p *Provider) ListParts(ctx context.Context, upload blobstore.MultipartUpload, cursor string) (blobstore.PartPage, error) {
	dir := p.multipartDir(upload.Key, upload.UploadID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return blobstore.PartPage{}, nil
		}
		return blobstore.PartPage{}, fmt.Errorf("local_fs_dev: list parts: %w", err)
	}
	var parts []blobstore.CompletedPart
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "part-") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		var partNum int32
		if _, err := fmt.Sscanf(name, "part-%05d", &partNum); err != nil {
			continue
		}
		parts = append(parts, blobstore.CompletedPart{PartNumber: partNum, Size: info.Size()})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	// Filter out parts with PartNumber <= cursor.
	if cursor != "" {
		cursorNum, _ := strconv.Atoi(cursor)
		filtered := parts[:0]
		for _, pt := range parts {
			if int(pt.PartNumber) > cursorNum {
				filtered = append(filtered, pt)
			}
		}
		parts = filtered
	}
	return blobstore.PartPage{Parts: parts}, nil
}

// DeleteBatch removes a batch of versions.
func (p *Provider) DeleteBatch(ctx context.Context, refs []blobstore.VersionedObjectRef) (blobstore.BatchDeleteResult, error) {
	result := blobstore.BatchDeleteResult{}
	for _, ref := range refs {
		if err := p.DeleteVersion(ctx, ref); err != nil {
			result.Errors = append(result.Errors, blobstore.BatchDeleteError{Ref: ref, Error: err.Error()})
		} else {
			result.Deleted = append(result.Deleted, ref)
		}
	}
	return result, nil
}

// --- helpers ---

func (p *Provider) multipartDir(key, uploadID string) string {
	return filepath.Join(p.root, "_multipart", key, uploadID)
}

func (p *Provider) hasAnyVersionLocked(key string) bool {
	dir := p.versionsDir(key)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") {
			return true
		}
	}
	return false
}

func (p *Provider) listVersionsLocked(key string) ([]*sidecar, error) {
	dir := p.versionsDir(key)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("local_fs_dev: read versions %q: %w", key, err)
	}
	var out []*sidecar
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		sc, err := loadSidecar(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		out = append(out, sc)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WriteTime.Before(out[j].WriteTime) })
	return out, nil
}

func (p *Provider) readSidecar(key, versionID string) (*sidecar, error) {
	sc, err := loadSidecar(p.versionMetaPath(key, versionID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("local_fs_dev: %w: key %q version %q", blobstore.ErrNotFound, key, versionID)
		}
		return nil, err
	}
	return sc, nil
}

// latestVersion returns the most recent version (including delete
// markers). S3 semantics: when the current version is a delete
// marker, Head/Get without a version ID return 404.
func latestVersion(versions []*sidecar) *sidecar {
	if len(versions) == 0 {
		return nil
	}
	return versions[len(versions)-1]
}

func latestNonDeleteMarker(versions []*sidecar) *sidecar {
	for i := len(versions) - 1; i >= 0; i-- {
		if !versions[i].IsDeleteMarker {
			return versions[i]
		}
	}
	return nil
}

func sidecarToMeta(sc sidecar) blobstore.ObjectMeta {
	return blobstore.ObjectMeta{
		Key:             sc.Key,
		VersionID:       sc.VersionID,
		VersioningState: sc.VersioningState,
		Size:            sc.SizeBytes,
		ChecksumSHA256:  sc.ChecksumSHA256,
		Retention:       sc.Retention,
		WriteTime:       sc.WriteTime,
	}
}

func effectiveRetentionMode(spec blobstore.RetentionSpec) blobstore.RetentionMode {
	if spec.Mode == "" {
		return blobstore.RetentionNone
	}
	return spec.Mode
}

func writeSidecar(path string, sc sidecar) error {
	return writeJSON(path, sc)
}

func writeJSON(path string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("local_fs_dev: marshal: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return fmt.Errorf("local_fs_dev: write %q: %w", path, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("local_fs_dev: publish %q: %w", path, err)
	}
	return nil
}

func readJSON(path string, v any) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, v)
}

func loadSidecar(path string) (*sidecar, error) {
	var sc sidecar
	if err := readJSON(path, &sc); err != nil {
		return nil, err
	}
	return &sc, nil
}

type multipartMeta struct {
	Key            string                  `json:"key"`
	UploadID       string                  `json:"upload_id"`
	ContentType    string                  `json:"content_type"`
	ChecksumSHA256 string                  `json:"checksum_sha256"`
	Retention      blobstore.RetentionSpec `json:"retention"`
	StartedAt      time.Time               `json:"started_at"`
}

// versionCounter is an atomic counter appended to the timestamp so
// two version IDs generated in the same nanosecond still differ.
var versionCounter uint64

// newVersionID returns a sortable unique ID mimicking S3 version IDs.
// The format is {unixNano:016x}-{counter:08x} so IDs sort
// chronologically while remaining collision-free under contention.
func newVersionID() string {
	nano := uint64(time.Now().UnixNano())
	seq := atomic.AddUint64(&versionCounter, 1)
	return fmt.Sprintf("%016x-%08x", nano, seq)
}

// computeWriteResultHash binds provider, account, bucket, immutable
// key, versioning state, size, checksum, and version ID into a single
// canonical hash. For the local adapter the provider/account/bucket
// are fixed.
func computeWriteResultHash(key, versionID string, state blobstore.BucketVersioningState, size int64, sum string) string {
	h := sha256.New()
	fmt.Fprintf(h, "local_fs_dev/v1\x00%s\x00%s\x00%s\x00%d\x00%s", key, versionID, state, size, sum)
	return hex.EncodeToString(h.Sum(nil))
}

var (
	_ blobstore.BlobStore     = (*Provider)(nil)
	_ blobstore.BlobInventory = (*Provider)(nil)
)
