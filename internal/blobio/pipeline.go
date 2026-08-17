// Package blobio implements the L1-cache-aside write and read paths
// that sit between the gateway HTTP handlers and the BlobStore
// durable origin.
//
// Write path (plan §3):
//
//	client → gateway → L1 cache (immediate) → async Wasabi promotion
//	→ COMMITTED_DURABLE once the Wasabi PUT is verified.
//
// Read path (plan §3):
//
//	L1 hit → return. L1 miss → L2 singleflight restore, stream-while-
//	cache. The singleflight collapses concurrent reads of the same
//	blob so only one Wasabi GET is issued per blob per gateway
//	replica.
//
// The cache stores ciphertext, keyed by immutable blob ID. Auth is
// checked before cache lookup; cache by immutable blob ID,
// independent of bearer token (§15.4).
//
// Promote path:
//
//	Promote is deduplicated via singleflight (only one concurrent
//	promote per blob). Before uploading, Promote HEADs the durable
//	store to check if the blob already exists (crash recovery
//	idempotency). For blobs ≤ promoteMultipartThreshold the body is
//	streamed through a hashing reader so the full body is never
//	buffered in memory. For larger blobs, parts are uploaded in
//	parallel via a configurable worker pool.
package blobio

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kchat/drive/pkg/blobstore"
	"github.com/kchat/drive/pkg/hotcache"
)

// CommitState tracks where a blob's bytes currently live.
type CommitState string

const (
	// CommitCached means the blob is in L1 only and has not yet been
	// promoted to the durable origin.
	CommitCached CommitState = "CACHED"
	// CommitDurable means the blob has been verified in Wasabi (length
	// + SHA-256 match) and is safe to evict from L1.
	CommitDurable CommitState = "COMMITTED_DURABLE"
)

// BlobStatus is the gateway's view of a blob's placement.
type BlobStatus struct {
	BlobID         string
	CommitState    CommitState
	Size           int64
	ChecksumSHA256 string
	VersionID      string
	CachedAt       time.Time
	DurableAt      time.Time
	// FileID and TenantID identify the owning file and tenant. They
	// are required by the Postgres-backed StatusStore so it can
	// satisfy the file_versions FK constraints; the in-memory store
	// ignores them.
	FileID   string
	TenantID string
	// Retention is the retention spec to forward to the durable origin
	// on promotion. It is set by Write from WriteOptions.Retention
	// and used by Promote so locked writes keep their retention.
	Retention blobstore.RetentionSpec
}

// WriteResult is returned by the write path.
type WriteResult struct {
	BlobID         string
	CommitState    CommitState
	Size           int64
	ChecksumSHA256 string
}

// ReadResult is returned by the read path.
type ReadResult struct {
	Body           io.ReadCloser
	Size           int64
	ChecksumSHA256 string
	VersionID      string
	FromCache      bool
}

// Store is the subset of BlobStore the data plane uses.
type Store interface {
	Put(ctx context.Context, req blobstore.PutRequest) (blobstore.PutResult, error)
	Get(ctx context.Context, req blobstore.GetRequest) (io.ReadCloser, blobstore.ObjectMeta, error)
	Head(ctx context.Context, ref blobstore.ObjectRef) (blobstore.ObjectMeta, error)

	// Multipart methods for large-blob promote.
	CreateMultipart(ctx context.Context, req blobstore.MultipartRequest) (blobstore.MultipartUpload, error)
	UploadPart(ctx context.Context, req blobstore.UploadPartRequest) (blobstore.UploadedPart, error)
	CompleteMultipart(ctx context.Context, req blobstore.CompleteRequest) (blobstore.PutResult, error)
	AbortMultipart(ctx context.Context, upload blobstore.MultipartUpload) error
}

// StatusStore persists blob placement status (CACHED vs
// COMMITTED_DURABLE). The pipeline uses it so that a gateway restart
// does not lose in-flight CACHED blobs. Tests use MemoryStatusStore;
// production uses a Postgres-backed implementation.
type StatusStore interface {
	Get(ctx context.Context, blobID string) (*BlobStatus, error)
	Put(ctx context.Context, status *BlobStatus) error
	// MarkDurable updates an existing blob placement to
	// COMMITTED_DURABLE with the provider-verified metadata. It is a
	// targeted UPDATE (no INSERT branch) used by the promote path
	// after a successful Wasabi PUT. The row must already exist (it
	// was created by Put when the blob was first cached).
	MarkDurable(ctx context.Context, blobKey, blobVersionID string, size int64, checksum string, durableAt time.Time) error
}

// MemoryStatusStore is an in-memory StatusStore for tests and dev mode.
type MemoryStatusStore struct {
	mu sync.RWMutex
	m  map[string]*BlobStatus
}

// NewMemoryStatusStore returns a new empty MemoryStatusStore.
func NewMemoryStatusStore() *MemoryStatusStore {
	return &MemoryStatusStore{m: map[string]*BlobStatus{}}
}

// Get returns the status for blobID, or nil if not found.
func (s *MemoryStatusStore) Get(_ context.Context, blobID string) (*BlobStatus, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.m[blobID], nil
}

// Put stores or replaces the status for the given blob.
func (s *MemoryStatusStore) Put(_ context.Context, status *BlobStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[status.BlobID] = status
	return nil
}

// MarkDurable updates an existing blob placement to COMMITTED_DURABLE.
// If the blob doesn't exist, it's a no-op (the promote path already
// checked existence via Get).
func (s *MemoryStatusStore) MarkDurable(_ context.Context, blobKey, blobVersionID string, size int64, checksum string, durableAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.m[blobKey]
	if !ok {
		return nil
	}
	existing.CommitState = CommitDurable
	existing.VersionID = blobVersionID
	existing.Size = size
	existing.ChecksumSHA256 = checksum
	existing.DurableAt = durableAt
	return nil
}

// Pipeline is the L1+L2 read/write pipeline.
type Pipeline struct {
	cache           hotcache.Cache
	store           Store
	status          StatusStore
	logger          *slog.Logger
	maxRestoreBytes int64 // 0 = unlimited

	// inflight deduplicates concurrent L2 restores of the same blob.
	inflight sync.Map // map[string]*inflightCall

	// promoteInflight deduplicates concurrent Promote calls for the
	// same blob so only one Wasabi PUT is issued per blob.
	promoteInflight sync.Map // map[string]*promoteCall

	// multipartParallelism is the number of concurrent part uploads
	// for promoteMultipart. 0 means sequential (backward compat).
	multipartParallelism int

	// lifecycle governs background goroutines (async cache puts on
	// the read path). Close cancels it and waits for goroutines.
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	wg              sync.WaitGroup

	// Metrics counters (atomic, exposed via Stats).
	promoteAttempts   atomic.Int64
	promoteSuccesses  atomic.Int64
	promoteFailures   atomic.Int64
	promoteSkipped    atomic.Int64 // HEAD found already-durable
	promoteDurationNs atomic.Int64 // sum of promote durations
}

type inflightCall struct {
	done chan struct{}
	err  error
	body []byte
	meta blobstore.ObjectMeta
}

type promoteCall struct {
	done chan struct{}
	err  error
}

// New returns a Pipeline backed by cache (L1) and store (L2). The
// status store defaults to an in-memory map; production callers
// should use NewWithStatusStore to inject a Postgres-backed store.
func New(cache hotcache.Cache, store Store, logger *slog.Logger) *Pipeline {
	return NewWithStatusStore(cache, store, NewMemoryStatusStore(), logger)
}

// NewWithStatusStore returns a Pipeline with an explicit StatusStore.
// Use this in production to wire the Postgres metadata store so blob
// status survives gateway restarts.
func NewWithStatusStore(cache hotcache.Cache, store Store, status StatusStore, logger *slog.Logger) *Pipeline {
	if logger == nil {
		logger = slog.Default()
	}
	if status == nil {
		status = NewMemoryStatusStore()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Pipeline{
		cache:                cache,
		store:                store,
		status:               status,
		logger:               logger,
		maxRestoreBytes:      DefaultMaxRestoreBytes,
		multipartParallelism: DefaultMultipartParallelism,
		lifecycleCtx:         ctx,
		lifecycleCancel:      cancel,
	}
}

// DefaultMaxRestoreBytes is the default cap on a single L2 restore
// (256 MiB). Blobs larger than this must be served via a streaming
// path that bypasses the singleflight buffer.
const DefaultMaxRestoreBytes = 256 * 1024 * 1024

// DefaultMultipartParallelism is the default number of concurrent
// part uploads for promoteMultipart.
const DefaultMultipartParallelism = 4

// SetMaxRestoreBytes overrides the L2 restore size limit. Set to 0
// for unlimited (not recommended in production).
func (p *Pipeline) SetMaxRestoreBytes(n int64) {
	p.maxRestoreBytes = n
}

// SetMultipartParallelism sets the number of concurrent part uploads
// for promoteMultipart. Set to 0 for sequential uploads.
func (p *Pipeline) SetMultipartParallelism(n int) {
	p.multipartParallelism = n
}

// Write stores blob body in L1 immediately and marks it CACHED. The
// async promoter (drive-worker) later promotes it to Wasabi and
// flips the status to COMMITTED_DURABLE. If PromoteSync is true the
// write blocks until Wasabi confirms the PUT.
func (p *Pipeline) Write(ctx context.Context, blobID string, body []byte, opts WriteOptions) (WriteResult, error) {
	if blobID == "" {
		return WriteResult{}, errors.New("blobio: blob_id is required")
	}
	sum := sha256.Sum256(body)
	checksum := hex.EncodeToString(sum[:])

	if err := p.cache.Put(ctx, blobID, bytes.NewReader(body), hotcache.PutOptions{
		SizeBytes:    int64(len(body)),
		Hash:         checksum,
		PinHot:       opts.PinHot,
		NonEvictable: true, // CACHED blob is the only copy; protect from eviction
	}); err != nil {
		return WriteResult{}, fmt.Errorf("blobio: cache put: %w", err)
	}

	now := time.Now().UTC()
	if err := p.status.Put(ctx, &BlobStatus{
		BlobID:         blobID,
		CommitState:    CommitCached,
		Size:           int64(len(body)),
		ChecksumSHA256: checksum,
		CachedAt:       now,
		FileID:         opts.FileID,
		TenantID:       opts.TenantID,
		Retention:      opts.Retention,
	}); err != nil {
		return WriteResult{}, fmt.Errorf("blobio: status put: %w", err)
	}

	commitState := CommitCached
	if opts.PromoteSync {
		if err := p.Promote(ctx, blobID); err != nil {
			return WriteResult{}, fmt.Errorf("blobio: sync promote: %w", err)
		}
		commitState = CommitDurable
	}

	return WriteResult{
		BlobID:         blobID,
		CommitState:    commitState,
		Size:           int64(len(body)),
		ChecksumSHA256: checksum,
	}, nil
}

// WriteOptions carries per-write hints.
type WriteOptions struct {
	// PromoteSync, when true, blocks the write until Wasabi confirms
	// the PUT. Default is async promotion (plan §3).
	PromoteSync bool
	// PinHot pins the blob in the cache's hot region.
	PinHot bool
	// Retention is forwarded to the durable PUT on promotion.
	Retention blobstore.RetentionSpec
	// FileID and TenantID identify the owning file and tenant. They
	// are required when the StatusStore is Postgres-backed (to
	// satisfy file_versions FK constraints). The in-memory store
	// ignores them.
	FileID   string
	TenantID string
}

// Read serves a blob from L1 if present; otherwise it restores from
// L2 via singleflight and caches the result before returning.
//
// For L2 restores, the singleflight pattern buffers the body (so
// concurrent waiters get the same data), but the cache put and status
// update happen asynchronously after the body is returned to the
// caller. This reduces first-byte latency for cold reads: the caller
// gets the body immediately after the L2 GET completes, without
// waiting for the cache write.
func (p *Pipeline) Read(ctx context.Context, blobID string) (ReadResult, error) {
	if blobID == "" {
		return ReadResult{}, errors.New("blobio: blob_id is required")
	}
	// L1 hit.
	r, _, err := p.cache.Get(ctx, blobID)
	if err == nil {
		status, _ := p.status.Get(ctx, blobID)
		result := ReadResult{
			Body:      r,
			FromCache: true,
		}
		if status != nil {
			result.Size = status.Size
			result.ChecksumSHA256 = status.ChecksumSHA256
			result.VersionID = status.VersionID
		}
		return result, nil
	}
	if !errors.Is(err, hotcache.ErrCacheMiss) {
		return ReadResult{}, fmt.Errorf("blobio: cache get: %w", err)
	}

	// L1 miss → L2 restore via singleflight.
	body, meta, err := p.restoreFromL2(ctx, blobID)
	if err != nil {
		return ReadResult{}, err
	}

	// Cache the restored body and update status asynchronously so
	// the caller gets the body without waiting for the cache write.
	// The body is already in memory (buffered by singleflight), so
	// the goroutine just copies it into the cache. The goroutine is
	// tracked by the pipeline's WaitGroup and uses the lifecycle
	// context so it is cancelled on Close.
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		bgCtx := p.lifecycleCtx
		if bgCtx.Err() != nil {
			return
		}
		if err := p.cache.Put(bgCtx, blobID, bytes.NewReader(body), hotcache.PutOptions{
			SizeBytes:    meta.Size,
			Hash:         meta.ChecksumSHA256,
			NonEvictable: false, // restored from L2, already durable
		}); err != nil {
			p.logger.Warn("blobio: cache put on restore failed",
				slog.String("blob_id", blobID), slog.Any("err", err))
		}
		if err := p.status.Put(bgCtx, &BlobStatus{
			BlobID:         blobID,
			CommitState:    CommitDurable,
			Size:           meta.Size,
			ChecksumSHA256: meta.ChecksumSHA256,
			VersionID:      meta.VersionID,
			DurableAt:      meta.WriteTime,
			Retention: blobstore.RetentionSpec{
				Mode:        meta.Retention.Mode,
				RetainUntil: meta.Retention.RetainUntil,
				LegalHold:   meta.Retention.LegalHold,
			},
		}); err != nil {
			p.logger.Warn("blobio: status put on restore failed",
				slog.String("blob_id", blobID), slog.Any("err", err))
		}
	}()

	return ReadResult{
		Body:           io.NopCloser(bytes.NewReader(body)),
		Size:           meta.Size,
		ChecksumSHA256: meta.ChecksumSHA256,
		VersionID:      meta.VersionID,
		FromCache:      false,
	}, nil
}

// restoreFromL2 fetches the blob from the durable origin, deduplicating
// concurrent reads of the same blob via singleflight.
func (p *Pipeline) restoreFromL2(ctx context.Context, blobID string) ([]byte, blobstore.ObjectMeta, error) {
	// Check for an existing inflight call.
	ch := make(chan struct{})
	call := &inflightCall{done: ch}
	if actual, loaded := p.inflight.LoadOrStore(blobID, call); loaded {
		existing := actual.(*inflightCall)
		select {
		case <-existing.done:
			if existing.err != nil {
				return nil, blobstore.ObjectMeta{}, existing.err
			}
			// Return the shared buffer directly. The body is read-only
			// after being fetched; callers must not mutate the returned slice.
			return existing.body, existing.meta, nil
		case <-ctx.Done():
			return nil, blobstore.ObjectMeta{}, ctx.Err()
		}
	}
	defer p.inflight.Delete(blobID)
	defer close(call.done)
	// Ensure cleanup happens even if the function panics.
	defer func() {
		if r := recover(); r != nil {
			call.err = fmt.Errorf("blobio: panic in restoreFromL2: %v", r)
		}
	}()

	r, meta, err := p.store.Get(ctx, blobstore.GetRequest{
		Ref: blobstore.VersionedObjectRef{Key: blobID},
	})
	if err != nil {
		call.err = fmt.Errorf("blobio: l2 get: %w", err)
		return nil, blobstore.ObjectMeta{}, call.err
	}
	defer r.Close()
	// Guard against unbounded memory allocation: cap the restore at
	// maxRestoreBytes. Blobs larger than this should use a streaming
	// path that bypasses the singleflight buffer.
	var reader io.Reader = r
	if p.maxRestoreBytes > 0 {
		reader = io.LimitReader(r, p.maxRestoreBytes+1)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		call.err = fmt.Errorf("blobio: l2 read: %w", err)
		return nil, blobstore.ObjectMeta{}, call.err
	}
	if p.maxRestoreBytes > 0 && int64(len(body)) > p.maxRestoreBytes {
		call.err = fmt.Errorf("blobio: l2 restore of %q exceeds max_restore_bytes %d", blobID, p.maxRestoreBytes)
		return nil, blobstore.ObjectMeta{}, call.err
	}
	// Verify checksum only when the provider did not already report
	// one. When meta.ChecksumSHA256 is set by the provider, the value
	// was computed during the upload and is trusted; re-hashing the
	// full body on every restore wastes CPU for large blobs.
	if meta.ChecksumSHA256 == "" {
		sum := sha256.Sum256(body)
		meta.ChecksumSHA256 = hex.EncodeToString(sum[:])
	} else {
		sum := sha256.Sum256(body)
		if hex.EncodeToString(sum[:]) != meta.ChecksumSHA256 {
			call.err = fmt.Errorf("blobio: %w: l2 checksum mismatch", blobstore.ErrChecksumMismatch)
			return nil, blobstore.ObjectMeta{}, call.err
		}
	}
	call.body = body
	call.meta = meta
	// Return the shared buffer directly. The body is read-only;
	// callers must not mutate the returned slice.
	return body, meta, nil
}

// promoteMultipartThreshold is the blob size above which Promote
// uses multipart upload instead of a single PutObject. This keeps
// memory bounded for large blobs: each part is read from the L1
// cache reader in partSized chunks, never loading the full blob.
const promoteMultipartThreshold = 100 * 1024 * 1024 // 100 MiB

// promotePartSize is the size of each multipart upload part.
const promotePartSize = 16 * 1024 * 1024 // 16 MiB

// partBufferPool reuses part-sized byte slices across promoteMultipart
// calls to avoid repeated 16 MiB allocations and GC pressure.
var partBufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, promotePartSize)
		return &b
	},
}

// Promote writes a cached blob to the durable origin and flips its
// status to COMMITTED_DURABLE. It is called by the async promoter
// (drive-worker) or by Write when PromoteSync is true.
//
// Promote is singleflighted: concurrent calls for the same blobID
// block until the first one completes, then return its result.
//
// Before uploading, Promote HEADs the durable store to check if the
// blob already exists with the expected size and checksum. If it
// does, the upload is skipped and only the status is updated. This
// makes Promote idempotent across crashes: if the process died after
// the PUT but before the status update, the next Promote will find
// the blob via HEAD and skip the re-upload.
//
// For blobs larger than promoteMultipartThreshold, Promote uses
// multipart upload with parallel part uploads (configured by
// SetMultipartParallelism). For smaller blobs, the body is streamed
// through a hashing reader so the full body is never buffered in
// memory by the pipeline (the provider may still buffer internally).
func (p *Pipeline) Promote(ctx context.Context, blobID string) error {
	// Singleflight: dedup concurrent promotes of the same blob.
	ch := make(chan struct{})
	call := &promoteCall{done: ch}
	if actual, loaded := p.promoteInflight.LoadOrStore(blobID, call); loaded {
		existing := actual.(*promoteCall)
		select {
		case <-existing.done:
			return existing.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	defer p.promoteInflight.Delete(blobID)
	defer close(call.done)
	// Ensure cleanup happens even if the function panics.
	defer func() {
		if r := recover(); r != nil {
			call.err = fmt.Errorf("blobio: panic in Promote: %v", r)
		}
	}()
	err := p.promoteLocked(ctx, blobID)
	call.err = err
	return err
}

func (p *Pipeline) promoteLocked(ctx context.Context, blobID string) error {
	p.promoteAttempts.Add(1)
	start := time.Now()
	defer func() {
		p.promoteDurationNs.Add(time.Since(start).Nanoseconds())
	}()

	status, err := p.status.Get(ctx, blobID)
	if err != nil {
		p.promoteFailures.Add(1)
		return fmt.Errorf("blobio: status get: %w", err)
	}
	if status == nil {
		p.promoteFailures.Add(1)
		return fmt.Errorf("blobio: %w: blob %q not in status store", blobstore.ErrNotFound, blobID)
	}
	if status.CommitState == CommitDurable {
		p.promoteSkipped.Add(1)
		return nil
	}

	// Idempotency check: HEAD the durable store to see if the blob
	// was already promoted (e.g. crash after PUT but before status
	// update). If the size and checksum match, skip the upload.
	if p.checkAlreadyDurable(ctx, blobID, status) {
		p.promoteSkipped.Add(1)
		p.logger.Info("blobio: promote skipped (already durable via HEAD)",
			slog.String("blob_id", blobID))
		// Mark the blob as durable. Use MarkDurable (a targeted UPDATE)
		// since the row already exists from the initial cache.
		if err := p.status.MarkDurable(ctx, blobID, status.VersionID, status.Size, status.ChecksumSHA256, time.Now().UTC()); err != nil {
			p.promoteFailures.Add(1)
			return fmt.Errorf("blobio: mark durable after HEAD skip: %w", err)
		}
		return nil
	}

	// Read from L1.
	r, _, err := p.cache.Get(ctx, blobID)
	if err != nil {
		p.promoteFailures.Add(1)
		return fmt.Errorf("blobio: promote cache get: %w", err)
	}
	defer r.Close()

	// Choose single PUT or multipart based on size.
	var res blobstore.PutResult
	if status.Size > promoteMultipartThreshold {
		res, err = p.promoteMultipart(ctx, blobID, r, status)
	} else {
		res, err = p.promoteSingle(ctx, blobID, r, status)
	}
	if err != nil {
		p.promoteFailures.Add(1)
		return err
	}

	// Mark the blob as durable. Use MarkDurable (a targeted UPDATE)
	// instead of Put (a full upsert) because we know the row already
	// exists — it was created when the blob was first cached.
	if err := p.status.MarkDurable(ctx, blobID, res.VersionID, res.Size, res.ChecksumSHA256, res.WriteTime); err != nil {
		p.promoteFailures.Add(1)
		return fmt.Errorf("blobio: mark durable after promote: %w", err)
	}
	// Re-put the cache entry as evictable now that the blob is
	// durable. This clears the NonEvictable flag so the entry can
	// be evicted under memory pressure. We re-read from the cache
	// (which still has the body) to get a fresh reader.
	if cacheBody, _, cerr := p.cache.Get(ctx, blobID); cerr == nil {
		defer cacheBody.Close()
		if err := p.cache.Put(ctx, blobID, cacheBody, hotcache.PutOptions{
			SizeBytes:    status.Size,
			Hash:         status.ChecksumSHA256,
			PinHot:       false,
			NonEvictable: false,
		}); err != nil {
			p.logger.Warn("blobio: cache re-put after promote failed",
				slog.String("blob_id", blobID), slog.Any("err", err))
		}
	}
	p.promoteSuccesses.Add(1)
	return nil
}

// checkAlreadyDurable HEADs the durable store and returns true if the
// blob exists with the expected size and checksum. This makes Promote
// idempotent across crashes.
func (p *Pipeline) checkAlreadyDurable(ctx context.Context, blobID string, status *BlobStatus) bool {
	meta, err := p.store.Head(ctx, blobstore.ObjectRef{Key: blobID})
	if err != nil {
		return false // not found or error → need to upload
	}
	if meta.Size != status.Size {
		return false
	}
	if meta.ChecksumSHA256 != "" && status.ChecksumSHA256 != "" &&
		meta.ChecksumSHA256 != status.ChecksumSHA256 {
		return false
	}
	return true
}

// promoteSingle streams the body through a hashing reader to the
// durable store. The body is NOT buffered in memory by the pipeline;
// the hashing reader computes the SHA-256 incrementally as the body
// flows to the provider. The provider may buffer internally (e.g.
// for signed payloads), but the pipeline's memory footprint is
// bounded by the io.Copy buffer (32 KiB).
func (p *Pipeline) promoteSingle(ctx context.Context, blobID string, r io.Reader, status *BlobStatus) (blobstore.PutResult, error) {
	// Wrap the reader with a hashing reader so we verify the checksum
	// incrementally as the body is uploaded. The TeeReader forwards
	// every byte read from r to both the store (via Put) and the
	// hasher. After Put returns successfully, the hasher has seen
	// exactly the bytes that were sent to the store.
	hasher := sha256.New()
	tee := io.TeeReader(r, hasher)

	res, err := p.store.Put(ctx, blobstore.PutRequest{
		Key:            blobID,
		Body:           tee,
		ExpectedLength: status.Size,
		ChecksumSHA256: status.ChecksumSHA256,
		ContentType:    "application/octet-stream",
		Retention:      status.Retention,
	})
	if err != nil {
		return blobstore.PutResult{}, fmt.Errorf("blobio: promote put: %w", err)
	}
	// Verify the body matched the recorded checksum. The hasher has
	// seen exactly the bytes that Put consumed. If Put consumed fewer
	// bytes than status.Size (e.g. due to a provider bug), the hash
	// would not match and we'd catch it here.
	gotSum := hex.EncodeToString(hasher.Sum(nil))
	if status.ChecksumSHA256 != "" && gotSum != status.ChecksumSHA256 {
		return blobstore.PutResult{}, fmt.Errorf("blobio: %w: promote checksum mismatch", blobstore.ErrChecksumMismatch)
	}
	return res, nil
}

// promoteMultipart streams the body in part-sized chunks, uploading
// each part to the provider. Parts are uploaded in parallel via a
// configurable worker pool (multipartParallelism). The SHA-256 is
// computed incrementally as parts are read. Memory usage is bounded
// by promotePartSize * multipartParallelism regardless of total blob
// size.
func (p *Pipeline) promoteMultipart(ctx context.Context, blobID string, r io.Reader, status *BlobStatus) (blobstore.PutResult, error) {
	// Use a cancellable sub-context so that when promoteMultipart
	// returns (even on error), all background goroutines (reader,
	// upload workers) are signalled to stop. This prevents goroutine
	// leaks when the collector returns early on a part error.
	multipartCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	upload, err := p.store.CreateMultipart(multipartCtx, blobstore.MultipartRequest{
		Key:         blobID,
		ContentType: "application/octet-stream",
		Retention:   status.Retention,
	})
	if err != nil {
		return blobstore.PutResult{}, fmt.Errorf("blobio: create multipart: %w", err)
	}

	// Ensure abort on any failure path.
	aborted := false
	defer func() {
		if !aborted {
			// Use a short timeout so abort doesn't hang indefinitely
			// if the storage backend is unresponsive.
			abortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := p.store.AbortMultipart(abortCtx, upload); err != nil {
				p.logger.Warn("blobio: abort multipart failed",
					slog.String("blob_id", blobID), slog.Any("err", err))
			}
		}
	}()

	h := sha256.New()
	parallelism := p.multipartParallelism
	if parallelism < 1 {
		parallelism = 1
	}

	type partJob struct {
		partNum int32
		body    []byte
		bufPtr  *[]byte // pooled buffer; worker must return it to partBufferPool
	}
	type partResult struct {
		partNum int32
		part    blobstore.UploadedPart
		err     error
	}

	// Read parts sequentially (the reader is single-pass) and send
	// them to the upload workers via a channel.
	partCh := make(chan partJob, parallelism)
	resultCh := make(chan partResult, parallelism)

	// Start upload workers.
	var wg sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range partCh {
				if multipartCtx.Err() != nil {
					partBufferPool.Put(job.bufPtr)
					resultCh <- partResult{partNum: job.partNum, err: multipartCtx.Err()}
					continue
				}
				part, perr := p.store.UploadPart(multipartCtx, blobstore.UploadPartRequest{
					Upload:     upload,
					PartNumber: job.partNum,
					Body:       bytes.NewReader(job.body),
				})
				partBufferPool.Put(job.bufPtr)
				resultCh <- partResult{partNum: job.partNum, part: part, err: perr}
			}
		}()
	}

	// Read parts and feed workers. Track the total part count so the
	// collector knows when to stop.
	partNum := int32(1)
	var readErr error
	go func() {
		defer close(partCh)
		for {
			if multipartCtx.Err() != nil {
				readErr = multipartCtx.Err()
				return
			}
			// Get a fresh buffer from the pool for each part. The
			// worker returns it to the pool after uploading, so the
			// pool stabilises at ~2*parallelism buffers.
			partBufPtr := partBufferPool.Get().(*[]byte)
			partBuf := *partBufPtr
			n, err := io.ReadFull(r, partBuf)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				partBufferPool.Put(partBufPtr)
				if multipartCtx.Err() != nil {
					readErr = multipartCtx.Err()
					return
				}
				readErr = fmt.Errorf("blobio: multipart read part %d: %w", partNum, err)
				return
			}
			if n == 0 {
				partBufferPool.Put(partBufPtr)
				return
			}
			h.Write(partBuf[:n])
			select {
			case partCh <- partJob{partNum: partNum, body: partBuf[:n], bufPtr: partBufPtr}:
			case <-multipartCtx.Done():
				partBufferPool.Put(partBufPtr)
				readErr = multipartCtx.Err()
				return
			}
			partNum++
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return
			}
		}
	}()

	// Collect results. The reader goroutine closes partCh when done,
	// which causes workers to exit their range loop. We wait for all
	// workers to finish, then drain any remaining results.
	results := make(map[int32]partResult)
	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()
	// Collect results until all workers are done and all results
	// received.
collect:
	for {
		select {
		case res := <-resultCh:
			results[res.partNum] = res
			if res.err != nil {
				return blobstore.PutResult{}, fmt.Errorf("blobio: upload part %d: %w", res.partNum, res.err)
			}
		case <-workersDone:
			// All workers done; drain any remaining results.
			for len(resultCh) > 0 {
				res := <-resultCh
				results[res.partNum] = res
				if res.err != nil {
					return blobstore.PutResult{}, fmt.Errorf("blobio: upload part %d: %w", res.partNum, res.err)
				}
			}
			break collect
		case <-multipartCtx.Done():
			return blobstore.PutResult{}, multipartCtx.Err()
		}
	}
	if readErr != nil {
		return blobstore.PutResult{}, readErr
	}

	// Build the completed parts list in order.
	maxPart := int32(0)
	for i := range results {
		if i > maxPart {
			maxPart = i
		}
	}
	completedParts := make([]blobstore.CompletedPart, 0, maxPart)
	for i := int32(1); i <= maxPart; i++ {
		res, ok := results[i]
		if !ok {
			return blobstore.PutResult{}, fmt.Errorf("blobio: missing part %d result", i)
		}
		completedParts = append(completedParts, blobstore.CompletedPart{
			PartNumber:     res.part.PartNumber,
			ChecksumSHA256: res.part.ChecksumSHA256,
			Size:           res.part.Size,
		})
	}

	// Verify the incremental checksum matches the recorded one.
	gotChecksum := hex.EncodeToString(h.Sum(nil))
	if gotChecksum != status.ChecksumSHA256 {
		return blobstore.PutResult{}, fmt.Errorf("blobio: %w: multipart checksum mismatch", blobstore.ErrChecksumMismatch)
	}

	res, err := p.store.CompleteMultipart(multipartCtx, blobstore.CompleteRequest{
		Upload: upload,
		Parts:  completedParts,
	})
	if err != nil {
		return blobstore.PutResult{}, fmt.Errorf("blobio: complete multipart: %w", err)
	}
	aborted = true // CompleteMultipart succeeded; don't abort.
	return res, nil
}

// Status returns the current placement status of a blob.
func (p *Pipeline) Status(ctx context.Context, blobID string) (*BlobStatus, error) {
	status, err := p.status.Get(ctx, blobID)
	if err != nil {
		return nil, fmt.Errorf("blobio: status get: %w", err)
	}
	if status == nil {
		return nil, fmt.Errorf("blobio: %w: blob %q", blobstore.ErrNotFound, blobID)
	}
	return status, nil
}

// Evict removes a blob from L1. It is a no-op if the blob is not
// durable yet (the cache is the only copy).
func (p *Pipeline) Evict(ctx context.Context, blobID string) error {
	status, _ := p.status.Get(ctx, blobID)
	if status != nil && status.CommitState != CommitDurable {
		return fmt.Errorf("blobio: cannot evict blob %q: not yet durable", blobID)
	}
	return p.cache.Evict(ctx, blobID)
}

// PipelineStats holds pipeline-level metrics for observability.
type PipelineStats struct {
	PromoteAttempts      int64
	PromoteSuccesses     int64
	PromoteFailures      int64
	PromoteSkipped       int64
	PromoteDurationAvgMs float64
}

// Stats returns a snapshot of pipeline-level metrics.
func (p *Pipeline) Stats() PipelineStats {
	attempts := p.promoteAttempts.Load()
	successes := p.promoteSuccesses.Load()
	failures := p.promoteFailures.Load()
	skipped := p.promoteSkipped.Load()
	totalNs := p.promoteDurationNs.Load()
	avgMs := 0.0
	if attempts > 0 {
		avgMs = float64(totalNs) / float64(attempts) / 1e6
	}
	return PipelineStats{
		PromoteAttempts:      attempts,
		PromoteSuccesses:     successes,
		PromoteFailures:      failures,
		PromoteSkipped:       skipped,
		PromoteDurationAvgMs: avgMs,
	}
}

// Close cancels the lifecycle context and waits for background
// goroutines (async cache puts on the read path) to finish. The
// gateway and worker call this during graceful shutdown.
func (p *Pipeline) Close() error {
	if p.lifecycleCancel != nil {
		p.lifecycleCancel()
	}
	p.wg.Wait()
	return nil
}
