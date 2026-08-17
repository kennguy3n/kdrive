// Package blobstore defines the provider-agnostic BlobStore and
// BlobInventory interfaces that every storage backend must implement.
//
// The interfaces are the canonical abstraction described in
// KChat-Privacy-Drive-Storage-Architecture-v1.0.md §13.2. They let the
// gateway add, remove, and migrate between backends without
// customer-visible changes.
//
// Phase 1 ships exactly two adapters:
//   - local_fs_dev: dev/CI loopback (pkg/blobstore/local_fs_dev)
//   - s3: production S3-compatible adapter (pkg/blobstore/s3)
//     supports Wasabi, AWS S3, and Backblaze B2 via the provider
//     config field.
//
// All layers above this package operate on ciphertext. Object keys
// carry no tenant, user, group, file, or folder identifiers (§3
// invariant 14). KChat SHA-256 is the authoritative end-to-end
// checksum; provider ETags are never used as the integrity check
// (§13.3).
package blobstore

import (
	"context"
	"io"
	"time"
)

// ObjectRef identifies an opaque immutable object by its provider-
// independent key. The key contains no tenant, user, group, file, or
// folder identifiers.
type ObjectRef struct {
	Key string
}

// VersionedObjectRef identifies a specific provider version of an
// object. VersionID is nullable: never-versioned providers (R2 with
// versioning off, a never-versioned AWS/Wasabi bucket) legitimately
// return no version ID. For those objects the caller computes a
// provider_object_identity_hash over provider, account, bucket,
// immutable key, NEVER_ENABLED versioning state, size, checksum and
// write-result hash (§13.2).
type VersionedObjectRef struct {
	Key       string
	VersionID string // nullable
}

// RetentionMode names the object-lock retention mode.
type RetentionMode string

const (
	RetentionNone       RetentionMode = "NONE"
	RetentionGovernance RetentionMode = "GOVERNANCE"
	RetentionCompliance RetentionMode = "COMPLIANCE"
)

// RetentionSpec is the normalized retention request carried on every
// write. Normal Drive writes require NONE; locked writes require
// explicit values (§13.2).
type RetentionSpec struct {
	Mode        RetentionMode
	RetainUntil time.Time
	LegalHold   bool
}

// RetentionState is the effective retention reported by the provider
// after a write or a retention query.
type RetentionState struct {
	Mode        RetentionMode
	RetainUntil time.Time
	LegalHold   bool
}

// BucketVersioningState names the provider bucket versioning mode.
type BucketVersioningState string

const (
	VersioningNeverEnabled          BucketVersioningState = "NEVER_ENABLED"
	VersioningEnabled               BucketVersioningState = "ENABLED"
	VersioningSuspendedVersionAware BucketVersioningState = "SUSPENDED_VERSION_AWARE"
)

// PutRequest is the normalized write request. Every provider writer —
// including upload edge, replication, repair, rehydrate, archive
// packing, migration, backup and L4 hold — uses this shape.
type PutRequest struct {
	// Key is the provider-independent opaque object key.
	Key string
	// Body is the ciphertext payload.
	Body io.Reader
	// ExpectedLength is the expected ciphertext length in bytes.
	ExpectedLength int64
	// ChecksumSHA256 is the KChat SHA-256 of the ciphertext. This is
	// the authoritative end-to-end checksum; provider ETags are never
	// used as the integrity check.
	ChecksumSHA256 string
	// ContentType is always application/octet-stream for Drive
	// ciphertext. It is carried verbatim for provider compatibility.
	ContentType string
	// IdempotencyToken makes the PUT idempotent across retries.
	IdempotencyToken string
	// Region is the residency class for the write.
	Region string
	// DurabilityClass is the durability tier requested.
	DurabilityClass string
	// ProviderVersionID, when non-empty, pins the write to an exact
	// provider version (used by qualified direct-staging promotion).
	ProviderVersionID string
	// Retention is the normalized retention spec. Normal Drive writes
	// require NONE; locked writes require explicit values.
	Retention RetentionSpec
	// IfNoneMatch, when true, asks the provider to fail if the object
	// already exists (conditional create). Providers that cannot
	// honour this MUST return ErrPreconditionFailed.
	IfNoneMatch bool
}

// PutResult reports the outcome of a successful PUT. It returns a
// nullable provider version ID, bucket versioning state, effective
// retention mode and date, legal-hold state, provider write time,
// stored size, verified checksum, and canonical write-result hash.
type PutResult struct {
	Key             string
	VersionID       string // nullable
	VersioningState BucketVersioningState
	Retention       RetentionState
	WriteTime       time.Time
	Size            int64
	ChecksumSHA256  string
	// WriteResultHash is the canonical hash over the verified write
	// result, binding provider, account, bucket, immutable key,
	// versioning state, size, checksum, and (when present) version ID.
	WriteResultHash string
}

// ObjectMeta is the read-side projection of a stored object.
type ObjectMeta struct {
	Key             string
	VersionID       string // nullable
	VersioningState BucketVersioningState
	Size            int64
	ChecksumSHA256  string
	Retention       RetentionState
	WriteTime       time.Time
}

// ByteRange is a closed byte range [Start, End] where both endpoints
// are inclusive. End == -1 means "to the end of the object".
type ByteRange struct {
	Start int64
	End   int64
}

// GetRequest fetches an object, optionally restricted to a byte range.
type GetRequest struct {
	Ref   VersionedObjectRef
	Range *ByteRange
}

// MultipartRequest initiates a multipart upload.
type MultipartRequest struct {
	Key              string
	ContentType      string
	ChecksumSHA256   string
	Region           string
	DurabilityClass  string
	Retention        RetentionSpec
	IdempotencyToken string
}

// MultipartUpload identifies an in-progress multipart upload.
type MultipartUpload struct {
	Key      string
	UploadID string
}

// SignPartRequest asks the provider for a part-upload target. Phase-1
// product clients receive edge grants, not provider presigns
// (§13.3); this is the seam a future qualified direct-staging adapter
// would implement.
type SignPartRequest struct {
	Upload      MultipartUpload
	PartNumber  int32
	ExpectedLen int64
}

// SignedRequest is a provider-issued upload target for one part.
type SignedRequest struct {
	URL     string
	Headers map[string]string
	// ExpiresAt is when the signed target stops accepting writes.
	ExpiresAt time.Time
}

// UploadPartRequest uploads one part of a multipart upload directly
// from the server (used by the worker for promote). Unlike
// SignUploadPart (which returns a presigned URL for the client),
// this method uploads the part body server-side.
type UploadPartRequest struct {
	Upload     MultipartUpload
	PartNumber int32
	Body       io.Reader
}

// UploadedPart describes one successfully uploaded part.
type UploadedPart struct {
	PartNumber     int32
	ChecksumSHA256 string
	Size           int64
}

// CompleteRequest finalizes a multipart upload.
type CompleteRequest struct {
	Upload    MultipartUpload
	Parts     []CompletedPart
	Retention RetentionSpec
}

// CompletedPart describes one verified part of a multipart upload.
type CompletedPart struct {
	PartNumber     int32
	ChecksumSHA256 string
	Size           int64
}

// CopyWithinProviderRequest is a server-side copy inside one
// provider-compatible namespace. Cross-provider replication is a
// worker pipeline, not this call (§13.2).
type CopyWithinProviderRequest struct {
	Src       VersionedObjectRef
	DstKey    string
	Retention RetentionSpec
}

// RetentionUpdate permits monotonic extension and a separately
// authorized clear only after policy and provider rules allow it. It
// never silently shortens a retention period (§13.2).
type RetentionUpdate struct {
	Ref         VersionedObjectRef
	Mode        RetentionMode
	RetainUntil time.Time
	// ClearRetention, when true and policy permits, removes the
	// retention lock. Ordinary blob credentials cannot bypass
	// governance retention; lock-administration credentials live in a
	// separate quorum-controlled path.
	ClearRetention bool
}

// ProviderCapabilities reports the full §13.3 capability flag set as
// a typed struct. The gateway consults these flags before routing a
// request.
type ProviderCapabilities struct {
	ProviderVersioning         bool
	DeleteAllVersions          bool
	ConditionalPut             bool
	NativeSHA256Checksum       bool
	S3ObjectLock               bool
	PerVersionRetentionGet     bool
	PerVersionRetentionExtend  bool
	PerVersionRetentionClear   bool
	LegalHoldSetClear          bool
	GovernanceBypassDenied     bool
	ProviderSpecificPrefixLock bool
	Lifecycle                  bool
	EventNotifications         bool
	Replication                bool
	ExactJurisdiction          string
	MaxObjectSize              int64
	MaxPartSize                int64
	MaxParts                   int32
	MinPartBytes               int64
	ChecksumOnComplete         bool
	ListAndAbortMultipart      bool
	ConfigurableCORS           bool
	BrowserPresignedUpload     bool
	SignedHeaderSet            string
	ReusableGrant              bool
	PathStyleAddressing        bool
	VirtualHostAddressing      bool
	SupportedSignatureVersions []string
	PrivateBucketControls      bool
	BucketVersioningState      BucketVersioningState
}

// BlobStore is the provider-agnostic interface every adapter
// implements.
type BlobStore interface {
	Put(ctx context.Context, req PutRequest) (PutResult, error)
	Head(ctx context.Context, ref ObjectRef) (ObjectMeta, error)
	Get(ctx context.Context, req GetRequest) (io.ReadCloser, ObjectMeta, error)
	Delete(ctx context.Context, ref ObjectRef) error
	PurgeAllVersions(ctx context.Context, ref ObjectRef) error

	CreateMultipart(ctx context.Context, req MultipartRequest) (MultipartUpload, error)
	SignUploadPart(ctx context.Context, req SignPartRequest) (SignedRequest, error)
	UploadPart(ctx context.Context, req UploadPartRequest) (UploadedPart, error)
	CompleteMultipart(ctx context.Context, req CompleteRequest) (PutResult, error)
	AbortMultipart(ctx context.Context, upload MultipartUpload) error

	CopyWithinProvider(ctx context.Context, req CopyWithinProviderRequest) (PutResult, error)
	GetRetention(ctx context.Context, ref VersionedObjectRef) (RetentionState, error)
	UpdateRetention(ctx context.Context, req RetentionUpdate) (RetentionState, error)
	SetLegalHold(ctx context.Context, ref VersionedObjectRef, enabled bool) (RetentionState, error)
	Capabilities(ctx context.Context) ProviderCapabilities
}

// ListObjectsRequest paginates object keys under a prefix. This is a
// trusted reconciliation interface, not a client-facing listing API
// (§13.2).
type ListObjectsRequest struct {
	Prefix  string
	Cursor  string
	MaxKeys int32
}

// ObjectPage is one page of ListObjects output.
type ObjectPage struct {
	Objects     []ObjectMeta
	NextCursor  string
	IsTruncated bool
}

// ListVersionsRequest paginates provider versions of one object.
type ListVersionsRequest struct {
	Key     string
	Cursor  string
	MaxKeys int32
}

// VersionPage is one page of ListObjectVersions output.
type VersionPage struct {
	Versions    []ObjectMeta
	NextCursor  string
	IsTruncated bool
}

// ListUploadsRequest paginates in-progress multipart uploads.
type ListUploadsRequest struct {
	Prefix  string
	Cursor  string
	MaxKeys int32
}

// UploadPage is one page of ListMultipartUploads output.
type UploadPage struct {
	Uploads    []MultipartUpload
	NextCursor string
}

// PartPage is one page of ListParts output.
type PartPage struct {
	Parts      []CompletedPart
	NextCursor string
}

// BatchDeleteResult reports the outcome of a batch version delete.
type BatchDeleteResult struct {
	Deleted []VersionedObjectRef
	Errors  []BatchDeleteError
}

// BatchDeleteError describes a per-version delete failure.
type BatchDeleteError struct {
	Ref   VersionedObjectRef
	Error string
}

// BlobInventory is the trusted reconciliation and erasure interface,
// not a client-facing listing API (§13.2). The B2 adapter may use the
// native B2 API where its S3 surface is insufficient. Large buckets
// may ingest signed provider inventory reports, but every adapter
// must still implement targeted version and multipart enumeration for
// purge confirmation.
type BlobInventory interface {
	ListObjects(ctx context.Context, req ListObjectsRequest) (ObjectPage, error)
	ListObjectVersions(ctx context.Context, req ListVersionsRequest) (VersionPage, error)
	DeleteVersion(ctx context.Context, ref VersionedObjectRef) error
	ListMultipartUploads(ctx context.Context, req ListUploadsRequest) (UploadPage, error)
	ListParts(ctx context.Context, upload MultipartUpload, cursor string) (PartPage, error)
	DeleteBatch(ctx context.Context, refs []VersionedObjectRef) (BatchDeleteResult, error)
}

// Normalized error taxonomy. Adapters wrap provider errors into these
// so the gateway can make uniform retry/throttle/retention decisions.
var (
	// ErrNotFound is returned when an object or version does not exist.
	ErrNotFound = errSentinel("blobstore: not found")
	// ErrPreconditionFailed is returned when a conditional create
	// (IfNoneMatch) finds the object already present.
	ErrPreconditionFailed = errSentinel("blobstore: precondition failed")
	// ErrThrottled is returned when the provider is rate-limiting the
	// request. The caller should back off.
	ErrThrottled = errSentinel("blobstore: throttled")
	// ErrRetentionConflict is returned when a write or delete would
	// bypass a governance or compliance retention lock.
	ErrRetentionConflict = errSentinel("blobstore: retention conflict")
	// ErrVersionMismatch is returned when the requested provider
	// version ID does not match the object's current version.
	ErrVersionMismatch = errSentinel("blobstore: version mismatch")
	// ErrChecksumMismatch is returned when the provider-reported
	// checksum does not match the expected KChat SHA-256.
	ErrChecksumMismatch = errSentinel("blobstore: checksum mismatch")
	// ErrAmbiguousTimeout is returned when a PUT times out and it is
	// unknown whether the object was written. The caller MUST HEAD +
	// verify before retrying (§13.3).
	ErrAmbiguousTimeout = errSentinel("blobstore: ambiguous timeout")
)

// errSentinel is a string error type so errors.Is and errors.As work
// across wrapped chains.
type errSentinel string

func (e errSentinel) Error() string { return string(e) }
