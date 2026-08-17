// Package s3 implements the BlobStore and BlobInventory interfaces
// against any S3-compatible API (Wasabi, AWS S3, Backblaze B2).
//
// The adapter is built directly on the AWS SDK v2 S3 client, which
// works with any S3-compatible endpoint. Provider-specific behavior
// (capabilities, error prefixes, write-result hash namespace,
// guardrail defaults) is selected via the ProviderName field on
// Config.
//
// Supported providers:
//   - "wasabi": 90-day minimum storage duration, fair-use egress
//     <= 1x active stored bytes, Object Lock supported. Guardrail
//     defaults live in pkg/storageguardrails.
//   - "s3": AWS S3. Object Lock supported. No minimum storage
//     duration. Standard AWS pricing.
//   - "b2": Backblaze B2 via its S3-compatible API. No S3 Object
//     Lock (B2 uses its own file-lock mechanism). No minimum storage
//     duration. B2 fair-use egress is more generous than Wasabi.
//
// Bucket versioning should be enabled so PurgeAllVersions and
// DeleteVersion behave correctly. Object Lock is supported via
// capability flags when the bucket is configured with a lock policy
// (Wasabi and AWS S3 only).
package s3

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/kchat/drive/pkg/blobstore"
)

// WasabiMinStorageDays is Wasabi's 90-day minimum storage duration.
// Objects deleted before this window still incur 90 days of billable
// storage. The data plane must not write short-TTL objects to Wasabi.
// Kept for backwards compatibility; new code should use
// storageguardrails.ProfileFor(providerName).MinStorageDays.
const WasabiMinStorageDays = 90

// WasabiStorageUSDPerTBMonth is Wasabi's headline storage price per
// TB-month. A "TB" here is 1e12 bytes and a "month" is 30 days,
// matching how Wasabi bills.
const WasabiStorageUSDPerTBMonth = 6.99

const minStorageDuration = WasabiMinStorageDays * 24 * time.Hour

// Config is the S3-compatible provider runtime configuration.
type Config struct {
	// ProviderName selects the provider profile. Known values:
	// "wasabi", "s3", "b2". Defaults to "s3" when empty. The
	// provider name is used in error messages, write-result hashes,
	// and capability reporting.
	ProviderName string
	// Endpoint is the S3-compatible endpoint URL, e.g.
	// "https://s3.ap-southeast-1.wasabisys.com" (Wasabi),
	// "https://s3.us-west-004.backblazeb2.com" (B2), or "" for
	// AWS S3 default regional endpoint.
	Endpoint string
	// Region is the region label used when signing requests.
	Region string
	// Bucket is the bucket used by this adapter instance.
	Bucket string
	// AccessKey / SecretKey are the service credentials. They are
	// never logged.
	AccessKey string
	SecretKey string
	// UsePathStyle forces path-style addressing. Most S3-compatible
	// endpoints (Wasabi, B2, MinIO) require or prefer path-style;
	// AWS S3 supports both.
	UsePathStyle bool
}

// S3API is the subset of s3.Client this adapter uses. Keeping it as an
// interface lets tests inject a fake without spinning up a real HTTP
// mock.
type S3API interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, in *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput, opts ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	ListObjectVersions(ctx context.Context, in *s3.ListObjectVersionsInput, opts ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error)
	CopyObject(ctx context.Context, in *s3.CopyObjectInput, opts ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, in *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput, opts ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	ListMultipartUploads(ctx context.Context, in *s3.ListMultipartUploadsInput, opts ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error)
	ListParts(ctx context.Context, in *s3.ListPartsInput, opts ...func(*s3.Options)) (*s3.ListPartsOutput, error)
	GetObjectRetention(ctx context.Context, in *s3.GetObjectRetentionInput, opts ...func(*s3.Options)) (*s3.GetObjectRetentionOutput, error)
	PutObjectRetention(ctx context.Context, in *s3.PutObjectRetentionInput, opts ...func(*s3.Options)) (*s3.PutObjectRetentionOutput, error)
	GetObjectLegalHold(ctx context.Context, in *s3.GetObjectLegalHoldInput, opts ...func(*s3.Options)) (*s3.GetObjectLegalHoldOutput, error)
	PutObjectLegalHold(ctx context.Context, in *s3.PutObjectLegalHoldInput, opts ...func(*s3.Options)) (*s3.PutObjectLegalHoldOutput, error)
}

// Provider is the S3-compatible BlobStore implementation.
type Provider struct {
	cfg          Config
	client       S3API
	breaker      *CircuitBreaker // nil = no circuit breaker
	errPrefixVal string          // cached provider-specific error prefix
}

// errPrefix returns the provider-specific error message prefix, e.g.
// "wasabi: " or "s3: " or "b2: ".
func (p *Provider) errPrefix() string {
	return p.errPrefixVal
}

// New returns a Provider backed by a freshly constructed s3.Client
// with a tuned HTTP transport and retry policy for production use.
//
// HTTP transport tuning:
//   - MaxIdleConnsPerHost: 100 (default is 2, which bottlenecks
//     concurrent requests to one endpoint).
//   - IdleConnTimeout: 90s (keep-alive window).
//   - TLS handshake timeout: 5s for cross-region latency.
//   - Response header timeout: 30s to detect stalled connections.
//
// Retry policy:
//   - Up to 3 retries with exponential backoff for 5xx and throttling.
//   - No retry for 4xx (client errors are not transient).
//   - Max retry backoff: 20s.
func New(cfg Config) (*Provider, error) {
	if cfg.ProviderName == "" {
		cfg.ProviderName = "s3"
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}
	httpClient := &http.Client{
		Transport: transport,
		Timeout:   0, // no overall timeout; per-request context controls it
	}
	client := s3.New(s3.Options{
		Region:       cfg.Region,
		BaseEndpoint: endpointPtr(cfg.Endpoint),
		UsePathStyle: cfg.UsePathStyle,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		HTTPClient:   httpClient,
	}, func(o *s3.Options) {
		// Use the SDK's standard retryer with custom thresholds.
		o.Retryer = retry.NewStandard(func(so *retry.StandardOptions) {
			so.MaxAttempts = 3
			so.MaxBackoff = 20 * time.Second
			so.Retryables = []retry.IsErrorRetryable{
				retry.IsErrorRetryableFunc(func(err error) aws.Ternary {
					if isRetryableHTTPError(err) {
						return aws.TrueTernary
					}
					return aws.UnknownTernary
				}),
			}
		})
	})
	return &Provider{cfg: cfg, client: client, errPrefixVal: cfg.ProviderName + ": "}, nil
}

// NewWithCircuitBreaker returns a Provider with a circuit breaker
// that fail-fasts requests when the storage backend is consistently
// returning errors. The breaker opens after threshold consecutive
// failures and stays open for resetTimeout before allowing a probe.
func NewWithCircuitBreaker(cfg Config, threshold int, resetTimeout time.Duration) (*Provider, error) {
	p, err := New(cfg)
	if err != nil {
		return nil, err
	}
	p.breaker = NewCircuitBreaker(threshold, resetTimeout)
	return p, nil
}

// isRetryableHTTPError returns true for transient HTTP errors that
// are safe to retry: 5xx server errors, 429 throttling, and network
// timeouts. 4xx errors (except 429) are not retried.
func isRetryableHTTPError(err error) bool {
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		code := respErr.Response.StatusCode
		if code >= 500 || code == 429 {
			return true
		}
		return false
	}
	// Network-level errors (dial failures, timeouts).
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

// NewWithClient returns a Provider using a caller-supplied S3API.
// Tests use this to exercise the adapter against an in-memory fake.
func NewWithClient(cfg Config, client S3API) (*Provider, error) {
	if cfg.ProviderName == "" {
		cfg.ProviderName = "s3"
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if client == nil {
		return nil, errors.New("s3: client is required")
	}
	return &Provider{cfg: cfg, client: client, errPrefixVal: cfg.ProviderName + ": "}, nil
}

func (c Config) validate() error {
	prefix := c.ProviderName + ": "
	if c.ProviderName == "" {
		prefix = "s3: "
	}
	if c.Endpoint == "" {
		return errors.New(prefix + "endpoint is required")
	}
	if c.Region == "" {
		return errors.New(prefix + "region is required")
	}
	if c.Bucket == "" {
		return errors.New(prefix + "bucket is required")
	}
	if c.AccessKey == "" || c.SecretKey == "" {
		return errors.New(prefix + "access_key and secret_key are required")
	}
	return nil
}

func endpointPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// BucketName returns the configured bucket.
func (p *Provider) BucketName() string { return p.cfg.Bucket }

// RegionName returns the configured region.
func (p *Provider) RegionName() string { return p.cfg.Region }

// Put uploads ciphertext to s3://{bucket}/{key}. It uses
// UNSIGNED-PAYLOAD for the request signature so non-seekable streams
// work; backend integrity is verified by the KChat SHA-256 checksum
// on the caller side, never by ETag.
func (p *Provider) Put(ctx context.Context, req blobstore.PutRequest) (blobstore.PutResult, error) {
	if req.Key == "" {
		return blobstore.PutResult{}, errors.New(p.errPrefix() + "key is required")
	}
	if req.Body == nil {
		return blobstore.PutResult{}, errors.New(p.errPrefix() + "body is required")
	}
	if p.breaker != nil {
		if err := p.breaker.Allow(); err != nil {
			return blobstore.PutResult{}, err
		}
	}
	in := &s3.PutObjectInput{
		Bucket: aws.String(p.cfg.Bucket),
		Key:    aws.String(req.Key),
		Body:   req.Body,
	}
	if req.ContentType != "" {
		in.ContentType = aws.String(req.ContentType)
	}
	if req.ExpectedLength > 0 {
		in.ContentLength = aws.Int64(req.ExpectedLength)
	}
	if req.ChecksumSHA256 != "" {
		in.ChecksumSHA256 = aws.String(req.ChecksumSHA256)
	}
	if req.IfNoneMatch {
		in.IfNoneMatch = aws.String("*")
	}
	// Reject retention requests for providers that don't support
	// S3 Object Lock (e.g., Backblaze B2).
	if req.Retention.Mode != blobstore.RetentionNone || req.Retention.LegalHold {
		caps := p.Capabilities(ctx)
		if !caps.S3ObjectLock {
			return blobstore.PutResult{}, errors.New(p.errPrefix() + "object lock not supported by this provider")
		}
	}
	if req.Retention.Mode != blobstore.RetentionNone {
		applyRetentionToPut(in, req.Retention)
	}
	if req.Retention.LegalHold {
		in.ObjectLockLegalHoldStatus = s3types.ObjectLockLegalHoldStatus("ON")
	}

	out, err := p.client.PutObject(ctx, in, s3.WithAPIOptions(
		v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
	))
	if err != nil {
		classified := p.classifyError(err, req.Key)
		// Only trip the breaker on server/network errors, not on
		// client errors (ErrNotFound, ErrPreconditionFailed,
		// ErrChecksumMismatch, ErrRetentionConflict).
		if p.breaker != nil && !isClientError(classified) {
			p.breaker.RecordFailure()
		}
		return blobstore.PutResult{}, classified
	}
	if p.breaker != nil {
		p.breaker.RecordSuccess()
	}
	now := time.Now().UTC()
	size := req.ExpectedLength
	if out.Size != nil {
		size = aws.ToInt64(out.Size)
	}
	versionID := aws.ToString(out.VersionId)
	sum := req.ChecksumSHA256
	if out.ChecksumSHA256 != nil {
		sum = aws.ToString(out.ChecksumSHA256)
	}
	return blobstore.PutResult{
		Key:             req.Key,
		VersionID:       versionID,
		VersioningState: blobstore.VersioningEnabled,
		Size:            size,
		ChecksumSHA256:  sum,
		WriteTime:       now,
		WriteResultHash: computeWriteResultHash(p.cfg.ProviderName, p.cfg.Bucket, req.Key, versionID, blobstore.VersioningEnabled, size, sum),
	}, nil
}

// Head returns metadata for the current version of key.
func (p *Provider) Head(ctx context.Context, ref blobstore.ObjectRef) (blobstore.ObjectMeta, error) {
	if ref.Key == "" {
		return blobstore.ObjectMeta{}, errors.New(p.errPrefix() + "key is required")
	}
	if p.breaker != nil {
		if err := p.breaker.Allow(); err != nil {
			return blobstore.ObjectMeta{}, err
		}
	}
	out, err := p.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(p.cfg.Bucket),
		Key:    aws.String(ref.Key),
	})
	if err != nil {
		classified := p.classifyError(err, ref.Key)
		if p.breaker != nil && !isClientError(classified) {
			p.breaker.RecordFailure()
		}
		return blobstore.ObjectMeta{}, classified
	}
	if p.breaker != nil {
		p.breaker.RecordSuccess()
	}
	return headOutputToMeta(ref.Key, out), nil
}

// Get fetches an object, honouring req.Range when set.
func (p *Provider) Get(ctx context.Context, req blobstore.GetRequest) (io.ReadCloser, blobstore.ObjectMeta, error) {
	if req.Ref.Key == "" {
		return nil, blobstore.ObjectMeta{}, errors.New(p.errPrefix() + "key is required")
	}
	if p.breaker != nil {
		if err := p.breaker.Allow(); err != nil {
			return nil, blobstore.ObjectMeta{}, err
		}
	}
	in := &s3.GetObjectInput{
		Bucket: aws.String(p.cfg.Bucket),
		Key:    aws.String(req.Ref.Key),
	}
	if req.Ref.VersionID != "" {
		in.VersionId = aws.String(req.Ref.VersionID)
	}
	if req.Range != nil {
		in.Range = aws.String(formatRange(req.Range))
	}
	out, err := p.client.GetObject(ctx, in)
	if err != nil {
		classified := p.classifyError(err, req.Ref.Key)
		if p.breaker != nil && !isClientError(classified) {
			p.breaker.RecordFailure()
		}
		return nil, blobstore.ObjectMeta{}, classified
	}
	if p.breaker != nil {
		p.breaker.RecordSuccess()
	}
	meta := blobstore.ObjectMeta{
		Key:             req.Ref.Key,
		VersionID:       aws.ToString(out.VersionId),
		VersioningState: blobstore.VersioningEnabled,
		Size:            aws.ToInt64(out.ContentLength),
		ChecksumSHA256:  aws.ToString(out.ChecksumSHA256),
		WriteTime:       aws.ToTime(out.LastModified),
	}
	return out.Body, meta, nil
}

// Delete places a delete marker (versioning is enabled).
func (p *Provider) Delete(ctx context.Context, ref blobstore.ObjectRef) error {
	if ref.Key == "" {
		return errors.New(p.errPrefix() + "key is required")
	}
	if p.breaker != nil {
		if err := p.breaker.Allow(); err != nil {
			return err
		}
	}
	_, err := p.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(p.cfg.Bucket),
		Key:    aws.String(ref.Key),
	})
	if err != nil {
		classified := p.classifyError(err, ref.Key)
		if p.breaker != nil && !isClientError(classified) {
			p.breaker.RecordFailure()
		}
		return classified
	}
	if p.breaker != nil {
		p.breaker.RecordSuccess()
	}
	return nil
}

// PurgeAllVersions lists every version and delete marker for key and
// deletes them individually. This is the erasure path.
func (p *Provider) PurgeAllVersions(ctx context.Context, ref blobstore.ObjectRef) error {
	if ref.Key == "" {
		return errors.New(p.errPrefix() + "key is required")
	}
	var cursor *string
	for {
		out, err := p.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket:    aws.String(p.cfg.Bucket),
			Prefix:    aws.String(ref.Key),
			KeyMarker: cursor,
			MaxKeys:   aws.Int32(1000),
		})
		if err != nil {
			return p.classifyError(err, ref.Key)
		}
		var objects []s3types.ObjectIdentifier
		// S3 lists keys in lexicographic order. Track whether we've
		// moved past the target key so we can stop early instead of
		// iterating through every object that shares the prefix.
		passedKey := false
		for _, v := range out.Versions {
			k := aws.ToString(v.Key)
			if k == ref.Key {
				objects = append(objects, s3types.ObjectIdentifier{
					Key:       aws.String(ref.Key),
					VersionId: v.VersionId,
				})
			} else if k > ref.Key {
				passedKey = true
			}
		}
		for _, d := range out.DeleteMarkers {
			k := aws.ToString(d.Key)
			if k == ref.Key {
				objects = append(objects, s3types.ObjectIdentifier{
					Key:       aws.String(ref.Key),
					VersionId: d.VersionId,
				})
			} else if k > ref.Key {
				passedKey = true
			}
		}
		if len(objects) > 0 {
			if _, err := p.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(p.cfg.Bucket),
				Delete: &s3types.Delete{Objects: objects},
			}); err != nil {
				return p.classifyError(err, ref.Key)
			}
		}
		// If we've seen keys past the target and the target's versions
		// are all in this page, there's no need to fetch more pages.
		if passedKey {
			break
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		cursor = out.NextKeyMarker
	}
	return nil
}

// CreateMultipart initiates a multipart upload.
func (p *Provider) CreateMultipart(ctx context.Context, req blobstore.MultipartRequest) (blobstore.MultipartUpload, error) {
	if req.Key == "" {
		return blobstore.MultipartUpload{}, errors.New(p.errPrefix() + "key is required")
	}
	if p.breaker != nil {
		if err := p.breaker.Allow(); err != nil {
			return blobstore.MultipartUpload{}, err
		}
	}
	in := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(p.cfg.Bucket),
		Key:    aws.String(req.Key),
	}
	if req.ContentType != "" {
		in.ContentType = aws.String(req.ContentType)
	}
	if req.ChecksumSHA256 != "" {
		in.ChecksumAlgorithm = s3types.ChecksumAlgorithm("SHA256")
	}
	// Reject retention requests for providers that don't support
	// S3 Object Lock (e.g., Backblaze B2).
	if req.Retention.Mode != blobstore.RetentionNone || req.Retention.LegalHold {
		caps := p.Capabilities(ctx)
		if !caps.S3ObjectLock {
			return blobstore.MultipartUpload{}, errors.New(p.errPrefix() + "object lock not supported by this provider")
		}
	}
	if req.Retention.Mode != blobstore.RetentionNone {
		applyRetentionToCreateMultipart(in, req.Retention)
	}
	if req.Retention.LegalHold {
		in.ObjectLockLegalHoldStatus = s3types.ObjectLockLegalHoldStatus("ON")
	}
	out, err := p.client.CreateMultipartUpload(ctx, in)
	if err != nil {
		classified := p.classifyError(err, req.Key)
		if p.breaker != nil && !isClientError(classified) {
			p.breaker.RecordFailure()
		}
		return blobstore.MultipartUpload{}, classified
	}
	if p.breaker != nil {
		p.breaker.RecordSuccess()
	}
	return blobstore.MultipartUpload{Key: req.Key, UploadID: aws.ToString(out.UploadId)}, nil
}

// SignUploadPart is the seam a future qualified direct-staging adapter
// would implement. Phase-1 product clients receive edge grants, not
// provider presigns, so this returns an error indicating the mode is
// disabled.
func (p *Provider) SignUploadPart(ctx context.Context, req blobstore.SignPartRequest) (blobstore.SignedRequest, error) {
	return blobstore.SignedRequest{}, errors.New(p.errPrefix() + "direct provider presign is disabled in phase 1 (ADR-019); use the blob-edge capability path")
}

// CompleteMultipart finalizes a multipart upload. Parts are uploaded
// out-of-band by the gateway's edge path; this call assembles them.
func (p *Provider) CompleteMultipart(ctx context.Context, req blobstore.CompleteRequest) (blobstore.PutResult, error) {
	if req.Upload.Key == "" || req.Upload.UploadID == "" {
		return blobstore.PutResult{}, errors.New(p.errPrefix() + "upload key and id are required")
	}
	if p.breaker != nil {
		if err := p.breaker.Allow(); err != nil {
			return blobstore.PutResult{}, err
		}
	}
	parts := make([]s3types.CompletedPart, 0, len(req.Parts))
	for _, part := range req.Parts {
		parts = append(parts, s3types.CompletedPart{
			PartNumber:     aws.Int32(part.PartNumber),
			ChecksumSHA256: aws.String(part.ChecksumSHA256),
		})
	}
	out, err := p.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(p.cfg.Bucket),
		Key:             aws.String(req.Upload.Key),
		UploadId:        aws.String(req.Upload.UploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		classified := p.classifyError(err, req.Upload.Key)
		if p.breaker != nil && !isClientError(classified) {
			p.breaker.RecordFailure()
		}
		return blobstore.PutResult{}, classified
	}
	if p.breaker != nil {
		p.breaker.RecordSuccess()
	}
	versionID := aws.ToString(out.VersionId)
	// CompleteMultipartUpload does not return size or checksum. HEAD
	// the assembled object to populate the PutResult with verified
	// metadata so downstream callers (e.g. the pipeline) can record
	// the real size and checksum.
	headOut, headErr := p.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:    aws.String(p.cfg.Bucket),
		Key:       aws.String(req.Upload.Key),
		VersionId: out.VersionId,
	})
	size := int64(0)
	checksum := ""
	writeTime := time.Now().UTC()
	if headErr == nil {
		size = aws.ToInt64(headOut.ContentLength)
		checksum = aws.ToString(headOut.ChecksumSHA256)
		if headOut.LastModified != nil {
			writeTime = *headOut.LastModified
		}
	}
	return blobstore.PutResult{
		Key:             req.Upload.Key,
		VersionID:       versionID,
		VersioningState: blobstore.VersioningEnabled,
		Size:            size,
		ChecksumSHA256:  checksum,
		WriteTime:       writeTime,
		WriteResultHash: computeWriteResultHash(p.cfg.ProviderName, p.cfg.Bucket, req.Upload.Key, versionID, blobstore.VersioningEnabled, size, checksum),
	}, nil
}

// AbortMultipart aborts an in-progress multipart upload.
func (p *Provider) AbortMultipart(ctx context.Context, upload blobstore.MultipartUpload) error {
	if upload.Key == "" || upload.UploadID == "" {
		return errors.New(p.errPrefix() + "upload key and id are required")
	}
	_, err := p.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(p.cfg.Bucket),
		Key:      aws.String(upload.Key),
		UploadId: aws.String(upload.UploadID),
	})
	if err != nil {
		return p.classifyError(err, upload.Key)
	}
	return nil
}

// UploadPart uploads one part of a multipart upload directly from
// the server. The body is streamed to a temp file while computing
// the SHA-256 checksum via TeeReader, then streamed to S3. This
// avoids buffering the entire part in memory. The returned
// UploadedPart carries the part number, size, and SHA-256 checksum
// for the CompleteMultipart call.
func (p *Provider) UploadPart(ctx context.Context, req blobstore.UploadPartRequest) (blobstore.UploadedPart, error) {
	if req.Upload.Key == "" || req.Upload.UploadID == "" {
		return blobstore.UploadedPart{}, errors.New(p.errPrefix() + "upload key and id are required")
	}
	if req.PartNumber < 1 || req.PartNumber > 10000 {
		return blobstore.UploadedPart{}, errors.New(p.errPrefix() + "part number must be 1-10000")
	}
	if req.Body == nil {
		return blobstore.UploadedPart{}, errors.New(p.errPrefix() + "part body is required")
	}
	if p.breaker != nil {
		if err := p.breaker.Allow(); err != nil {
			return blobstore.UploadedPart{}, err
		}
	}

	// Stream the part body to a temp file while computing the SHA-256
	// checksum via TeeReader. This avoids buffering the entire part
	// (up to 5 GiB) in memory. The temp file is seekable so the SDK
	// can determine ContentLength without buffering.
	// Cap at MaxPartSize (5 GiB) to prevent unbounded disk usage.
	const maxPartBytes = 5 * 1024 * 1024 * 1024
	tmpFile, err := os.CreateTemp("", "s3-uploadpart-*")
	if err != nil {
		return blobstore.UploadedPart{}, fmt.Errorf(p.errPrefix()+"create temp file for part %d: %w", req.PartNumber, err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)
	defer tmpFile.Close()

	hasher := sha256.New()
	tee := io.TeeReader(io.LimitReader(req.Body, maxPartBytes+1), hasher)
	n, err := io.Copy(tmpFile, tee)
	if err != nil {
		return blobstore.UploadedPart{}, fmt.Errorf(p.errPrefix()+"read part %d: %w", req.PartNumber, err)
	}
	if n > maxPartBytes {
		return blobstore.UploadedPart{}, errors.New(p.errPrefix() + "part size exceeds 5 GiB limit")
	}
	checksum := hex.EncodeToString(hasher.Sum(nil))

	if _, err := tmpFile.Seek(0, 0); err != nil {
		return blobstore.UploadedPart{}, fmt.Errorf(p.errPrefix()+"seek temp file for part %d: %w", req.PartNumber, err)
	}

	out, err := p.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:         aws.String(p.cfg.Bucket),
		Key:            aws.String(req.Upload.Key),
		UploadId:       aws.String(req.Upload.UploadID),
		PartNumber:     aws.Int32(req.PartNumber),
		Body:           tmpFile,
		ContentLength:  aws.Int64(n),
		ChecksumSHA256: aws.String(checksum),
	})
	_ = out // ETag not used; KChat SHA-256 is authoritative
	if err != nil {
		classified := p.classifyError(err, req.Upload.Key)
		if p.breaker != nil && !isClientError(classified) {
			p.breaker.RecordFailure()
		}
		return blobstore.UploadedPart{}, classified
	}
	if p.breaker != nil {
		p.breaker.RecordSuccess()
	}

	// KChat SHA-256 is the authoritative checksum; ETag is not used.
	return blobstore.UploadedPart{
		PartNumber:     req.PartNumber,
		ChecksumSHA256: checksum,
		Size:           n,
	}, nil
}

// CopyWithinProvider performs a server-side CopyObject inside the
// same Wasabi bucket.
func (p *Provider) CopyWithinProvider(ctx context.Context, req blobstore.CopyWithinProviderRequest) (blobstore.PutResult, error) {
	if req.Src.Key == "" || req.DstKey == "" {
		return blobstore.PutResult{}, errors.New(p.errPrefix() + "src and dst keys are required")
	}
	src := p.cfg.Bucket + "/" + req.Src.Key
	if req.Src.VersionID != "" {
		src += "?versionId=" + req.Src.VersionID
	}
	in := &s3.CopyObjectInput{
		Bucket:     aws.String(p.cfg.Bucket),
		Key:        aws.String(req.DstKey),
		CopySource: aws.String(src),
	}
	if req.Retention.Mode != blobstore.RetentionNone {
		applyRetentionToCopy(in, req.Retention)
	}
	if req.Retention.LegalHold {
		in.ObjectLockLegalHoldStatus = s3types.ObjectLockLegalHoldStatus("ON")
	}
	out, err := p.client.CopyObject(ctx, in)
	if err != nil {
		return blobstore.PutResult{}, p.classifyError(err, req.DstKey)
	}
	versionID := aws.ToString(out.VersionId)
	// CopyObjectResult does not carry a VersionId; the top-level
	// VersionId field is the only source. The HEAD below will
	// populate it if the endpoint didn't return it on CopyObject.
	// CopyObject does not return content length; query it.
	head, herr := p.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(p.cfg.Bucket),
		Key:    aws.String(req.DstKey),
	})
	size := int64(0)
	sum := ""
	if herr == nil && head != nil {
		size = aws.ToInt64(head.ContentLength)
		sum = aws.ToString(head.ChecksumSHA256)
		if versionID == "" {
			versionID = aws.ToString(head.VersionId)
		}
	}
	return blobstore.PutResult{
		Key:             req.DstKey,
		VersionID:       versionID,
		VersioningState: blobstore.VersioningEnabled,
		Size:            size,
		ChecksumSHA256:  sum,
		WriteTime:       time.Now().UTC(),
		WriteResultHash: computeWriteResultHash(p.cfg.ProviderName, p.cfg.Bucket, req.DstKey, versionID, blobstore.VersioningEnabled, size, sum),
	}, nil
}

// GetRetention returns the retention state of a specific version.
func (p *Provider) GetRetention(ctx context.Context, ref blobstore.VersionedObjectRef) (blobstore.RetentionState, error) {
	if ref.Key == "" {
		return blobstore.RetentionState{}, errors.New(p.errPrefix() + "key is required")
	}
	out, err := p.client.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{
		Bucket:    aws.String(p.cfg.Bucket),
		Key:       aws.String(ref.Key),
		VersionId: aws.String(ref.VersionID),
	})
	if err != nil {
		return blobstore.RetentionState{}, p.classifyError(err, ref.Key)
	}
	state := blobstore.RetentionState{Mode: blobstore.RetentionNone}
	if out.Retention != nil {
		state.Mode = blobstore.RetentionMode(string(out.Retention.Mode))
		state.RetainUntil = aws.ToTime(out.Retention.RetainUntilDate)
	}
	hold, herr := p.client.GetObjectLegalHold(ctx, &s3.GetObjectLegalHoldInput{
		Bucket:    aws.String(p.cfg.Bucket),
		Key:       aws.String(ref.Key),
		VersionId: aws.String(ref.VersionID),
	})
	if herr == nil && hold != nil && hold.LegalHold != nil {
		state.LegalHold = string(hold.LegalHold.Status) == "ON"
	}
	return state, nil
}

// UpdateRetention permits monotonic extension and a separately
// authorized clear. It never silently shortens a retention period.
func (p *Provider) UpdateRetention(ctx context.Context, req blobstore.RetentionUpdate) (blobstore.RetentionState, error) {
	if req.Ref.Key == "" {
		return blobstore.RetentionState{}, errors.New(p.errPrefix() + "key is required")
	}
	in := &s3.PutObjectRetentionInput{
		Bucket:    aws.String(p.cfg.Bucket),
		Key:       aws.String(req.Ref.Key),
		VersionId: aws.String(req.Ref.VersionID),
	}
	if req.ClearRetention {
		in.Retention = &s3types.ObjectLockRetention{Mode: s3types.ObjectLockRetentionMode("OFF")}
	} else {
		mode := s3types.ObjectLockRetentionMode("GOVERNANCE")
		if req.Mode == blobstore.RetentionCompliance {
			mode = s3types.ObjectLockRetentionMode("COMPLIANCE")
		}
		in.Retention = &s3types.ObjectLockRetention{
			Mode:            mode,
			RetainUntilDate: aws.Time(req.RetainUntil),
		}
	}
	if _, err := p.client.PutObjectRetention(ctx, in); err != nil {
		return blobstore.RetentionState{}, p.classifyError(err, req.Ref.Key)
	}
	return p.GetRetention(ctx, req.Ref)
}

// SetLegalHold toggles the legal-hold flag on a version.
func (p *Provider) SetLegalHold(ctx context.Context, ref blobstore.VersionedObjectRef, enabled bool) (blobstore.RetentionState, error) {
	if ref.Key == "" {
		return blobstore.RetentionState{}, errors.New(p.errPrefix() + "key is required")
	}
	status := s3types.ObjectLockLegalHoldStatus("OFF")
	if enabled {
		status = s3types.ObjectLockLegalHoldStatus("ON")
	}
	if _, err := p.client.PutObjectLegalHold(ctx, &s3.PutObjectLegalHoldInput{
		Bucket:    aws.String(p.cfg.Bucket),
		Key:       aws.String(ref.Key),
		VersionId: aws.String(ref.VersionID),
		LegalHold: &s3types.ObjectLockLegalHold{Status: status},
	}); err != nil {
		return blobstore.RetentionState{}, p.classifyError(err, ref.Key)
	}
	return p.GetRetention(ctx, ref)
}

// Capabilities reports the Wasabi envelope.
func (p *Provider) Capabilities(ctx context.Context) blobstore.ProviderCapabilities {
	return defaultCapabilities(p.cfg.ProviderName, p.cfg.Region)
}

// defaultCapabilities returns the capability envelope for a given
// provider. Wasabi and AWS S3 support S3 Object Lock; Backblaze B2's
// S3-compatible API does not support Object Lock the same way (B2
// uses its own file-lock mechanism).
func defaultCapabilities(providerName, region string) blobstore.ProviderCapabilities {
	caps := blobstore.ProviderCapabilities{
		ProviderVersioning:    true,
		DeleteAllVersions:     true,
		ConditionalPut:        true,
		NativeSHA256Checksum:  true,
		ListAndAbortMultipart: true,
		BucketVersioningState: blobstore.VersioningEnabled,
		MaxObjectSize:         5 * 1024 * 1024 * 1024 * 1024,
		MaxPartSize:           5 * 1024 * 1024 * 1024,
		MaxParts:              10000,
		MinPartBytes:          5 * 1024 * 1024,
		ChecksumOnComplete:    true,
		ExactJurisdiction:     region,
	}
	switch providerName {
	case "wasabi", "s3":
		// Wasabi and AWS S3 support S3 Object Lock.
		caps.S3ObjectLock = true
		caps.PerVersionRetentionGet = true
		caps.PerVersionRetentionExtend = true
		caps.PerVersionRetentionClear = true
		caps.LegalHoldSetClear = true
		caps.GovernanceBypassDenied = true
	case "b2":
		// B2's S3-compatible API does not support S3 Object Lock.
		// B2 has its own file-lock mechanism accessed via the native
		// B2 API, which is not used in this adapter.
		caps.S3ObjectLock = false
		caps.PerVersionRetentionGet = false
		caps.PerVersionRetentionExtend = false
		caps.PerVersionRetentionClear = false
		caps.LegalHoldSetClear = false
		caps.GovernanceBypassDenied = false
	}
	return caps
}

// --- BlobInventory ---

// ListObjects paginates object keys under prefix.
func (p *Provider) ListObjects(ctx context.Context, req blobstore.ListObjectsRequest) (blobstore.ObjectPage, error) {
	in := &s3.ListObjectsV2Input{
		Bucket: aws.String(p.cfg.Bucket),
	}
	if req.Prefix != "" {
		in.Prefix = aws.String(req.Prefix)
	}
	if req.Cursor != "" {
		in.ContinuationToken = aws.String(req.Cursor)
	}
	// Cap MaxKeys at 1000 (S3 API maximum per page).
	maxKeys := req.MaxKeys
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}
	in.MaxKeys = aws.Int32(maxKeys)
	out, err := p.client.ListObjectsV2(ctx, in)
	if err != nil {
		return blobstore.ObjectPage{}, p.classifyError(err, "")
	}
	objects := make([]blobstore.ObjectMeta, 0, len(out.Contents))
	for _, obj := range out.Contents {
		objects = append(objects, blobstore.ObjectMeta{
			Key:             aws.ToString(obj.Key),
			Size:            aws.ToInt64(obj.Size),
			VersioningState: blobstore.VersioningEnabled,
			WriteTime:       aws.ToTime(obj.LastModified),
		})
	}
	return blobstore.ObjectPage{
		Objects:     objects,
		NextCursor:  aws.ToString(out.NextContinuationToken),
		IsTruncated: aws.ToBool(out.IsTruncated),
	}, nil
}

// ListObjectVersions paginates versions of one object.
func (p *Provider) ListObjectVersions(ctx context.Context, req blobstore.ListVersionsRequest) (blobstore.VersionPage, error) {
	if req.Key == "" {
		return blobstore.VersionPage{}, errors.New(p.errPrefix() + "key is required")
	}
	in := &s3.ListObjectVersionsInput{
		Bucket: aws.String(p.cfg.Bucket),
		Prefix: aws.String(req.Key),
	}
	if req.Cursor != "" {
		in.KeyMarker = aws.String(req.Cursor)
	}
	// Cap MaxKeys at 1000 (S3 API maximum per page).
	maxKeys := req.MaxKeys
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}
	in.MaxKeys = aws.Int32(maxKeys)
	out, err := p.client.ListObjectVersions(ctx, in)
	if err != nil {
		return blobstore.VersionPage{}, p.classifyError(err, req.Key)
	}
	versions := make([]blobstore.ObjectMeta, 0, len(out.Versions))
	for _, v := range out.Versions {
		if aws.ToString(v.Key) != req.Key {
			continue
		}
		versions = append(versions, blobstore.ObjectMeta{
			Key:             req.Key,
			VersionID:       aws.ToString(v.VersionId),
			Size:            aws.ToInt64(v.Size),
			VersioningState: blobstore.VersioningEnabled,
			WriteTime:       aws.ToTime(v.LastModified),
		})
	}
	return blobstore.VersionPage{
		Versions:    versions,
		NextCursor:  aws.ToString(out.NextKeyMarker),
		IsTruncated: aws.ToBool(out.IsTruncated),
	}, nil
}

// DeleteVersion removes a specific version.
func (p *Provider) DeleteVersion(ctx context.Context, ref blobstore.VersionedObjectRef) error {
	if ref.Key == "" {
		return errors.New(p.errPrefix() + "key is required")
	}
	_, err := p.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String(p.cfg.Bucket),
		Key:       aws.String(ref.Key),
		VersionId: aws.String(ref.VersionID),
	})
	if err != nil {
		return p.classifyError(err, ref.Key)
	}
	return nil
}

// ListMultipartUploads paginates in-progress multipart uploads.
func (p *Provider) ListMultipartUploads(ctx context.Context, req blobstore.ListUploadsRequest) (blobstore.UploadPage, error) {
	in := &s3.ListMultipartUploadsInput{
		Bucket: aws.String(p.cfg.Bucket),
	}
	if req.Prefix != "" {
		in.Prefix = aws.String(req.Prefix)
	}
	if req.Cursor != "" {
		in.KeyMarker = aws.String(req.Cursor)
	}
	out, err := p.client.ListMultipartUploads(ctx, in)
	if err != nil {
		return blobstore.UploadPage{}, p.classifyError(err, "")
	}
	uploads := make([]blobstore.MultipartUpload, 0, len(out.Uploads))
	for _, u := range out.Uploads {
		uploads = append(uploads, blobstore.MultipartUpload{
			Key:      aws.ToString(u.Key),
			UploadID: aws.ToString(u.UploadId),
		})
	}
	return blobstore.UploadPage{
		Uploads:    uploads,
		NextCursor: aws.ToString(out.NextKeyMarker),
	}, nil
}

// ListParts paginates parts of one multipart upload.
func (p *Provider) ListParts(ctx context.Context, upload blobstore.MultipartUpload, cursor string) (blobstore.PartPage, error) {
	in := &s3.ListPartsInput{
		Bucket:   aws.String(p.cfg.Bucket),
		Key:      aws.String(upload.Key),
		UploadId: aws.String(upload.UploadID),
	}
	if cursor != "" {
		in.PartNumberMarker = aws.String(cursor)
	}
	out, err := p.client.ListParts(ctx, in)
	if err != nil {
		return blobstore.PartPage{}, p.classifyError(err, upload.Key)
	}
	parts := make([]blobstore.CompletedPart, 0, len(out.Parts))
	for _, part := range out.Parts {
		parts = append(parts, blobstore.CompletedPart{
			PartNumber:     aws.ToInt32(part.PartNumber),
			Size:           aws.ToInt64(part.Size),
			ChecksumSHA256: aws.ToString(part.ChecksumSHA256),
		})
	}
	return blobstore.PartPage{
		Parts:      parts,
		NextCursor: aws.ToString(out.NextPartNumberMarker),
	}, nil
}

// DeleteBatch removes a batch of versions. S3 DeleteObjects API
// accepts at most 1000 objects per request; larger batches are
// chunked automatically.
func (p *Provider) DeleteBatch(ctx context.Context, refs []blobstore.VersionedObjectRef) (blobstore.BatchDeleteResult, error) {
	if len(refs) == 0 {
		return blobstore.BatchDeleteResult{}, nil
	}
	const maxBatchSize = 1000
	result := blobstore.BatchDeleteResult{}
	for start := 0; start < len(refs); start += maxBatchSize {
		end := start + maxBatchSize
		if end > len(refs) {
			end = len(refs)
		}
		chunk := refs[start:end]
		pageResult, err := p.deleteBatchChunk(ctx, chunk)
		if err != nil {
			return result, err
		}
		result.Deleted = append(result.Deleted, pageResult.Deleted...)
		result.Errors = append(result.Errors, pageResult.Errors...)
	}
	return result, nil
}

// deleteBatchChunk sends a single DeleteObjects request for up to
// 1000 refs.
func (p *Provider) deleteBatchChunk(ctx context.Context, refs []blobstore.VersionedObjectRef) (blobstore.BatchDeleteResult, error) {
	objects := make([]s3types.ObjectIdentifier, 0, len(refs))
	for _, ref := range refs {
		objects = append(objects, s3types.ObjectIdentifier{
			Key:       aws.String(ref.Key),
			VersionId: aws.String(ref.VersionID),
		})
	}
	out, err := p.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(p.cfg.Bucket),
		Delete: &s3types.Delete{Objects: objects},
	})
	if err != nil {
		return blobstore.BatchDeleteResult{}, p.classifyError(err, "")
	}
	result := blobstore.BatchDeleteResult{}
	for _, d := range out.Deleted {
		result.Deleted = append(result.Deleted, blobstore.VersionedObjectRef{
			Key:       aws.ToString(d.Key),
			VersionID: aws.ToString(d.VersionId),
		})
	}
	for _, e := range out.Errors {
		result.Errors = append(result.Errors, blobstore.BatchDeleteError{
			Ref: blobstore.VersionedObjectRef{
				Key:       aws.ToString(e.Key),
				VersionID: aws.ToString(e.VersionId),
			},
			Error: aws.ToString(e.Message),
		})
	}
	return result, nil
}

// --- helpers ---

func (p *Provider) classifyError(err error, key string) error {
	if err == nil {
		return nil
	}
	// Check typed SDK errors first — these are reliable and not
	// subject to string-matching false positives.
	var noSuchKey *s3types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return fmt.Errorf(p.errPrefix()+"%w: key %q (%v)", blobstore.ErrNotFound, key, err)
	}
	var noSuchBucket *s3types.NoSuchBucket
	if errors.As(err, &noSuchBucket) {
		return fmt.Errorf(p.errPrefix()+"%w: bucket %q (%v)", blobstore.ErrNotFound, p.cfg.Bucket, err)
	}
	var noSuchUpload *s3types.NoSuchUpload
	if errors.As(err, &noSuchUpload) {
		return fmt.Errorf(p.errPrefix()+"%w: upload for key %q (%v)", blobstore.ErrNotFound, key, err)
	}
	var notFound *s3types.NotFound
	if errors.As(err, &notFound) {
		return fmt.Errorf(p.errPrefix()+"%w: key %q (%v)", blobstore.ErrNotFound, key, err)
	}
	// Fall back to HTTP status code via smithy's ResponseError, which
	// wraps the transport-level response. This catches S3-compatible
	// endpoints that return 404/412/503 without a typed error body.
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		switch respErr.HTTPStatusCode() {
		case 404:
			return fmt.Errorf(p.errPrefix()+"%w: key %q (%v)", blobstore.ErrNotFound, key, err)
		case 412:
			return fmt.Errorf(p.errPrefix()+"%w: key %q (%v)", blobstore.ErrPreconditionFailed, key, err)
		case 503:
			return fmt.Errorf(p.errPrefix()+"%w: key %q (%v)", blobstore.ErrThrottled, key, err)
		}
	}
	// Object Lock / retention conflicts surface as typed errors on
	// real S3 but as string-matched messages on some S3-compatible
	// endpoints. Keep a narrow string check as a last resort for
	// retention conflicts only.
	msg := err.Error()
	if strings.Contains(msg, "ObjectLock") || strings.Contains(strings.ToLower(msg), "retention") {
		return fmt.Errorf(p.errPrefix()+"%w: key %q (%v)", blobstore.ErrRetentionConflict, key, err)
	}
	return fmt.Errorf(p.errPrefix()+"key %q: %w", key, err)
}

// isClientError returns true for errors that are expected client-side
// responses (not-found, precondition failed, retention conflict,
// checksum mismatch) and should NOT trip the circuit breaker. Only
// server errors (5xx), throttling (503), and network failures
// indicate Wasabi is down and should count toward the breaker.
func isClientError(err error) bool {
	return errors.Is(err, blobstore.ErrNotFound) ||
		errors.Is(err, blobstore.ErrPreconditionFailed) ||
		errors.Is(err, blobstore.ErrRetentionConflict) ||
		errors.Is(err, blobstore.ErrChecksumMismatch)
}

func headOutputToMeta(key string, out *s3.HeadObjectOutput) blobstore.ObjectMeta {
	return blobstore.ObjectMeta{
		Key:             key,
		VersionID:       aws.ToString(out.VersionId),
		VersioningState: blobstore.VersioningEnabled,
		Size:            aws.ToInt64(out.ContentLength),
		ChecksumSHA256:  aws.ToString(out.ChecksumSHA256),
		WriteTime:       aws.ToTime(out.LastModified),
	}
}

func applyRetentionToPut(in *s3.PutObjectInput, spec blobstore.RetentionSpec) {
	if spec.Mode == blobstore.RetentionCompliance {
		in.ObjectLockMode = s3types.ObjectLockMode("COMPLIANCE")
	} else if spec.Mode == blobstore.RetentionGovernance {
		in.ObjectLockMode = s3types.ObjectLockMode("GOVERNANCE")
	}
	if !spec.RetainUntil.IsZero() {
		in.ObjectLockRetainUntilDate = aws.Time(spec.RetainUntil)
	}
}

func applyRetentionToCreateMultipart(in *s3.CreateMultipartUploadInput, spec blobstore.RetentionSpec) {
	if spec.Mode == blobstore.RetentionCompliance {
		in.ObjectLockMode = s3types.ObjectLockMode("COMPLIANCE")
	} else if spec.Mode == blobstore.RetentionGovernance {
		in.ObjectLockMode = s3types.ObjectLockMode("GOVERNANCE")
	}
	if !spec.RetainUntil.IsZero() {
		in.ObjectLockRetainUntilDate = aws.Time(spec.RetainUntil)
	}
}

func applyRetentionToCopy(in *s3.CopyObjectInput, spec blobstore.RetentionSpec) {
	if spec.Mode == blobstore.RetentionCompliance {
		in.ObjectLockMode = s3types.ObjectLockMode("COMPLIANCE")
	} else if spec.Mode == blobstore.RetentionGovernance {
		in.ObjectLockMode = s3types.ObjectLockMode("GOVERNANCE")
	}
	if !spec.RetainUntil.IsZero() {
		in.ObjectLockRetainUntilDate = aws.Time(spec.RetainUntil)
	}
}

func formatRange(r *blobstore.ByteRange) string {
	if r.End < 0 {
		return "bytes=" + strconv.FormatInt(r.Start, 10) + "-"
	}
	return "bytes=" + strconv.FormatInt(r.Start, 10) + "-" + strconv.FormatInt(r.End, 10)
}

func computeWriteResultHash(provider, bucket, key, versionID string, state blobstore.BucketVersioningState, size int64, sum string) string {
	if provider == "" {
		provider = "s3"
	}
	h := sha256.New()
	fmt.Fprintf(h, "%s/v1\x00%s\x00%s\x00%s\x00%s\x00%d\x00%s", provider, bucket, key, versionID, state, size, sum)
	return hex.EncodeToString(h.Sum(nil))
}

var (
	_ blobstore.BlobStore     = (*Provider)(nil)
	_ blobstore.BlobInventory = (*Provider)(nil)
)
