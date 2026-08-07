// Package hotcache defines the L1 hot object cache interface that
// sits in front of the Wasabi durable origin. The interface is ported
// from zk-object-fabric/cache/hot_object_cache and adapted for
// KChat Drive's blob-keyed caching.
//
// The cache stores ciphertext, not plaintext. It is keyed by immutable
// blob ID (chunk or manifest) so that range-aligned encrypted chunks
// can be served directly without reconstruction. Auth is checked
// before cache lookup; cache by immutable blob ID, independent of
// bearer token (§15.4).
package hotcache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

// ErrCacheMiss is returned by Get when a blob is not present.
var ErrCacheMiss = errors.New("hotcache: miss")

// Cache is the interface implemented by the L1 hot cache.
type Cache interface {
	// Get returns a reader for the cached blob. It returns
	// ErrCacheMiss if the blob is not in the cache.
	Get(ctx context.Context, blobID string) (io.ReadCloser, Metadata, error)
	// Put stores a blob in the cache. Implementations may stream the
	// body to disk; they MUST record size and hash on completion.
	Put(ctx context.Context, blobID string, r io.Reader, opts PutOptions) error
	// Evict removes a blob from the cache. It is idempotent.
	Evict(ctx context.Context, blobID string) error
	// Stats reports cache-wide counters.
	Stats() Stats
	// Close releases resources associated with the cache. It waits
	// for in-flight writes to finish (disk cache) or is a no-op
	// (memory cache). The gateway calls Close during graceful
	// shutdown so temp files are not orphaned.
	Close() error
}

// PutOptions carries per-entry hints for the cache writer.
type PutOptions struct {
	SizeBytes int64
	Hash      string
	TTL       time.Duration
	PinHot    bool
	// NonEvictable, when true, prevents the entry from being evicted
	// by automatic LRU eviction. The entry can still be removed
	// explicitly via Evict. This is used by the pipeline to protect
	// CACHED (not yet durable) blobs from being lost under memory
	// pressure. Once the blob becomes COMMITTED_DURABLE, the caller
	// re-puts the entry with NonEvictable=false.
	NonEvictable bool
}

// Metadata describes an in-cache entry.
type Metadata struct {
	BlobID       string
	SizeBytes    int64
	Hash         string
	StoredAt     time.Time
	LastAccess   time.Time
	HitCount     uint64
	Pinned       bool
	NonEvictable bool
}

// Stats is the aggregate cache snapshot.
type Stats struct {
	Entries    int64
	BytesUsed  int64
	BytesLimit int64
	Hits       uint64
	Misses     uint64
	Evictions  uint64
}

// EvictionPolicyKind names the eviction algorithm.
type EvictionPolicyKind string

const (
	EvictionLRU       EvictionPolicyKind = "lru"
	EvictionLRUHotPin EvictionPolicyKind = "lru_hot_pin"
)

// EvictionPolicy configures the eviction algorithm for a cache tier.
type EvictionPolicy struct {
	Kind                EvictionPolicyKind
	MaxBytes            int64
	HotRegionFraction   float64
	HotDemotionHitCount uint64
	TTL                 time.Duration
}

// DefaultEvictionPolicy returns LRU with hot-pin support and a 10%
// hot region.
func DefaultEvictionPolicy(maxBytes int64) EvictionPolicy {
	return EvictionPolicy{
		Kind:                EvictionLRUHotPin,
		MaxBytes:            maxBytes,
		HotRegionFraction:   0.1,
		HotDemotionHitCount: 1,
	}
}

// Validate performs structural checks on an eviction policy.
func (e EvictionPolicy) Validate() error {
	switch e.Kind {
	case EvictionLRU, EvictionLRUHotPin:
	case "":
		return fmt.Errorf("hotcache: eviction policy kind is required")
	default:
		return fmt.Errorf("hotcache: unknown eviction kind %q", e.Kind)
	}
	if e.MaxBytes < 0 {
		return fmt.Errorf("hotcache: eviction max_bytes must be non-negative")
	}
	if e.HotRegionFraction < 0 || e.HotRegionFraction >= 1 {
		return fmt.Errorf("hotcache: eviction hot_region_fraction must be in [0, 1)")
	}
	if e.TTL < 0 {
		return fmt.Errorf("hotcache: eviction ttl must be non-negative")
	}
	if e.Kind == EvictionLRU && (e.HotRegionFraction != 0 || e.HotDemotionHitCount != 0) {
		return fmt.Errorf("hotcache: hot-pin fields only apply to eviction kind %q", EvictionLRUHotPin)
	}
	return nil
}
