package hotcache

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// MemoryCache is the in-memory Cache implementation. It uses a
// doubly-linked list keyed by a hash map for O(1) LRU bookkeeping
// and supports a hot-pin region per EvictionPolicy.
//
// The implementation buffers blob bodies in memory. It backs the L1
// hot cache on small gateways and tests; an NVMe-backed disk cache
// sits behind the same interface for production.
type MemoryCache struct {
	mu        sync.Mutex
	policy    EvictionPolicy
	hot       *list.List
	main      *list.List
	index     map[string]*list.Element
	bytesUsed int64
	hotLimit  int64
	hotBytes  int64
	stats     Stats
	clock     func() time.Time
}

type memEntry struct {
	blobID       string
	body         []byte
	hash         string
	storedAt     time.Time
	lastAccess   time.Time
	hits         uint64
	pinned       bool
	nonEvictable bool
	expiresAt    time.Time
}

// NewMemoryCache builds a MemoryCache honouring the eviction policy.
func NewMemoryCache(policy EvictionPolicy) (*MemoryCache, error) {
	if policy.Kind == "" {
		policy.Kind = EvictionLRU
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	c := &MemoryCache{
		policy: policy,
		hot:    list.New(),
		main:   list.New(),
		index:  map[string]*list.Element{},
		stats:  Stats{BytesLimit: policy.MaxBytes},
		clock:  time.Now,
	}
	if policy.Kind == EvictionLRUHotPin && policy.HotRegionFraction > 0 && policy.MaxBytes > 0 {
		c.hotLimit = int64(float64(policy.MaxBytes) * policy.HotRegionFraction)
	}
	return c, nil
}

// Get returns a reader for the cached blob, or ErrCacheMiss.
func (c *MemoryCache) Get(_ context.Context, blobID string) (io.ReadCloser, Metadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.index[blobID]
	if !ok {
		c.stats.Misses++
		return nil, Metadata{}, ErrCacheMiss
	}
	entry := el.Value.(*memEntry)
	now := c.clock()
	if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
		c.removeLocked(el)
		c.stats.Misses++
		return nil, Metadata{}, ErrCacheMiss
	}
	entry.hits++
	entry.lastAccess = now
	c.stats.Hits++
	if entry.pinned {
		c.hot.MoveToFront(el)
	} else {
		c.main.MoveToFront(el)
	}
	md := Metadata{
		BlobID:       entry.blobID,
		SizeBytes:    int64(len(entry.body)),
		Hash:         entry.hash,
		StoredAt:     entry.storedAt,
		LastAccess:   entry.lastAccess,
		HitCount:     entry.hits,
		Pinned:       entry.pinned,
		NonEvictable: entry.nonEvictable,
	}
	// Return a copy so the caller cannot mutate the cached body.
	return io.NopCloser(bytes.NewReader(append([]byte(nil), entry.body...))), md, nil
}

// Put stores a blob in the cache.
func (c *MemoryCache) Put(_ context.Context, blobID string, r io.Reader, opts PutOptions) error {
	if blobID == "" {
		return errors.New("hotcache: blob_id is required")
	}
	if r == nil {
		return errors.New("hotcache: reader is required")
	}
	// Pre-check size if provided to avoid OOM on oversized blobs.
	if opts.SizeBytes > 0 && c.policy.MaxBytes > 0 && opts.SizeBytes > int64(c.policy.MaxBytes) {
		return fmt.Errorf("hotcache: blob %d bytes exceeds cache capacity %d", opts.SizeBytes, c.policy.MaxBytes)
	}
	// Use LimitReader as a safety net even when SizeBytes is not provided.
	var reader io.Reader = r
	if c.policy.MaxBytes > 0 {
		reader = io.LimitReader(r, int64(c.policy.MaxBytes)+1)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("hotcache: buffer blob: %w", err)
	}
	size := int64(len(data))
	if c.policy.MaxBytes > 0 && size > int64(c.policy.MaxBytes) {
		return fmt.Errorf("hotcache: blob %d bytes exceeds cache capacity %d", size, c.policy.MaxBytes)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.index[blobID]; ok {
		c.removeLocked(existing)
	}
	now := c.clock()
	pin := opts.PinHot && c.policy.Kind == EvictionLRUHotPin
	entry := &memEntry{
		blobID:       blobID,
		body:         data,
		hash:         opts.Hash,
		storedAt:     now,
		lastAccess:   now,
		pinned:       pin,
		nonEvictable: opts.NonEvictable,
	}
	if opts.TTL > 0 {
		entry.expiresAt = now.Add(opts.TTL)
	} else if c.policy.TTL > 0 {
		entry.expiresAt = now.Add(c.policy.TTL)
	}
	if pin {
		c.evictHotLocked(size)
	}
	c.evictMainLocked(size)
	var el *list.Element
	if pin {
		el = c.hot.PushFront(entry)
		c.hotBytes += size
	} else {
		el = c.main.PushFront(entry)
	}
	c.index[blobID] = el
	c.bytesUsed += size
	c.stats.Entries = int64(len(c.index))
	c.stats.BytesUsed = c.bytesUsed
	return nil
}

// Evict removes a blob. Missing blobs are silent.
func (c *MemoryCache) Evict(_ context.Context, blobID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.index[blobID]
	if !ok {
		return nil
	}
	c.removeLocked(el)
	return nil
}

// Stats returns a snapshot of cache-wide counters.
func (c *MemoryCache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// Close is a no-op for the memory cache. All data is in-process
// and will be lost on exit, which is expected for an in-memory cache.
func (c *MemoryCache) Close() error { return nil }

func (c *MemoryCache) evictMainLocked(incoming int64) {
	if c.policy.MaxBytes <= 0 {
		return
	}
	for c.bytesUsed+incoming > c.policy.MaxBytes {
		back := c.main.Back()
		if back == nil {
			back = c.hot.Back()
			if back == nil {
				return
			}
		}
		// Skip non-evictable entries (CACHED blobs that are the
		// only copy). Walk backwards until we find an evictable one.
		for back != nil && back.Value.(*memEntry).nonEvictable {
			back = back.Prev()
		}
		if back == nil {
			// All entries are non-evictable; cannot evict.
			return
		}
		c.removeLocked(back)
	}
}

func (c *MemoryCache) evictHotLocked(incoming int64) {
	if c.hotLimit <= 0 {
		return
	}
	for c.hotBytes+incoming > c.hotLimit {
		back := c.hot.Back()
		if back == nil {
			return
		}
		for back != nil && back.Value.(*memEntry).nonEvictable {
			back = back.Prev()
		}
		if back == nil {
			return
		}
		c.removeLocked(back)
	}
}

func (c *MemoryCache) removeLocked(el *list.Element) {
	entry := el.Value.(*memEntry)
	size := int64(len(entry.body))
	if entry.pinned {
		c.hot.Remove(el)
		c.hotBytes -= size
	} else {
		c.main.Remove(el)
	}
	delete(c.index, entry.blobID)
	c.bytesUsed -= size
	c.stats.Entries = int64(len(c.index))
	c.stats.BytesUsed = c.bytesUsed
	c.stats.Evictions++
}

var _ Cache = (*MemoryCache)(nil)
