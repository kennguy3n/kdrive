package hotcache

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// diskWriteBufPool reuses 128 KB write buffers across Put calls to
// avoid repeated allocation and GC pressure on large blob writes.
var diskWriteBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 128*1024)
		return &b
	},
}

// DiskCache is an NVMe / block-storage backed Cache.
//
// Blob bodies are written to {RootPath}/{shard}/{blobID}.bin and
// metadata is written to {RootPath}/{shard}/{blobID}.meta.json. A
// shard prefix (the first two bytes of the blobID) keeps any one
// directory from holding millions of files.
//
// The in-memory index is rebuilt from disk on Open so cache entries
// survive gateway restarts — the principal reason to run a disk cache
// over MemoryCache on production Linode nodes.
type DiskCache struct {
	mu sync.Mutex

	policy EvictionPolicy
	root   string

	hot   *list.List
	main  *list.List
	index map[string]*list.Element

	bytesUsed int64
	hotLimit  int64
	hotBytes  int64

	// inflightWrites tracks in-flight Put operations so Close can
	// wait for them to finish, preventing orphaned temp files.
	inflightWrites sync.WaitGroup

	stats Stats
	clock func() time.Time
}

// DiskCacheConfig captures the on-disk cache's tuning knobs.
type DiskCacheConfig struct {
	RootPath string
	Policy   EvictionPolicy
	Clock    func() time.Time
}

type metaFile struct {
	BlobID       string    `json:"blob_id"`
	SizeBytes    int64     `json:"size_bytes"`
	Hash         string    `json:"hash,omitempty"`
	StoredAt     time.Time `json:"stored_at"`
	ExpiresAt    time.Time `json:"expires_at,omitempty"`
	Pinned       bool      `json:"pinned,omitempty"`
	NonEvictable bool      `json:"non_evictable,omitempty"`
	HitCount     uint64    `json:"hit_count,omitempty"`
}

type diskEntry struct {
	blobID       string
	sizeBytes    int64
	hash         string
	storedAt     time.Time
	lastAccess   time.Time
	expiresAt    time.Time
	pinned       bool
	nonEvictable bool
	hits         uint64
}

// NewDiskCache constructs (and warms from disk) a DiskCache.
func NewDiskCache(cfg DiskCacheConfig) (*DiskCache, error) {
	if cfg.RootPath == "" {
		return nil, errors.New("hotcache: disk cache root_path is required")
	}
	if cfg.Policy.Kind == "" {
		cfg.Policy.Kind = EvictionLRU
	}
	if err := cfg.Policy.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.RootPath, 0o755); err != nil {
		return nil, fmt.Errorf("hotcache: create cache root %q: %w", cfg.RootPath, err)
	}
	c := &DiskCache{
		policy: cfg.Policy,
		root:   cfg.RootPath,
		hot:    list.New(),
		main:   list.New(),
		index:  map[string]*list.Element{},
		stats:  Stats{BytesLimit: cfg.Policy.MaxBytes},
		clock:  cfg.Clock,
	}
	if c.clock == nil {
		c.clock = time.Now
	}
	if cfg.Policy.Kind == EvictionLRUHotPin && cfg.Policy.HotRegionFraction > 0 && cfg.Policy.MaxBytes > 0 {
		c.hotLimit = int64(float64(cfg.Policy.MaxBytes) * cfg.Policy.HotRegionFraction)
	}
	if err := c.warm(); err != nil {
		return nil, err
	}
	return c, nil
}

// Get returns a reader for the cached blob, or ErrCacheMiss.
func (c *DiskCache) Get(_ context.Context, blobID string) (io.ReadCloser, Metadata, error) {
	if err := validateBlobID(blobID); err != nil {
		return nil, Metadata{}, err
	}
	c.mu.Lock()
	el, ok := c.index[blobID]
	if !ok {
		c.stats.Misses++
		c.mu.Unlock()
		return nil, Metadata{}, ErrCacheMiss
	}
	entry := el.Value.(*diskEntry)
	now := c.clock()
	if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
		c.removeLocked(el)
		c.stats.Misses++
		c.mu.Unlock()
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
	bodyPath := c.bodyPath(blobID)
	md := Metadata{
		BlobID:       entry.blobID,
		SizeBytes:    entry.sizeBytes,
		Hash:         entry.hash,
		StoredAt:     entry.storedAt,
		LastAccess:   entry.lastAccess,
		HitCount:     entry.hits,
		Pinned:       entry.pinned,
		NonEvictable: entry.nonEvictable,
	}
	// Open the file while still holding the lock to prevent a TOCTOU
	// race where another goroutine evicts (and deletes) the file
	// between unlock and open.
	f, err := os.Open(bodyPath)
	if err != nil {
		if el2, ok := c.index[blobID]; ok && el2 == el {
			c.removeLocked(el2)
		}
		if c.stats.Hits > 0 {
			c.stats.Hits--
		}
		c.stats.Misses++
		c.mu.Unlock()
		return nil, Metadata{}, ErrCacheMiss
	}
	c.mu.Unlock()
	return f, md, nil
}

// Put stores a blob in the cache.
func (c *DiskCache) Put(_ context.Context, blobID string, r io.Reader, opts PutOptions) error {
	if err := validateBlobID(blobID); err != nil {
		return err
	}
	if r == nil {
		return errors.New("hotcache: reader is required")
	}
	c.inflightWrites.Add(1)
	defer c.inflightWrites.Done()
	shardDir := c.shardDir(blobID)
	if err := os.MkdirAll(shardDir, 0o755); err != nil {
		return fmt.Errorf("hotcache: create shard dir: %w", err)
	}
	tmp, err := os.CreateTemp(shardDir, blobID+".*.tmp")
	if err != nil {
		return fmt.Errorf("hotcache: create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	// Use a pooled 128 KB buffer instead of the default 32 KB to
	// reduce syscall overhead on large blob writes.
	bufPtr := diskWriteBufPool.Get().(*[]byte)
	buf := *bufPtr
	defer diskWriteBufPool.Put(bufPtr)
	size, copyErr := io.CopyBuffer(tmp, r, buf)
	closeErr := tmp.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("hotcache: write body: %w", copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("hotcache: close body: %w", closeErr)
	}
	if c.policy.MaxBytes > 0 && size > c.policy.MaxBytes {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("hotcache: blob %d bytes exceeds cache capacity %d", size, c.policy.MaxBytes)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.index[blobID]; ok {
		c.removeLocked(existing)
	}
	now := c.clock()
	pin := opts.PinHot && c.policy.Kind == EvictionLRUHotPin
	entry := &diskEntry{
		blobID:       blobID,
		sizeBytes:    size,
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
	bodyPath := c.bodyPath(blobID)
	// fsync the temp file before rename so the body is durable on
	// disk before the atomic publish.
	if f, ferr := os.OpenFile(tmpPath, os.O_WRONLY, 0o644); ferr == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	if err := os.Rename(tmpPath, bodyPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("hotcache: publish body: %w", err)
	}
	if err := writeMeta(c.metaPath(blobID), entry); err != nil {
		_ = os.Remove(bodyPath)
		return err
	}
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

// Evict removes a blob. It is idempotent.
func (c *DiskCache) Evict(_ context.Context, blobID string) error {
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
func (c *DiskCache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// Close waits for all in-flight Put operations to finish, then
// sweeps any remaining .tmp files. The gateway calls Close during
// graceful shutdown so temp files are not orphaned by a SIGTERM
// mid-write.
func (c *DiskCache) Close() error {
	c.inflightWrites.Wait()
	// Sweep leftover .tmp files from crashed writes.
	return filepath.Walk(c.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".tmp") {
			if rmErr := os.Remove(path); rmErr != nil {
				return rmErr
			}
		}
		return nil
	})
}

func (c *DiskCache) warm() error {
	entries, err := readShards(c.root)
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].storedAt.Before(entries[j].storedAt)
	})
	now := c.clock()
	for _, e := range entries {
		if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
			_ = os.Remove(c.bodyPath(e.blobID))
			_ = os.Remove(c.metaPath(e.blobID))
			continue
		}
		e.lastAccess = e.storedAt
		var el *list.Element
		if e.pinned && c.policy.Kind == EvictionLRUHotPin {
			el = c.hot.PushFront(e)
			c.hotBytes += e.sizeBytes
		} else {
			e.pinned = false
			el = c.main.PushFront(e)
		}
		c.index[e.blobID] = el
		c.bytesUsed += e.sizeBytes
	}
	c.stats.Entries = int64(len(c.index))
	c.stats.BytesUsed = c.bytesUsed
	if c.policy.MaxBytes > 0 {
		c.evictMainLocked(0)
	}
	return nil
}

func (c *DiskCache) evictMainLocked(incoming int64) {
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
		for back != nil && back.Value.(*diskEntry).nonEvictable {
			back = back.Prev()
		}
		if back == nil {
			return
		}
		c.removeLocked(back)
	}
}

func (c *DiskCache) evictHotLocked(incoming int64) {
	if c.hotLimit <= 0 {
		return
	}
	for c.hotBytes+incoming > c.hotLimit {
		back := c.hot.Back()
		if back == nil {
			return
		}
		for back != nil && back.Value.(*diskEntry).nonEvictable {
			back = back.Prev()
		}
		if back == nil {
			return
		}
		c.removeLocked(back)
	}
}

func (c *DiskCache) removeLocked(el *list.Element) {
	entry := el.Value.(*diskEntry)
	if entry.pinned {
		c.hot.Remove(el)
		c.hotBytes -= entry.sizeBytes
	} else {
		c.main.Remove(el)
	}
	delete(c.index, entry.blobID)
	c.bytesUsed -= entry.sizeBytes
	c.stats.Entries = int64(len(c.index))
	c.stats.BytesUsed = c.bytesUsed
	c.stats.Evictions++
	_ = os.Remove(c.bodyPath(entry.blobID))
	_ = os.Remove(c.metaPath(entry.blobID))
}

// validateBlobID rejects blobIDs that could escape the cache root
// via path traversal. BlobIDs are expected to be opaque hex or
// base64 content-addressed keys.
func validateBlobID(blobID string) error {
	if blobID == "" {
		return errors.New("hotcache: blobID is required")
	}
	if strings.ContainsAny(blobID, `/\`) {
		return fmt.Errorf("hotcache: blobID %q must not contain path separators", blobID)
	}
	if blobID == "." || blobID == ".." || strings.Contains(blobID, "..") {
		return fmt.Errorf("hotcache: blobID %q must not be a relative path component", blobID)
	}
	return nil
}

func (c *DiskCache) shardDir(blobID string) string {
	return filepath.Join(c.root, shardOf(blobID))
}

func (c *DiskCache) bodyPath(blobID string) string {
	return filepath.Join(c.shardDir(blobID), blobID+".bin")
}

func (c *DiskCache) metaPath(blobID string) string {
	return filepath.Join(c.shardDir(blobID), blobID+".meta.json")
}

func shardOf(blobID string) string {
	if len(blobID) < 2 {
		return "_"
	}
	return blobID[:2]
}

func writeMeta(path string, e *diskEntry) error {
	body, err := json.Marshal(metaFile{
		BlobID:       e.blobID,
		SizeBytes:    e.sizeBytes,
		Hash:         e.hash,
		StoredAt:     e.storedAt,
		ExpiresAt:    e.expiresAt,
		Pinned:       e.pinned,
		NonEvictable: e.nonEvictable,
		HitCount:     e.hits,
	})
	if err != nil {
		return fmt.Errorf("hotcache: marshal meta: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return fmt.Errorf("hotcache: write meta: %w", err)
	}
	// fsync the temp file before rename so the metadata is durable
	// on disk before the atomic publish.
	if f, ferr := os.OpenFile(tmp, os.O_WRONLY, 0o644); ferr == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("hotcache: publish meta: %w", err)
	}
	return nil
}

func readShards(root string) ([]*diskEntry, error) {
	shards, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("hotcache: read root %q: %w", root, err)
	}
	var out []*diskEntry
	for _, sd := range shards {
		if !sd.IsDir() {
			continue
		}
		shardPath := filepath.Join(root, sd.Name())
		files, err := os.ReadDir(shardPath)
		if err != nil {
			return nil, fmt.Errorf("hotcache: read shard %q: %w", shardPath, err)
		}
		bodies := map[string]struct{}{}
		metas := map[string]string{}
		for _, f := range files {
			name := f.Name()
			switch {
			case hasSuffix(name, ".bin"):
				bodies[name[:len(name)-len(".bin")]] = struct{}{}
			case hasSuffix(name, ".meta.json"):
				metas[name[:len(name)-len(".meta.json")]] = filepath.Join(shardPath, name)
			case hasSuffix(name, ".tmp"):
				_ = os.Remove(filepath.Join(shardPath, name))
			}
		}
		for blobID, metaPath := range metas {
			if _, ok := bodies[blobID]; !ok {
				_ = os.Remove(metaPath)
				continue
			}
			entry, err := loadMeta(metaPath)
			if err != nil {
				_ = os.Remove(metaPath)
				_ = os.Remove(filepath.Join(shardPath, blobID+".bin"))
				continue
			}
			out = append(out, entry)
			delete(bodies, blobID)
		}
		for orphan := range bodies {
			_ = os.Remove(filepath.Join(shardPath, orphan+".bin"))
		}
	}
	return out, nil
}

func loadMeta(path string) (*diskEntry, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m metaFile
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	return &diskEntry{
		blobID:       m.BlobID,
		sizeBytes:    m.SizeBytes,
		hash:         m.Hash,
		storedAt:     m.StoredAt,
		expiresAt:    m.ExpiresAt,
		pinned:       m.Pinned,
		nonEvictable: m.NonEvictable,
		hits:         m.HitCount,
	}, nil
}

func hasSuffix(s, suf string) bool {
	return len(s) >= len(suf) && s[len(s)-len(suf):] == suf
}

var _ Cache = (*DiskCache)(nil)
