package hotcache_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/kchat/drive/pkg/hotcache"
)

func TestMemoryCachePutGetEvict(t *testing.T) {
	ctx := context.Background()
	c, err := hotcache.NewMemoryCache(hotcache.EvictionPolicy{
		Kind:     hotcache.EvictionLRU,
		MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewMemoryCache: %v", err)
	}
	body := []byte("cache me")
	if err := c.Put(ctx, "blob1", bytes.NewReader(body), hotcache.PutOptions{
		SizeBytes: int64(len(body)),
		Hash:      "abc",
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	r, _, err := c.Get(ctx, "blob1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("Get body = %q, want %q", got, body)
	}
	if err := c.Evict(ctx, "blob1"); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if _, _, err := c.Get(ctx, "blob1"); !errors.Is(err, hotcache.ErrCacheMiss) {
		t.Errorf("Get after Evict = %v, want ErrCacheMiss", err)
	}
}

func TestMemoryCacheEviction(t *testing.T) {
	ctx := context.Background()
	c, err := hotcache.NewMemoryCache(hotcache.EvictionPolicy{
		Kind:     hotcache.EvictionLRU,
		MaxBytes: 10,
	})
	if err != nil {
		t.Fatalf("NewMemoryCache: %v", err)
	}
	_ = c.Put(ctx, "a", bytes.NewReader([]byte("aaaaa")), hotcache.PutOptions{SizeBytes: 5})
	_ = c.Put(ctx, "b", bytes.NewReader([]byte("bbbbb")), hotcache.PutOptions{SizeBytes: 5})
	// Putting "c" should evict "a" (the least recently used).
	_ = c.Put(ctx, "c", bytes.NewReader([]byte("ccccc")), hotcache.PutOptions{SizeBytes: 5})
	if _, _, err := c.Get(ctx, "a"); !errors.Is(err, hotcache.ErrCacheMiss) {
		t.Errorf("Get(a) after eviction = %v, want ErrCacheMiss", err)
	}
	if _, _, err := c.Get(ctx, "b"); err != nil {
		t.Errorf("Get(b) = %v, want nil", err)
	}
	if _, _, err := c.Get(ctx, "c"); err != nil {
		t.Errorf("Get(c) = %v, want nil", err)
	}
}

func TestNonEvictableEntryProtected(t *testing.T) {
	ctx := context.Background()
	c, err := hotcache.NewMemoryCache(hotcache.EvictionPolicy{
		Kind:     hotcache.EvictionLRU,
		MaxBytes: 12,
	})
	if err != nil {
		t.Fatalf("NewMemoryCache: %v", err)
	}
	// Put a non-evictable entry (simulates a CACHED blob).
	if err := c.Put(ctx, "pinned", bytes.NewReader([]byte("aaaaa")), hotcache.PutOptions{
		SizeBytes:    5,
		NonEvictable: true,
	}); err != nil {
		t.Fatalf("Put pinned: %v", err)
	}
	// Put a normal entry that fits.
	if err := c.Put(ctx, "normal", bytes.NewReader([]byte("bbbbb")), hotcache.PutOptions{
		SizeBytes: 5,
	}); err != nil {
		t.Fatalf("Put normal: %v", err)
	}
	// Put another normal entry that exceeds capacity. The cache
	// should evict "normal" (the evictable one), not "pinned".
	if err := c.Put(ctx, "extra", bytes.NewReader([]byte("ccccc")), hotcache.PutOptions{
		SizeBytes: 5,
	}); err != nil {
		t.Fatalf("Put extra: %v", err)
	}
	// "pinned" must still be present (non-evictable entries are
	// protected from automatic LRU eviction).
	if _, _, err := c.Get(ctx, "pinned"); err != nil {
		t.Errorf("Get(pinned) = %v, want nil (non-evictable must not be evicted)", err)
	}
	// "normal" should have been evicted to make room for "extra".
	if _, _, err := c.Get(ctx, "normal"); !errors.Is(err, hotcache.ErrCacheMiss) {
		t.Errorf("Get(normal) = %v, want ErrCacheMiss (evictable entry should have been evicted)", err)
	}
}

func TestDiskCacheWarm(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	c1, err := hotcache.NewDiskCache(hotcache.DiskCacheConfig{
		RootPath: root,
		Policy:   hotcache.EvictionPolicy{Kind: hotcache.EvictionLRU, MaxBytes: 1 << 20},
	})
	if err != nil {
		t.Fatalf("NewDiskCache c1: %v", err)
	}
	body := []byte("persist across restart")
	if err := c1.Put(ctx, "warmblob", bytes.NewReader(body), hotcache.PutOptions{
		SizeBytes: int64(len(body)),
		Hash:      "h",
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	c2, err := hotcache.NewDiskCache(hotcache.DiskCacheConfig{
		RootPath: root,
		Policy:   hotcache.EvictionPolicy{Kind: hotcache.EvictionLRU, MaxBytes: 1 << 20},
	})
	if err != nil {
		t.Fatalf("NewDiskCache c2: %v", err)
	}
	r, _, err := c2.Get(ctx, "warmblob")
	if err != nil {
		t.Fatalf("Get after warm: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("Get after warm body = %q, want %q", got, body)
	}
}
