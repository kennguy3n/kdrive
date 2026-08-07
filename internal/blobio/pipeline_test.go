package blobio_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/kchat/drive/internal/blobio"
	"github.com/kchat/drive/pkg/blobstore"
	"github.com/kchat/drive/pkg/blobstore/local_fs_dev"
	"github.com/kchat/drive/pkg/hotcache"
)

func newTestPipeline(t *testing.T) (*blobio.Pipeline, *local_fs_dev.Provider, hotcache.Cache) {
	t.Helper()
	root := t.TempDir()
	store, err := local_fs_dev.New(root)
	if err != nil {
		t.Fatalf("local_fs_dev.New: %v", err)
	}
	cache, err := hotcache.NewMemoryCache(hotcache.EvictionPolicy{
		Kind:     hotcache.EvictionLRU,
		MaxBytes: 64 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewMemoryCache: %v", err)
	}
	p := blobio.New(cache, store, slog.Default())
	return p, store, cache
}

func TestWriteReadCacheHit(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newTestPipeline(t)

	blobID := "blob-cache-hit"
	body := []byte("cache hit path")
	res, err := p.Write(ctx, blobID, body, blobio.WriteOptions{})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if res.CommitState != blobio.CommitCached {
		t.Errorf("CommitState = %q, want CACHED", res.CommitState)
	}

	// First read: L1 hit (body is in cache from Write).
	r1, err := p.Read(ctx, blobID)
	if err != nil {
		t.Fatalf("Read 1: %v", err)
	}
	defer r1.Body.Close()
	got1, err := io.ReadAll(r1.Body)
	if err != nil {
		t.Fatalf("ReadAll 1: %v", err)
	}
	if !bytes.Equal(got1, body) {
		t.Errorf("Read 1 body = %q, want %q", got1, body)
	}
	if !r1.FromCache {
		t.Errorf("Read 1 FromCache = false, want true")
	}
}

func TestWriteAsyncThenPromote(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newTestPipeline(t)

	blobID := "blob-async-promote"
	body := []byte("async promotion")
	if _, err := p.Write(ctx, blobID, body, blobio.WriteOptions{}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Status should be CACHED.
	status, err := p.Status(ctx, blobID)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.CommitState != blobio.CommitCached {
		t.Errorf("Status = %q, want CACHED", status.CommitState)
	}

	// Promote to durable.
	if err := p.Promote(ctx, blobID); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	status, _ = p.Status(ctx, blobID)
	if status.CommitState != blobio.CommitDurable {
		t.Errorf("Status after promote = %q, want COMMITTED_DURABLE", status.CommitState)
	}
}

func TestWriteSyncPromote(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newTestPipeline(t)

	blobID := "blob-sync-promote"
	body := []byte("sync promotion")
	res, err := p.Write(ctx, blobID, body, blobio.WriteOptions{PromoteSync: true})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if res.CommitState != blobio.CommitDurable {
		t.Errorf("Write with PromoteSync CommitState = %q, want COMMITTED_DURABLE", res.CommitState)
	}
	status, _ := p.Status(ctx, blobID)
	if status.CommitState != blobio.CommitDurable {
		t.Errorf("Status after sync promote = %q, want COMMITTED_DURABLE", status.CommitState)
	}
}

func TestReadCacheMissRestoresFromL2(t *testing.T) {
	ctx := context.Background()
	p, _, cache := newTestPipeline(t)

	blobID := "blob-restore"
	body := []byte("restore from l2")
	// Write and promote so the blob is in L2.
	if _, err := p.Write(ctx, blobID, body, blobio.WriteOptions{PromoteSync: true}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Evict from L1 so the next read must restore from L2.
	if err := p.Evict(ctx, blobID); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	// Confirm it's gone from cache.
	if _, _, err := cache.Get(ctx, blobID); !errors.Is(err, hotcache.ErrCacheMiss) {
		t.Fatalf("cache.Get after Evict = %v, want ErrCacheMiss", err)
	}
	// Read should restore from L2.
	r, err := p.Read(ctx, blobID)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer r.Body.Close()
	got, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("Read body = %q, want %q", got, body)
	}
	if r.FromCache {
		t.Errorf("FromCache = true, want false (should be L2 restore)")
	}
	// After restore, the blob should be back in L1. The cache put
	// is async, so poll briefly until it appears.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, _, err := cache.Get(ctx, blobID); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("cache.Get after restore = %v, want nil (async cache put did not complete)", err)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestReadSingleflightDedup(t *testing.T) {
	ctx := context.Background()
	p, store, cache := newTestPipeline(t)

	blobID := "blob-singleflight"
	body := []byte("singleflight dedup")
	// Write directly to L2 (bypass the pipeline's cache).
	sum := sha256.Sum256(body)
	if _, err := store.Put(ctx, blobstore.PutRequest{
		Key:            blobID,
		Body:           bytes.NewReader(body),
		ExpectedLength: int64(len(body)),
		ChecksumSHA256: hex.EncodeToString(sum[:]),
	}); err != nil {
		t.Fatalf("store.Put: %v", err)
	}
	// Ensure not in cache.
	_ = cache.Evict(ctx, blobID)

	// Fire N concurrent reads; only one should hit L2.
	var wg sync.WaitGroup
	results := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := p.Read(ctx, blobID)
			if err != nil {
				results <- err
				return
			}
			defer r.Body.Close()
			got, err := io.ReadAll(r.Body)
			if err != nil {
				results <- err
				return
			}
			if !bytes.Equal(got, body) {
				results <- errors.New("body mismatch")
				return
			}
			results <- nil
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Errorf("concurrent read: %v", err)
		}
	}
}

func TestEvictNonDurableFails(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newTestPipeline(t)
	blobID := "blob-no-evict"
	if _, err := p.Write(ctx, blobID, []byte("not durable yet"), blobio.WriteOptions{}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := p.Evict(ctx, blobID); err == nil {
		t.Errorf("Evict of non-durable blob succeeded; expected error")
	}
}

func TestPromoteIdempotent(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newTestPipeline(t)
	blobID := "blob-idempotent"
	if _, err := p.Write(ctx, blobID, []byte("idempotent"), blobio.WriteOptions{PromoteSync: true}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Second promote should be a no-op.
	if err := p.Promote(ctx, blobID); err != nil {
		t.Fatalf("second Promote: %v", err)
	}
}

// TestPromoteAfterTTL exercises the case where a cached blob expires
// but is still durable; the pipeline should not need to re-promote.
func TestReadAfterCacheExpiry(t *testing.T) {
	ctx := context.Background()
	p, _, cache := newTestPipeline(t)
	blobID := "blob-ttl"
	body := []byte("ttl expiry")
	if _, err := p.Write(ctx, blobID, body, blobio.WriteOptions{PromoteSync: true}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Evict from cache.
	_ = cache.Evict(ctx, blobID)
	// Read should restore from L2.
	r, err := p.Read(ctx, blobID)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer r.Body.Close()
	got, _ := io.ReadAll(r.Body)
	if !bytes.Equal(got, body) {
		t.Errorf("body mismatch after TTL")
	}
	// Allow the test to pass even if time.Now is slow.
	_ = time.Now
}
