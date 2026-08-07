package worker_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/kchat/drive/internal/blobio"
	"github.com/kchat/drive/internal/config"
	"github.com/kchat/drive/internal/worker"
	"github.com/kchat/drive/pkg/blobstore/local_fs_dev"
	"github.com/kchat/drive/pkg/hotcache"
)

func TestNewDevModeNoJobs(t *testing.T) {
	cfg := &config.WorkerConfig{Env: "dev"}
	w, err := worker.New(cfg, slog.Default())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if len(w.Jobs()) != 0 {
		t.Errorf("dev mode jobs = %d, want 0", len(w.Jobs()))
	}
}

func TestNewWithDepsHasJobs(t *testing.T) {
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
	statusStore := blobio.NewMemoryStatusStore()
	pipeline := blobio.NewWithStatusStore(cache, store, statusStore, slog.Default())
	deps := worker.Deps{
		Store:    store,
		Pipeline: pipeline,
		Cache:    cache,
	}
	cfg := &config.WorkerConfig{
		Env:                 "production",
		PostgresDSN:         "postgres://localhost/kdrive",
		BackupCron:          "17 3 * * *",
		BackupRetentionDays: 14,
	}
	// MetaDB is nil; jobs that need it will handle nil gracefully
	// in their runOnce methods.
	w, err := worker.NewWithDeps(cfg, deps, slog.Default())
	if err != nil {
		t.Fatalf("NewWithDeps: %v", err)
	}
	jobs := w.Jobs()
	if len(jobs) != 5 {
		t.Errorf("production jobs = %d, want 5", len(jobs))
	}
	names := map[string]bool{}
	for _, j := range jobs {
		names[j.Name()] = true
	}
	for _, want := range []string{"promotion", "repair", "purge", "backup", "guardrail_rollup"} {
		if !names[want] {
			t.Errorf("missing job: %s", want)
		}
	}
}

func TestStartStopDevMode(t *testing.T) {
	cfg := &config.WorkerConfig{Env: "dev"}
	w, err := worker.New(cfg, slog.Default())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- w.Start(ctx)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Errorf("Start returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("Start did not return after cancel")
	}
}

func TestPromotionJobRunOnce(t *testing.T) {
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
	statusStore := blobio.NewMemoryStatusStore()
	pipeline := blobio.NewWithStatusStore(cache, store, statusStore, slog.Default())

	// Write a blob to the pipeline (CACHED state).
	ctx := context.Background()
	blobID := "test-promote-blob"
	body := []byte("promote me")
	if _, err := pipeline.Write(ctx, blobID, body, blobio.WriteOptions{}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify it's CACHED.
	status, _ := pipeline.Status(ctx, blobID)
	if status.CommitState != blobio.CommitCached {
		t.Fatalf("expected CACHED, got %s", status.CommitState)
	}

	// Run the promotion job's runOnce by calling Promote directly.
	if err := pipeline.Promote(ctx, blobID); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	status, _ = pipeline.Status(ctx, blobID)
	if status.CommitState != blobio.CommitDurable {
		t.Errorf("expected COMMITTED_DURABLE, got %s", status.CommitState)
	}
}
