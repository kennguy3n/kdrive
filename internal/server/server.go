// Package server wires the drive-gateway HTTP handler. It merges the
// API, edge, and L1 cache into one process (plan §6).
package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/kchat/drive/internal/blobio"
	"github.com/kchat/drive/internal/config"
	"github.com/kchat/drive/internal/metadata"
	"github.com/kchat/drive/pkg/blobstore"
	"github.com/kchat/drive/pkg/blobstore/local_fs_dev"
	"github.com/kchat/drive/pkg/blobstore/wasabi"
	"github.com/kchat/drive/pkg/hotcache"
)

// Gateway holds the wired dependencies for the drive-gateway process.
type Gateway struct {
	cfg      *config.GatewayConfig
	logger   *slog.Logger
	store    blobstore.BlobStore
	cache    hotcache.Cache
	pipeline *blobio.Pipeline
	metaDB   *metadata.Store
	db       *sql.DB // underlying Postgres pool; nil in dev mode
}

// NewGateway builds the gateway from config. In production it opens
// a Postgres connection pool, builds the Wasabi adapter, the L1
// cache, and the blobio pipeline. In dev mode it uses local_fs_dev
// and an in-memory cache with no Postgres.
func NewGateway(cfg *config.GatewayConfig, logger *slog.Logger) (*Gateway, error) {
	if cfg == nil {
		return nil, errors.New("server: config is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	store, err := buildStore(cfg)
	if err != nil {
		return nil, err
	}
	cache, err := buildCache(cfg)
	if err != nil {
		return nil, err
	}
	g := &Gateway{
		cfg:    cfg,
		logger: logger,
		store:  store,
		cache:  cache,
	}
	// In production, open Postgres and wire the pipeline with a
	// durable status store. In dev, use an in-memory status store.
	if cfg.PostgresDSN != "" {
		db, err := metadata.Open(cfg.PostgresDSN)
		if err != nil {
			return nil, fmt.Errorf("server: open postgres: %w", err)
		}
		g.db = db
		g.metaDB = metadata.New(db)
		// Auto-migrate on startup so the schema is applied without
		// a manual psql step. Migrations are idempotent (CREATE TABLE
		// IF NOT EXISTS, ON CONFLICT DO NOTHING).
		migrateCtx, migrateCancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := g.metaDB.AutoMigrate(migrateCtx); err != nil {
			migrateCancel()
			return nil, fmt.Errorf("server: auto-migrate: %w", err)
		}
		migrateCancel()
		statusStore := metadata.NewStatusStore(db)
		g.pipeline = blobio.NewWithStatusStore(cache, store, statusStore, logger)
	} else {
		g.pipeline = blobio.New(cache, store, logger)
	}
	return g, nil
}

func buildStore(cfg *config.GatewayConfig) (blobstore.BlobStore, error) {
	if cfg.Env != "production" || cfg.Wasabi.Endpoint == "" {
		root := "/tmp/kchat-drive-dev"
		return local_fs_dev.New(root)
	}
	if cfg.WasabiCircuitBreakerEnabled {
		threshold := cfg.WasabiCircuitBreakerThreshold
		if threshold <= 0 {
			threshold = 10
		}
		return wasabi.NewWithCircuitBreaker(wasabi.Config{
			Endpoint:     cfg.Wasabi.Endpoint,
			Region:       cfg.Wasabi.Region,
			Bucket:       cfg.Wasabi.Bucket,
			AccessKey:    cfg.Wasabi.AccessKey,
			SecretKey:    cfg.Wasabi.SecretKey,
			UsePathStyle: cfg.Wasabi.UsePathStyle,
		}, threshold, 30*time.Second)
	}
	return wasabi.New(wasabi.Config{
		Endpoint:     cfg.Wasabi.Endpoint,
		Region:       cfg.Wasabi.Region,
		Bucket:       cfg.Wasabi.Bucket,
		AccessKey:    cfg.Wasabi.AccessKey,
		SecretKey:    cfg.Wasabi.SecretKey,
		UsePathStyle: cfg.Wasabi.UsePathStyle,
	})
}

func buildCache(cfg *config.GatewayConfig) (hotcache.Cache, error) {
	if cfg.Cache.Type == "disk" && cfg.Cache.DiskRootPath != "" {
		return hotcache.NewDiskCache(hotcache.DiskCacheConfig{
			RootPath: cfg.Cache.DiskRootPath,
			Policy:   hotcache.DefaultEvictionPolicy(cfg.Cache.MaxBytes),
		})
	}
	return hotcache.NewMemoryCache(hotcache.EvictionPolicy{
		Kind:     hotcache.EvictionLRU,
		MaxBytes: cfg.Cache.MaxBytes,
	})
}

// Handler returns the HTTP handler for the gateway.
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", g.handleHealthz)
	mux.HandleFunc("/readyz", g.handleReadyz)
	mux.HandleFunc("/metrics", g.handleMetrics)

	// Register Drive REST API routes when Postgres is available.
	if g.metaDB != nil {
		api := newDriveAPI(g)
		api.registerDriveRoutes(mux)
	}

	// Wrap with CORS/COOP/COEP headers for the web sample.
	return withWebHeaders(mux)
}

// withWebHeaders adds CORS, COOP, and COEP headers required for the
// React web sample (cross-origin isolation for threaded WASM).
func withWebHeaders(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Cross-Origin Opener Policy + Embedder Policy for SharedWorker
		// + threaded WASM (plan §D.1).
		w.Header().Set("Cross-Origin-Opener-Policy", "same-origin")
		w.Header().Set("Cross-Origin-Embedder-Policy", "require-corp")
		h.ServeHTTP(w, r)
	})
}

func (g *Gateway) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (g *Gateway) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Check Postgres if configured.
	if g.metaDB != nil {
		if err := g.metaDB.Ping(ctx); err != nil {
			http.Error(w, "postgres: not ready", http.StatusServiceUnavailable)
			return
		}
	}

	// Check Wasabi connectivity with a lightweight HeadBucket.
	// This is a no-op in dev mode (local_fs_dev doesn't have a
	// HeadBucket, but it's always "ready" since it's local disk).
	if g.store != nil {
		if !g.checkStoreReady(ctx) {
			http.Error(w, "store: not ready", http.StatusServiceUnavailable)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

// checkStoreReady does a lightweight readiness check against the
// durable store. For Wasabi this would be HeadBucket; for local_fs_dev
// it's always ready.
func (g *Gateway) checkStoreReady(ctx context.Context) bool {
	// We use a Head on a known non-existent key with a short timeout.
	// If the store returns ErrNotFound, it's reachable and ready.
	// If it returns a network/timeout error, it's not ready.
	type result struct {
		err error
	}
	ch := make(chan result, 1)
	go func() {
		_, err := g.store.Head(ctx, blobstore.ObjectRef{Key: "__readyz_probe__"})
		ch <- result{err: err}
	}()
	select {
	case res := <-ch:
		// nil error (key exists) or ErrNotFound (key doesn't exist)
		// both mean the store is reachable and responding.
		// Any other error (network, auth, timeout) means not ready.
		return res.err == nil || errors.Is(res.err, blobstore.ErrNotFound)
	case <-ctx.Done():
		return false
	}
}

// Store returns the configured BlobStore (for tests and wiring).
func (g *Gateway) Store() blobstore.BlobStore { return g.store }

// Cache returns the configured L1 cache (for tests and wiring).
func (g *Gateway) Cache() hotcache.Cache { return g.cache }

// Pipeline returns the blobio pipeline (for tests and wiring).
func (g *Gateway) Pipeline() *blobio.Pipeline { return g.pipeline }

// Close releases resources held by the gateway, including the
// pipeline (background goroutines), the L1 cache, and the Postgres
// connection pool. The gateway main calls this during graceful
// shutdown after the HTTP server has stopped.
func (g *Gateway) Close() error {
	var firstErr error
	if g.pipeline != nil {
		if err := g.pipeline.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if g.cache != nil {
		if err := g.cache.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if g.db != nil {
		if err := g.db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
