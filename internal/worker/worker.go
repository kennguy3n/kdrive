// Package worker runs the drive-worker background jobs.
//
// Jobs (plan §6):
//   - PromotionJob: promotes CACHED file_versions to Wasabi (COMMITTED_DURABLE).
//   - RepairJob: samples durable blobs and verifies their checksums.
//   - PurgeJob: sweeps orphaned multipart uploads and abandoned write intents.
//   - BackupJob: triggers the nightly pg_dump → Wasabi backup.
//   - GuardrailRollupJob: rolls up per-tenant egress/cache-hit metrics.
package worker

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/kchat/drive/internal/blobio"
	"github.com/kchat/drive/internal/config"
	"github.com/kchat/drive/internal/metadata"
	"github.com/kchat/drive/pkg/blobstore"
	"github.com/kchat/drive/pkg/blobstore/local_fs_dev"
	"github.com/kchat/drive/pkg/blobstore/wasabi"
	"github.com/kchat/drive/pkg/hotcache"
)

// Worker holds the wired dependencies for the drive-worker process.
type Worker struct {
	cfg      *config.WorkerConfig
	logger   *slog.Logger
	jobs     []Job
	db       *sql.DB          // Postgres pool; nil in dev mode
	cache    hotcache.Cache   // L1 cache; nil in dev mode
	pipeline *blobio.Pipeline // blobio pipeline; nil in dev mode
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// Job is a background loop the worker runs.
type Job interface {
	Name() string
	Run(ctx context.Context) error
}

// Deps carries the external dependencies the worker jobs need. In
// production these are built from config; in tests they are injected
// directly.
type Deps struct {
	Store    blobstore.BlobStore // durable origin (Wasabi or local_fs_dev)
	Pipeline *blobio.Pipeline    // L1+L2 pipeline for promote/repair
	MetaDB   *sql.DB             // Postgres for metadata queries
	Cache    hotcache.Cache      // L1 cache (for repair/restore)
}

// New builds the worker from config. In dev mode (no Postgres DSN)
// the worker runs no jobs. In production it builds the store, cache,
// pipeline, and metadata connection from config.
func New(cfg *config.WorkerConfig, logger *slog.Logger) (*Worker, error) {
	if cfg == nil {
		return nil, errors.New("worker: config is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	w := &Worker{cfg: cfg, logger: logger}
	if cfg.Env != "production" && cfg.PostgresDSN == "" {
		w.jobs = nil
		return w, nil
	}
	deps, err := buildDeps(cfg, logger)
	if err != nil {
		return nil, err
	}
	w.db = deps.MetaDB
	w.cache = deps.Cache
	w.pipeline = deps.Pipeline
	w.jobs = w.buildJobs(*deps)
	return w, nil
}

// NewWithDeps builds a worker with explicitly injected dependencies.
// Tests use this to wire fakes without a real Postgres or Wasabi.
func NewWithDeps(cfg *config.WorkerConfig, deps Deps, logger *slog.Logger) (*Worker, error) {
	if cfg == nil {
		return nil, errors.New("worker: config is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	w := &Worker{cfg: cfg, logger: logger, db: deps.MetaDB, cache: deps.Cache, pipeline: deps.Pipeline}
	w.jobs = w.buildJobs(deps)
	return w, nil
}

func buildDeps(cfg *config.WorkerConfig, logger *slog.Logger) (*Deps, error) {
	store, err := buildWorkerStore(cfg)
	if err != nil {
		return nil, err
	}
	cache, err := buildWorkerCache(cfg)
	if err != nil {
		return nil, err
	}
	metaDB, err := metadata.Open(cfg.PostgresDSN)
	if err != nil {
		return nil, err
	}
	statusStore := metadata.NewStatusStore(metaDB)
	pipeline := blobio.NewWithStatusStore(cache, store, statusStore, logger)
	return &Deps{
		Store:    store,
		Pipeline: pipeline,
		MetaDB:   metaDB,
		Cache:    cache,
	}, nil
}

func buildWorkerStore(cfg *config.WorkerConfig) (blobstore.BlobStore, error) {
	if cfg.Wasabi.Endpoint == "" {
		return local_fs_dev.New("/tmp/kchat-drive-dev")
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

func buildWorkerCache(cfg *config.WorkerConfig) (hotcache.Cache, error) {
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

func (w *Worker) buildJobs(deps Deps) []Job {
	// meta may be nil when MetaDB is nil (e.g. in tests using
	// NewWithDeps without a real Postgres). Jobs that need meta
	// check for nil in their runOnce and skip with a debug log.
	var meta *metadata.Store
	if deps.MetaDB != nil {
		meta = metadata.New(deps.MetaDB)
	}
	// statusStore is the Postgres-backed blob placement store. It's
	// nil in dev/test mode; jobs that need it skip gracefully.
	var statusStore *metadata.PostgresStatusStore
	if deps.MetaDB != nil {
		statusStore = metadata.NewStatusStore(deps.MetaDB)
	}

	// Configurable tuning knobs (P2-10).
	promoteInterval := 30 * time.Second
	if w.cfg.PromoteIntervalMs > 0 {
		promoteInterval = time.Duration(w.cfg.PromoteIntervalMs) * time.Millisecond
	}
	promoteBatch := 100
	if w.cfg.PromoteBatchSize > 0 {
		promoteBatch = w.cfg.PromoteBatchSize
	}
	promoteParallelism := 4
	if w.cfg.PromoteParallelism > 0 {
		promoteParallelism = w.cfg.PromoteParallelism
	}
	repairInterval := 5 * time.Minute
	if w.cfg.RepairIntervalMs > 0 {
		repairInterval = time.Duration(w.cfg.RepairIntervalMs) * time.Millisecond
	}
	repairSample := 10
	if w.cfg.RepairSampleCount > 0 {
		repairSample = w.cfg.RepairSampleCount
	}
	queueDepthAlert := int64(1000)
	if w.cfg.QueueDepthAlert > 0 {
		queueDepthAlert = w.cfg.QueueDepthAlert
	}

	return []Job{
		&PromotionJob{
			interval:    promoteInterval,
			batchSize:   promoteBatch,
			parallelism: promoteParallelism,
			pipeline:    deps.Pipeline,
			meta:        meta,
			statusStore: statusStore,
			logger:      w.logger,
		},
		&RepairJob{
			interval:    repairInterval,
			sampleCount: repairSample,
			store:       deps.Store,
			statusStore: statusStore,
			logger:      w.logger,
		},
		&PurgeJob{
			interval: 1 * time.Hour,
			store:    deps.Store,
			meta:     meta,
			logger:   w.logger,
		},
		&BackupJob{
			cron:          w.cfg.BackupCron,
			retentionDays: w.cfg.BackupRetentionDays,
			logger:        w.logger,
		},
		&GuardrailRollupJob{
			interval:        5 * time.Minute,
			cache:           deps.Cache,
			meta:            meta,
			statusStore:     statusStore,
			queueDepthAlert: queueDepthAlert,
			logger:          w.logger,
		},
	}
}

// Start launches every job in its own goroutine.
func (w *Worker) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	for _, job := range w.jobs {
		w.wg.Add(1)
		go func(j Job) {
			defer w.wg.Done()
			w.logger.Info("worker job started", slog.String("job", j.Name()))
			if err := j.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				w.logger.Error("worker job exited with error", slog.String("job", j.Name()), slog.Any("err", err))
			}
		}(job)
	}
	if len(w.jobs) == 0 {
		w.logger.Info("worker: no jobs configured (dev mode); idling")
	}
	<-ctx.Done()
	return ctx.Err()
}

// Stop signals every job to stop and waits for them to finish.
func (w *Worker) Stop(ctx context.Context) error {
	if w.cancel != nil {
		w.cancel()
	}
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Jobs returns the configured jobs (for inspection/testing).
func (w *Worker) Jobs() []Job { return w.jobs }

// Close releases resources held by the worker, including the
// pipeline (background goroutines), the L1 cache, and the Postgres
// connection pool. The worker main calls this after Stop.
func (w *Worker) Close() error {
	var firstErr error
	// Find the pipeline from deps (stored in jobs) and close it.
	// The pipeline is shared across jobs; closing it once suffices.
	if w.pipeline != nil {
		if err := w.pipeline.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if w.cache != nil {
		if err := w.cache.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if w.db != nil {
		if err := w.db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
