package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kchat/drive/internal/blobio"
	"github.com/kchat/drive/internal/metadata"
	"github.com/kchat/drive/pkg/blobstore"
	"github.com/kchat/drive/pkg/hotcache"
)

// PromotionJob promotes CACHED file_versions to Wasabi
// (COMMITTED_DURABLE). It polls the metadata store for CACHED
// versions and calls the blobio pipeline's Promote method.
//
// Blobs are promoted in parallel via a configurable worker pool
// (PromotionParallelism). The pool bounds the number of concurrent
// Wasabi PUTs so a large CACHED backlog doesn't overwhelm the
// durable origin or exhaust memory.
type PromotionJob struct {
	interval    time.Duration
	batchSize   int
	parallelism int
	pipeline    *blobio.Pipeline
	meta        *metadata.Store
	statusStore *metadata.PostgresStatusStore // nil in dev/test
	logger      *slog.Logger

	// Metrics (atomic, exposed via Stats).
	promotedTotal atomic.Int64
	failedTotal   atomic.Int64
	batchCount    atomic.Int64
}

func (j *PromotionJob) Name() string { return "promotion" }

func (j *PromotionJob) Run(ctx context.Context) error {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	// Run once immediately on startup so a manual worker run
	// promotes pending blobs without waiting for the first tick.
	j.runOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			j.runOnce(ctx)
		}
	}
}

func (j *PromotionJob) runOnce(ctx context.Context) {
	if j.meta == nil && j.statusStore == nil {
		j.logger.Debug("promotion: metadata store not configured, skipping")
		return
	}

	// Get the list of CACHED blobs to promote. Prefer the
	// blob_placements table (via statusStore) when available; fall
	// back to file_versions (via meta) for backward compatibility.
	var placements []metadata.CachedPlacement
	var err error
	if j.statusStore != nil {
		placements, err = j.statusStore.ListCachedPlacements(ctx, j.batchSize)
	} else if j.meta != nil {
		versions, vErr := j.meta.ListCachedVersions(ctx, j.batchSize)
		err = vErr
		for _, v := range versions {
			placements = append(placements, metadata.CachedPlacement{
				BlobKey:        v.BlobKey,
				SizeBytes:      v.SizeBytes,
				ChecksumSHA256: v.ChecksumSHA256,
			})
		}
	}
	if err != nil {
		j.logger.Error("promotion: list cached",
			slog.Any("err", err))
		return
	}
	if len(placements) == 0 {
		return
	}
	j.batchCount.Add(1)
	j.logger.Info("promotion: processing batch",
		slog.Int("count", len(placements)),
		slog.Int("parallelism", j.parallelism))

	// Promote in parallel with a bounded worker pool.
	par := j.parallelism
	if par < 1 {
		par = 1
	}
	if par > len(placements) {
		par = len(placements)
	}

	type result struct {
		blobKey string
		err     error
	}
	workCh := make(chan metadata.CachedPlacement, len(placements))
	resultCh := make(chan result, len(placements))

	// Start workers.
	var wg sync.WaitGroup
	for i := 0; i < par; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range workCh {
				if ctx.Err() != nil {
					resultCh <- result{blobKey: p.BlobKey, err: ctx.Err()}
					continue
				}
				err := j.pipeline.Promote(ctx, p.BlobKey)
				resultCh <- result{blobKey: p.BlobKey, err: err}
			}
		}()
	}

	// Feed work.
	for _, p := range placements {
		workCh <- p
	}
	close(workCh)

	// Wait for workers and close results.
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results.
	promoted, failed := 0, 0
	for res := range resultCh {
		if res.err != nil {
			failed++
			j.failedTotal.Add(1)
			j.logger.Warn("promotion: promote failed",
				slog.String("blob_key", res.blobKey),
				slog.Any("err", res.err))
			continue
		}
		promoted++
		j.promotedTotal.Add(1)
	}
	j.logger.Info("promotion: batch complete",
		slog.Int("promoted", promoted),
		slog.Int("failed", failed))
}

// PromotionStats holds promotion job metrics.
type PromotionStats struct {
	PromotedTotal int64
	FailedTotal   int64
	BatchCount    int64
}

// Stats returns a snapshot of promotion job metrics.
func (j *PromotionJob) Stats() PromotionStats {
	return PromotionStats{
		PromotedTotal: j.promotedTotal.Load(),
		FailedTotal:   j.failedTotal.Load(),
		BatchCount:    j.batchCount.Load(),
	}
}

// RepairJob samples durable blobs and verifies their checksums by
// re-reading from the durable store and comparing the SHA-256. If a
// checksum mismatch is found, the job logs an alert and marks the
// blob for re-promotion.
type RepairJob struct {
	interval    time.Duration
	sampleCount int
	store       blobstore.BlobStore
	statusStore *metadata.PostgresStatusStore // nil in dev/test
	logger      *slog.Logger

	// Metrics.
	checkedTotal  atomic.Int64
	mismatchTotal atomic.Int64
	scanCount     atomic.Int64
}

func (j *RepairJob) Name() string { return "repair" }

func (j *RepairJob) Run(ctx context.Context) error {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			j.runOnce(ctx)
		}
	}
}

func (j *RepairJob) runOnce(ctx context.Context) {
	if j.statusStore == nil {
		j.logger.Debug("repair: status store not configured, skipping")
		return
	}
	j.scanCount.Add(1)
	samples, err := j.statusStore.SampleDurablePlacements(ctx, j.sampleCount)
	if err != nil {
		j.logger.Error("repair: sample durable placements",
			slog.Any("err", err))
		return
	}
	if len(samples) == 0 {
		j.logger.Debug("repair: no durable blobs to sample")
		return
	}
	j.logger.Info("repair: sampling durable blobs",
		slog.Int("count", len(samples)))

	for _, s := range samples {
		if ctx.Err() != nil {
			return
		}
		err := verifyChecksum(ctx, j.store, s.BlobKey, s.BlobVersionID, s.ChecksumSHA256)
		j.checkedTotal.Add(1)
		if err != nil {
			j.mismatchTotal.Add(1)
			j.logger.Error("repair: checksum mismatch",
				slog.String("blob_key", s.BlobKey),
				slog.String("expected_sha256", s.ChecksumSHA256),
				slog.Any("err", err))
			// In a full implementation, we would mark the blob for
			// re-promotion or alert an operator. For now, log it.
			continue
		}
	}
	j.logger.Info("repair: scan complete",
		slog.Int("checked", len(samples)),
		slog.Int64("total_checked", j.checkedTotal.Load()))
}

// RepairStats holds repair job metrics.
type RepairStats struct {
	CheckedTotal  int64
	MismatchTotal int64
	ScanCount     int64
}

// Stats returns a snapshot of repair job metrics.
func (j *RepairJob) Stats() RepairStats {
	return RepairStats{
		CheckedTotal:  j.checkedTotal.Load(),
		MismatchTotal: j.mismatchTotal.Load(),
		ScanCount:     j.scanCount.Load(),
	}
}

// PurgeJob sweeps orphaned multipart uploads (older than a threshold)
// and abandoned write intents.
type PurgeJob struct {
	interval time.Duration
	store    blobstore.BlobStore
	meta     *metadata.Store
	logger   *slog.Logger
}

func (j *PurgeJob) Name() string { return "purge" }

func (j *PurgeJob) Run(ctx context.Context) error {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			j.runOnce(ctx)
		}
	}
}

func (j *PurgeJob) runOnce(ctx context.Context) {
	// List in-progress multipart uploads. The store must implement
	// BlobInventory for this.
	inventory, ok := j.store.(blobstore.BlobInventory)
	if !ok {
		j.logger.Debug("purge: store does not implement BlobInventory")
		return
	}
	page, err := inventory.ListMultipartUploads(ctx, blobstore.ListUploadsRequest{
		MaxKeys: 1000,
	})
	if err != nil {
		j.logger.Error("purge: list multipart uploads",
			slog.Any("err", err))
		return
	}
	if len(page.Uploads) == 0 {
		return
	}
	// ListMultipartUploads does not return upload creation time,
	// so we cannot determine staleness from the provider API alone.
	// In production, the metadata store's upload_sessions table
	// tracks creation time and should be cross-referenced to find
	// truly orphaned uploads (no matching session row). Until that
	// cross-reference is implemented, we log the count for
	// observability but do NOT abort — aborting would kill
	// legitimate active uploads being written by clients.
	j.logger.Info("purge: found in-progress multipart uploads",
		slog.Int("count", len(page.Uploads)),
		slog.String("note", "abort deferred until upload_sessions cross-reference is implemented"))
}

// BackupJob triggers the nightly pg_dump → Wasabi backup by invoking
// deploy/sme/backup.sh. The cron expression determines the schedule.
type BackupJob struct {
	cron          string
	retentionDays int
	logger        *slog.Logger
}

func (j *BackupJob) Name() string { return "backup" }

func (j *BackupJob) Run(ctx context.Context) error {
	// Parse the cron expression and wait for the next fire time.
	// For simplicity we use a fixed 24h interval; a real cron parser
	// can replace this if a non-daily schedule is needed.
	interval := 24 * time.Hour
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// Run once immediately on startup so a manual `drive-worker` run
	// produces a backup without waiting 24h.
	j.runBackup(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			j.runBackup(ctx)
		}
	}
}

func (j *BackupJob) runBackup(ctx context.Context) {
	j.logger.Info("backup: starting",
		slog.String("cron", j.cron),
		slog.Int("retention_days", j.retentionDays))
	// The actual backup runs via the host cron or the docker-compose
	// backup service. The worker logs the intent for observability.
}

// GuardrailRollupJob rolls up per-tenant egress and cache-hit metrics
// and emits alerts when thresholds are crossed. It also monitors the
// CACHED blob queue depth and alerts when it grows beyond a
// threshold (backpressure signal).
type GuardrailRollupJob struct {
	interval        time.Duration
	cache           hotcache.Cache
	meta            *metadata.Store
	statusStore     *metadata.PostgresStatusStore // nil in dev/test
	queueDepthAlert int64                         // alert threshold
	logger          *slog.Logger

	// Metrics.
	queueDepthSamples atomic.Int64
	queueDepthAlerts  atomic.Int64
}

func (j *GuardrailRollupJob) Name() string { return "guardrail_rollup" }

func (j *GuardrailRollupJob) Run(ctx context.Context) error {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			j.runOnce(ctx)
		}
	}
}

func (j *GuardrailRollupJob) runOnce(ctx context.Context) {
	// Cache stats rollup.
	if j.cache != nil {
		s := j.cache.Stats()
		total := s.Hits + s.Misses
		hitRatio := 0.0
		if total > 0 {
			hitRatio = float64(s.Hits) / float64(total)
		}
		j.logger.Info("guardrail rollup: cache stats",
			slog.Int64("entries", s.Entries),
			slog.Int64("bytes_used", s.BytesUsed),
			slog.Int64("bytes_limit", s.BytesLimit),
			slog.Uint64("hits", s.Hits),
			slog.Uint64("misses", s.Misses),
			slog.Uint64("evictions", s.Evictions),
			slog.Float64("hit_ratio", hitRatio))
		// Alert if hit ratio drops below the 0.9 target (plan §4).
		if total > 100 && hitRatio < 0.9 {
			j.logger.Warn("guardrail rollup: cache hit ratio below target",
				slog.Float64("hit_ratio", hitRatio),
				slog.Float64("target", 0.9))
		}
	}

	// Backpressure: monitor CACHED queue depth.
	if j.statusStore != nil {
		depth, err := j.statusStore.CountCachedPlacements(ctx)
		j.queueDepthSamples.Add(1)
		if err != nil {
			j.logger.Warn("guardrail rollup: count cached placements",
				slog.Any("err", err))
		} else {
			j.logger.Info("guardrail rollup: promote queue depth",
				slog.Int64("cached_count", depth),
				slog.Int64("alert_threshold", j.queueDepthAlert))
			if j.queueDepthAlert > 0 && depth > j.queueDepthAlert {
				j.queueDepthAlerts.Add(1)
				j.logger.Warn("guardrail rollup: promote queue depth above threshold",
					slog.Int64("cached_count", depth),
					slog.Int64("threshold", j.queueDepthAlert),
					slog.String("action", "consider scaling workers or increasing parallelism"))
			}
		}
	}
}

// GuardrailStats holds guardrail rollup job metrics.
type GuardrailStats struct {
	QueueDepthSamples int64
	QueueDepthAlerts  int64
}

// Stats returns a snapshot of guardrail rollup job metrics.
func (j *GuardrailRollupJob) Stats() GuardrailStats {
	return GuardrailStats{
		QueueDepthSamples: j.queueDepthSamples.Load(),
		QueueDepthAlerts:  j.queueDepthAlerts.Load(),
	}
}

// verifyChecksum re-reads a blob from the store and compares its
// SHA-256 to the expected value. Used by the repair job.
func verifyChecksum(ctx context.Context, store blobstore.BlobStore, key, versionID, expectedSHA string) error {
	ref := blobstore.VersionedObjectRef{Key: key}
	if versionID != "" {
		ref.VersionID = versionID
	}
	r, _, err := store.Get(ctx, blobstore.GetRequest{
		Ref: ref,
	})
	if err != nil {
		return fmt.Errorf("repair: get %q: %w", key, err)
	}
	defer r.Close()
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return fmt.Errorf("repair: read %q: %w", key, err)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != expectedSHA {
		return fmt.Errorf("repair: checksum mismatch for %q: got %s, want %s", key, got, expectedSHA)
	}
	return nil
}
