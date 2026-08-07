package server

import (
	"fmt"
	"net/http"
	"strings"
)

// handleMetrics emits Prometheus-format metrics from the cache and
// pipeline. This is a lightweight text-format exporter — no external
// dependency needed.
//
// Gateway-exposed metrics:
//   - kdrive_cache_*: L1 cache entries, bytes, hits, misses, evictions
//   - kdrive_promote_*: promote attempts, successes, failures, skips
//   - kdrive_promote_duration_avg_ms: average promote duration
//
// Worker-side metrics (repair, queue-depth) are logged by the worker
// and are not exposed via HTTP since the worker does not serve HTTP.
func (g *Gateway) handleMetrics(w http.ResponseWriter, r *http.Request) {
	var sb strings.Builder

	// Cache metrics.
	if g.cache != nil {
		s := g.cache.Stats()
		writeMetric(&sb, "kdrive_cache_entries", s.Entries)
		writeMetric(&sb, "kdrive_cache_bytes_used", s.BytesUsed)
		writeMetric(&sb, "kdrive_cache_bytes_limit", s.BytesLimit)
		writeMetric(&sb, "kdrive_cache_hits_total", int64(s.Hits))
		writeMetric(&sb, "kdrive_cache_misses_total", int64(s.Misses))
		writeMetric(&sb, "kdrive_cache_evictions_total", int64(s.Evictions))

		// Computed hit ratio.
		total := s.Hits + s.Misses
		if total > 0 {
			writeMetricFloat(&sb, "kdrive_cache_hit_ratio", float64(s.Hits)/float64(total))
		}
	}

	// Pipeline metrics.
	if g.pipeline != nil {
		ps := g.pipeline.Stats()
		writeMetric(&sb, "kdrive_promote_attempts_total", ps.PromoteAttempts)
		writeMetric(&sb, "kdrive_promote_successes_total", ps.PromoteSuccesses)
		writeMetric(&sb, "kdrive_promote_failures_total", ps.PromoteFailures)
		writeMetric(&sb, "kdrive_promote_skipped_total", ps.PromoteSkipped)
		writeMetricFloat(&sb, "kdrive_promote_duration_avg_ms", ps.PromoteDurationAvgMs)
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(sb.String()))
}

func writeMetric(sb *strings.Builder, name string, value int64) {
	fmt.Fprintf(sb, "# TYPE %s counter\n%s %d\n", name, name, value)
}

func writeMetricFloat(sb *strings.Builder, name string, value float64) {
	fmt.Fprintf(sb, "# TYPE %s gauge\n%s %g\n", name, name, value)
}
