// Package main is the drive-worker binary. It runs background jobs
// only: async Wasabi promotion, repair/checksum sampling, orphan/
// multipart purge, nightly backup trigger, and guardrail/billing
// rollups (plan §6).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kchat/drive/internal/config"
	"github.com/kchat/drive/internal/worker"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "drive-worker: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("drive-worker", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to worker config JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *configPath == "" {
		*configPath = envOr("DRIVE_WORKER_CONFIG", "")
	}
	cfg, err := config.LoadWorker(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	w, err := worker.New(cfg, logger)
	if err != nil {
		return fmt.Errorf("build worker: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := w.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("worker: %w", err)
	}
	// Allow in-flight jobs a brief grace period on shutdown.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := w.Stop(shutdownCtx); err != nil {
		// Still try to close the DB pool even if Stop timed out.
		_ = w.Close()
		return err
	}
	return w.Close()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
