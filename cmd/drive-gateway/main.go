// Package main is the drive-gateway binary. It merges the API, edge,
// and L1 cache into one process per the simplified architecture plan
// §6. Run N replicas behind Traefik for zero-downtime deploys.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kchat/drive/internal/config"
	"github.com/kchat/drive/internal/server"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "drive-gateway: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("drive-gateway", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to gateway config JSON")
	addr := fs.String("addr", ":8080", "listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *configPath == "" {
		*configPath = envOr("DRIVE_GATEWAY_CONFIG", "")
	}
	cfg, err := config.LoadGateway(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	srv, err := server.NewGateway(cfg, logger)
	if err != nil {
		return fmt.Errorf("build server: %w", err)
	}

	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           srv.Handler(),
		MaxHeaderBytes:    1 << 20, // 1 MB
		ReadTimeout:       30 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("drive-gateway listening", slog.String("addr", *addr))
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("listen error", slog.Any("err", err))
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("drive-gateway shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	// Close the cache (flush in-flight writes) and the Postgres pool.
	if err := srv.Close(); err != nil {
		logger.Warn("close error", slog.Any("err", err))
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
