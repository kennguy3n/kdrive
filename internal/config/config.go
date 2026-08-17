// Package config loads the gateway and worker JSON configurations.
// The gateway reads a JSON config that does not expand environment
// variables; an entrypoint script renders the template with envsubst
// at container start (same pattern as zk-object-fabric deploy/sme).
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
)

// GatewayConfig is the drive-gateway runtime configuration.
type GatewayConfig struct {
	Env         string        `json:"env"`          // "dev" or "production"
	HTTPAddr    string        `json:"http_addr"`    // ":8080"
	PostgresDSN string        `json:"postgres_dsn"` // required in production
	Storage     StorageConfig `json:"storage"`
	Wasabi      WasabiConfig  `json:"wasabi"` // deprecated: use Storage
	Cache       CacheConfig   `json:"cache"`

	// --- Circuit breaker (P2-10) ---

	// StorageCircuitBreakerEnabled turns on the storage backend circuit
	// breaker. When enabled, after consecutive failures the breaker
	// opens and fails-fast requests until a probe succeeds. Default
	// false.
	StorageCircuitBreakerEnabled bool `json:"storage_circuit_breaker_enabled"`
	// StorageCircuitBreakerThreshold is the consecutive failure count
	// that opens the breaker. Default 10.
	StorageCircuitBreakerThreshold int `json:"storage_circuit_breaker_threshold"`

	// --- Deprecated circuit breaker fields (backwards compat) ---

	WasabiCircuitBreakerEnabled   bool `json:"wasabi_circuit_breaker_enabled"`
	WasabiCircuitBreakerThreshold int  `json:"wasabi_circuit_breaker_threshold"`
}

// StorageConfig is the generic S3-compatible storage backend
// configuration. The Provider field selects the provider profile
// ("wasabi", "s3", "b2"). All providers use the same AWS SDK v2 S3
// client; the profile only affects capability reporting, error
// prefixes, and guardrail defaults.
type StorageConfig struct {
	Provider     string `json:"provider"`       // "wasabi", "s3", "b2", "" (defaults to "wasabi" for backwards compat)
	Endpoint     string `json:"endpoint"`       // S3-compatible endpoint URL
	Region       string `json:"region"`         // region label for request signing
	Bucket       string `json:"bucket"`         // bucket name
	AccessKey    string `json:"access_key"`     // service credentials
	SecretKey    string `json:"secret_key"`     // service credentials
	UsePathStyle bool   `json:"use_path_style"` // path-style addressing
}

// WasabiConfig is the deprecated Wasabi-specific adapter
// configuration. New deployments should use the Storage field with
// Provider="wasabi" instead. Kept for backwards compatibility; the
// config loader migrates WasabiConfig to StorageConfig automatically.
type WasabiConfig struct {
	Endpoint     string `json:"endpoint"`
	Region       string `json:"region"`
	Bucket       string `json:"bucket"`
	AccessKey    string `json:"access_key"`
	SecretKey    string `json:"secret_key"`
	UsePathStyle bool   `json:"use_path_style"`
}

// CacheConfig is the L1 hot cache configuration.
type CacheConfig struct {
	// Type is "memory" or "disk". Production uses disk.
	Type string `json:"type"`
	// DiskRootPath is the NVMe/SSD root for the disk cache.
	DiskRootPath string `json:"disk_root_path"`
	// MaxBytes is the cache capacity in bytes.
	MaxBytes int64 `json:"max_bytes"`
}

// WorkerConfig is the drive-worker runtime configuration.
type WorkerConfig struct {
	Env         string        `json:"env"`
	PostgresDSN string        `json:"postgres_dsn"`
	Storage     StorageConfig `json:"storage"`
	Wasabi      WasabiConfig  `json:"wasabi"` // deprecated: use Storage
	Cache       CacheConfig   `json:"cache"`
	// BackupCron is the cron expression for the nightly pg_dump →
	// storage backup. Default "17 3 * * *".
	BackupCron string `json:"backup_cron"`
	// BackupRetentionDays is the backup retention window.
	BackupRetentionDays int `json:"backup_retention_days"`

	// --- Promotion tuning (P2-10) ---

	// PromoteInterval is the time between promotion sweeps. Default 30s.
	PromoteIntervalMs int `json:"promote_interval_ms"`
	// PromoteBatchSize is the max blobs per promotion sweep. Default 100.
	PromoteBatchSize int `json:"promote_batch_size"`
	// PromoteParallelism is the number of concurrent promote workers.
	// Default 4. Set to 1 for sequential promotion.
	PromoteParallelism int `json:"promote_parallelism"`

	// --- Repair tuning (P2-10) ---

	// RepairIntervalMs is the time between repair scans. Default 300000 (5min).
	RepairIntervalMs int `json:"repair_interval_ms"`
	// RepairSampleCount is the number of durable blobs to sample per
	// repair scan. Default 10.
	RepairSampleCount int `json:"repair_sample_count"`

	// --- Backpressure / alerting (P2-10) ---

	// QueueDepthAlert is the CACHED blob count above which the
	// guardrail rollup job emits a warning. Default 1000.
	QueueDepthAlert int64 `json:"queue_depth_alert"`

	// --- Circuit breaker (P2-10) ---

	// StorageCircuitBreakerEnabled turns on the storage backend circuit
	// breaker. Default false.
	StorageCircuitBreakerEnabled bool `json:"storage_circuit_breaker_enabled"`
	// StorageCircuitBreakerThreshold is the consecutive failure count
	// that opens the breaker. Default 10.
	StorageCircuitBreakerThreshold int `json:"storage_circuit_breaker_threshold"`

	// --- Deprecated circuit breaker fields (backwards compat) ---

	WasabiCircuitBreakerEnabled   bool `json:"wasabi_circuit_breaker_enabled"`
	WasabiCircuitBreakerThreshold int  `json:"wasabi_circuit_breaker_threshold"`
}

// LoadGateway loads the gateway config from path. If path is empty,
// returns a dev config backed by the local filesystem adapter.
func LoadGateway(path string) (*GatewayConfig, error) {
	if path == "" {
		return devGatewayConfig(), nil
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg GatewayConfig
	if err := json.Unmarshal(body, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	cfg.migrateDeprecatedFields()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// LoadWorker loads the worker config from path.
func LoadWorker(path string) (*WorkerConfig, error) {
	if path == "" {
		return devWorkerConfig(), nil
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg WorkerConfig
	if err := json.Unmarshal(body, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	cfg.migrateDeprecatedFields()
	if cfg.BackupCron == "" {
		cfg.BackupCron = "17 3 * * *"
	}
	if cfg.BackupRetentionDays == 0 {
		cfg.BackupRetentionDays = 14
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// migrateDeprecatedFields migrates the deprecated WasabiConfig and
// WasabiCircuitBreaker* fields to the new StorageConfig and
// StorageCircuitBreaker* fields, if the new fields are not already
// set. This preserves backwards compatibility with existing config
// files that use the "wasabi" block.
func (c *GatewayConfig) migrateDeprecatedFields() {
	migrateStorageFields(&c.Storage, &c.Wasabi,
		&c.StorageCircuitBreakerEnabled, &c.WasabiCircuitBreakerEnabled,
		&c.StorageCircuitBreakerThreshold, &c.WasabiCircuitBreakerThreshold)
}

// migrateDeprecatedFields is the WorkerConfig equivalent.
func (c *WorkerConfig) migrateDeprecatedFields() {
	migrateStorageFields(&c.Storage, &c.Wasabi,
		&c.StorageCircuitBreakerEnabled, &c.WasabiCircuitBreakerEnabled,
		&c.StorageCircuitBreakerThreshold, &c.WasabiCircuitBreakerThreshold)
}

// migrateStorageFields is the shared migration logic used by both
// GatewayConfig and WorkerConfig.
func migrateStorageFields(
	storage *StorageConfig, wasabi *WasabiConfig,
	storageCBEnabled, wasabiCBEnabled *bool,
	storageCBThreshold, wasabiCBThreshold *int,
) {
	if storage.Provider == "" && storage.Endpoint == "" {
		if wasabi.Endpoint != "" {
			*storage = StorageConfig{
				Provider:     "wasabi",
				Endpoint:     wasabi.Endpoint,
				Region:       wasabi.Region,
				Bucket:       wasabi.Bucket,
				AccessKey:    wasabi.AccessKey,
				SecretKey:    wasabi.SecretKey,
				UsePathStyle: wasabi.UsePathStyle,
			}
		}
	} else if storage.Provider == "" {
		storage.Provider = "wasabi"
	}
	if !*storageCBEnabled && *wasabiCBEnabled {
		*storageCBEnabled = true
	}
	if *storageCBThreshold == 0 && *wasabiCBThreshold > 0 {
		*storageCBThreshold = *wasabiCBThreshold
	}
}

func (c *WorkerConfig) validate() error {
	if err := c.Cache.validate(); err != nil {
		return fmt.Errorf("config: cache: %w", err)
	}
	if c.BackupRetentionDays < 1 || c.BackupRetentionDays > 365 {
		return fmt.Errorf("config: backup_retention_days must be 1-365, got %d", c.BackupRetentionDays)
	}
	// Validate tuning knobs. Zero means "use default" (handled by
	// the worker), but negative values are clearly wrong and should
	// fail-fast at startup rather than silently using defaults.
	if c.PromoteIntervalMs < 0 {
		return fmt.Errorf("config: promote_interval_ms must be >= 0, got %d", c.PromoteIntervalMs)
	}
	if c.PromoteBatchSize < 0 {
		return fmt.Errorf("config: promote_batch_size must be >= 0, got %d", c.PromoteBatchSize)
	}
	if c.PromoteParallelism < 0 {
		return fmt.Errorf("config: promote_parallelism must be >= 0, got %d", c.PromoteParallelism)
	}
	if c.PromoteParallelism > 100 {
		return fmt.Errorf("config: promote_parallelism must be <= 100, got %d", c.PromoteParallelism)
	}
	if c.RepairIntervalMs < 0 {
		return fmt.Errorf("config: repair_interval_ms must be >= 0, got %d", c.RepairIntervalMs)
	}
	if c.RepairSampleCount < 0 {
		return fmt.Errorf("config: repair_sample_count must be >= 0, got %d", c.RepairSampleCount)
	}
	if c.QueueDepthAlert < 0 {
		return fmt.Errorf("config: queue_depth_alert must be >= 0, got %d", c.QueueDepthAlert)
	}
	if c.StorageCircuitBreakerThreshold < 0 {
		return fmt.Errorf("config: storage_circuit_breaker_threshold must be >= 0, got %d", c.StorageCircuitBreakerThreshold)
	}
	if c.Env == "production" {
		if c.PostgresDSN == "" {
			return errors.New("config: postgres_dsn is required in production")
		}
		if err := c.Storage.validate(); err != nil {
			return fmt.Errorf("config: storage: %w", err)
		}
	}
	return nil
}

func (c *GatewayConfig) validate() error {
	// Validate HTTP address.
	if c.HTTPAddr != "" {
		if err := validateAddr(c.HTTPAddr); err != nil {
			return fmt.Errorf("config: http_addr: %w", err)
		}
	}
	// Validate cache config.
	if err := c.Cache.validate(); err != nil {
		return fmt.Errorf("config: cache: %w", err)
	}
	if c.StorageCircuitBreakerThreshold < 0 {
		return fmt.Errorf("config: storage_circuit_breaker_threshold must be >= 0, got %d", c.StorageCircuitBreakerThreshold)
	}
	if c.Env == "production" {
		if c.PostgresDSN == "" {
			return errors.New("config: postgres_dsn is required in production")
		}
		if err := c.Storage.validate(); err != nil {
			return fmt.Errorf("config: storage: %w", err)
		}
	}
	return nil
}

func (c *CacheConfig) validate() error {
	switch c.Type {
	case "", "memory":
		// Memory cache: MaxBytes must be positive.
		if c.MaxBytes <= 0 {
			return errors.New("max_bytes must be positive")
		}
	case "disk":
		if c.DiskRootPath == "" {
			return errors.New("disk_root_path is required when type=disk")
		}
		if c.MaxBytes <= 0 {
			return errors.New("max_bytes must be positive")
		}
	default:
		return fmt.Errorf("unknown cache type %q (want memory or disk)", c.Type)
	}
	return nil
}

func (s *StorageConfig) validate() error {
	if s.Endpoint == "" {
		return errors.New("endpoint is required")
	}
	if s.Region == "" {
		return errors.New("region is required")
	}
	if s.Bucket == "" {
		return errors.New("bucket is required")
	}
	if s.AccessKey == "" {
		return errors.New("access_key is required")
	}
	if s.SecretKey == "" {
		return errors.New("secret_key is required")
	}
	// Validate provider name if set.
	switch s.Provider {
	case "", "wasabi", "s3", "b2":
		// ok
	default:
		return fmt.Errorf("unknown provider %q (want wasabi, s3, or b2)", s.Provider)
	}
	return nil
}

// validateAddr checks that a listen address is well-formed. Accepts
// ":8080", "0.0.0.0:8080", "[::1]:8080", or a unix socket path.
func validateAddr(addr string) error {
	if strings.HasPrefix(addr, "unix:") {
		return nil
	}
	// Try to split host:port. net.SplitHostPort handles [::1]:8080.
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid address %q: %w", addr, err)
	}
	if port == "" {
		return fmt.Errorf("invalid address %q: missing port", addr)
	}
	return nil
}

func devGatewayConfig() *GatewayConfig {
	return &GatewayConfig{
		Env:         "dev",
		HTTPAddr:    ":8080",
		PostgresDSN: "postgres://postgres:postgres@localhost:5432/kdrive?sslmode=disable",
		Cache: CacheConfig{
			Type:     "memory",
			MaxBytes: 256 * 1024 * 1024,
		},
	}
}

func devWorkerConfig() *WorkerConfig {
	return &WorkerConfig{
		Env:                 "dev",
		BackupCron:          "17 3 * * *",
		BackupRetentionDays: 14,
		Cache: CacheConfig{
			Type:     "memory",
			MaxBytes: 256 * 1024 * 1024,
		},
	}
}
