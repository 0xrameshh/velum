package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr              string
	GRPCAddr              string
	DatabaseURL           string
	WorkerID              string
	WorkerPollEvery       time.Duration
	TaskLease             time.Duration
	LeaseReclaimEvery     time.Duration
	MigrateOnStartup      bool
	EnableGRPC            bool
	EnableEmbeddedWorker  bool
	EmbeddedWorkerQueue   string
	EnableScheduler       bool
	SchedulerPollEvery    time.Duration
	SchedulerBatchSize    int
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:             envOr("VELUM_HTTP_ADDR", ":8080"),
		GRPCAddr:             envOr("VELUM_GRPC_ADDR", ":9090"),
		DatabaseURL:          envOr("VELUM_DATABASE_URL", "postgres://velum:velum@localhost:5432/velum?sslmode=disable"),
		WorkerID:             envOr("VELUM_WORKER_ID", "worker-1"),
		WorkerPollEvery:      envDurationOr("VELUM_WORKER_POLL_EVERY", 500*time.Millisecond),
		TaskLease:            envDurationOr("VELUM_TASK_LEASE", 30*time.Second),
		LeaseReclaimEvery:    envDurationOr("VELUM_LEASE_RECLAIM_EVERY", 5*time.Second),
		MigrateOnStartup:     envBoolOr("VELUM_MIGRATE_ON_STARTUP", true),
		EnableGRPC:           envBoolOr("VELUM_ENABLE_GRPC", true),
		EnableEmbeddedWorker: envBoolOr("VELUM_ENABLE_EMBEDDED_WORKER", false),
		EmbeddedWorkerQueue:  envOr("VELUM_EMBEDDED_WORKER_QUEUE", "default"),
		EnableScheduler:      envBoolOr("VELUM_ENABLE_SCHEDULER", true),
		SchedulerPollEvery:   envDurationOr("VELUM_SCHEDULER_POLL_EVERY", time.Second),
		SchedulerBatchSize:   envIntOr("VELUM_SCHEDULER_BATCH_SIZE", 50),
	}
	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("VELUM_DATABASE_URL is required")
	}
	return cfg, nil
}

type WorkerConfig struct {
	GRPCAddr      string
	DatabaseURL   string
	WorkerID      string
	TaskQueue     string
	WorkerPollEvery time.Duration
	TaskLease     time.Duration
}

type APIConfig struct {
	HTTPAddr        string
	HistoryGRPCAddr string
}

func LoadAPI() (APIConfig, error) {
	cfg := APIConfig{
		HTTPAddr:        envOr("VELUM_HTTP_ADDR", ":8080"),
		HistoryGRPCAddr: envOr("VELUM_HISTORY_GRPC_ADDR", "localhost:9091"),
	}
	if cfg.HistoryGRPCAddr == "" {
		return cfg, fmt.Errorf("VELUM_HISTORY_GRPC_ADDR is required")
	}
	return cfg, nil
}

type MatcherConfig struct {
	GRPCAddr          string
	HistoryGRPCAddr   string
	DatabaseURL       string
	TaskLease         time.Duration
	LeaseReclaimEvery time.Duration
}

func LoadMatcher() (MatcherConfig, error) {
	cfg := MatcherConfig{
		GRPCAddr:          envOr("VELUM_GRPC_ADDR", ":9090"),
		HistoryGRPCAddr:   envOr("VELUM_HISTORY_GRPC_ADDR", "localhost:9091"),
		DatabaseURL:       envOr("VELUM_DATABASE_URL", "postgres://velum:velum@localhost:5432/velum?sslmode=disable"),
		TaskLease:         envDurationOr("VELUM_TASK_LEASE", 30*time.Second),
		LeaseReclaimEvery: envDurationOr("VELUM_LEASE_RECLAIM_EVERY", 5*time.Second),
	}
	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("VELUM_DATABASE_URL is required")
	}
	if cfg.GRPCAddr == "" {
		return cfg, fmt.Errorf("VELUM_GRPC_ADDR is required")
	}
	if cfg.HistoryGRPCAddr == "" {
		return cfg, fmt.Errorf("VELUM_HISTORY_GRPC_ADDR is required")
	}
	return cfg, nil
}

type SchedulerConfig struct {
	DatabaseURL        string
	HistoryGRPCAddr    string
	SchedulerPollEvery time.Duration
	SchedulerBatchSize int
}

func LoadScheduler() (SchedulerConfig, error) {
	cfg := SchedulerConfig{
		DatabaseURL:        envOr("VELUM_DATABASE_URL", "postgres://velum:velum@localhost:5432/velum?sslmode=disable"),
		HistoryGRPCAddr:    envOr("VELUM_HISTORY_GRPC_ADDR", "localhost:9091"),
		SchedulerPollEvery: envDurationOr("VELUM_SCHEDULER_POLL_EVERY", time.Second),
		SchedulerBatchSize: envIntOr("VELUM_SCHEDULER_BATCH_SIZE", 50),
	}
	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("VELUM_DATABASE_URL is required")
	}
	if cfg.HistoryGRPCAddr == "" {
		return cfg, fmt.Errorf("VELUM_HISTORY_GRPC_ADDR is required")
	}
	return cfg, nil
}

type HistoryConfig struct {
	GRPCAddr         string
	DatabaseURL      string
	MigrateOnStartup bool
}

func LoadHistory() (HistoryConfig, error) {
	cfg := HistoryConfig{
		GRPCAddr:         envOr("VELUM_HISTORY_GRPC_ADDR", ":9091"),
		DatabaseURL:      envOr("VELUM_DATABASE_URL", "postgres://velum:velum@localhost:5432/velum?sslmode=disable"),
		MigrateOnStartup: envBoolOr("VELUM_MIGRATE_ON_STARTUP", true),
	}
	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("VELUM_DATABASE_URL is required")
	}
	if cfg.GRPCAddr == "" {
		return cfg, fmt.Errorf("VELUM_HISTORY_GRPC_ADDR is required")
	}
	return cfg, nil
}

type MigrateConfig struct {
	DatabaseURL string
}

func LoadMigrate() (MigrateConfig, error) {
	cfg := MigrateConfig{
		DatabaseURL: envOr("VELUM_DATABASE_URL", "postgres://velum:velum@localhost:5432/velum?sslmode=disable"),
	}
	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("VELUM_DATABASE_URL is required")
	}
	return cfg, nil
}

func LoadWorker() (WorkerConfig, error) {
	cfg := WorkerConfig{
		GRPCAddr:        envOr("VELUM_GRPC_ADDR", "localhost:9090"),
		DatabaseURL:     envOr("VELUM_DATABASE_URL", ""),
		WorkerID:        envOr("VELUM_WORKER_ID", "worker-1"),
		TaskQueue:       envOr("VELUM_TASK_QUEUE", "default"),
		WorkerPollEvery: envDurationOr("VELUM_WORKER_POLL_EVERY", 500*time.Millisecond),
		TaskLease:       envDurationOr("VELUM_TASK_LEASE", 30*time.Second),
	}
	if cfg.GRPCAddr == "" {
		return cfg, fmt.Errorf("VELUM_GRPC_ADDR is required")
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envBoolOr(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func envIntOr(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envDurationOr(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
