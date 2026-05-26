package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/0xrameshh/velum/internal/config"
	"github.com/0xrameshh/velum/internal/persistence"
	"github.com/0xrameshh/velum/internal/platform"
)

func main() {
	platform.SetupLogger()

	cfg, err := config.LoadMigrate()
	if err != nil {
		slog.Error("config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()
	pool, err := platform.ConnectDB(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := persistence.Migrate(ctx, pool); err != nil {
		slog.Error("migrate", "error", err)
		os.Exit(1)
	}
	slog.Info("migrations applied")
}
