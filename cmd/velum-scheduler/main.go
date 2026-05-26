package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/0xrameshh/velum/internal/config"
	"github.com/0xrameshh/velum/internal/persistence"
	"github.com/0xrameshh/velum/internal/platform"
	"github.com/0xrameshh/velum/internal/scheduler"
)

func main() {
	platform.SetupLogger()

	cfg, err := config.LoadScheduler()
	if err != nil {
		slog.Error("config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := platform.ConnectDB(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	hist, closeHist, err := platform.ConnectHistory(ctx, cfg.HistoryGRPCAddr)
	if err != nil {
		slog.Error("history client", "error", err)
		os.Exit(1)
	}
	defer closeHist()

	store := persistence.NewStore(pool)
	sched := scheduler.New(store, hist, cfg.SchedulerPollEvery, cfg.SchedulerBatchSize)

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(ctx)
	}()

	select {
	case <-signalDone():
		slog.Info("shutdown")
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			slog.Error("scheduler stopped", "error", err)
		}
	}

	cancel()
}

func signalDone() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		platform.WaitForSignal()
		close(ch)
	}()
	return ch
}
