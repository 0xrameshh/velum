package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/0xrameshh/velum/internal/config"
	"github.com/0xrameshh/velum/internal/worker"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg, err := config.LoadWorker()
	if err != nil {
		slog.Error("config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dialCtx, dialCancel := context.WithTimeout(ctx, 30*time.Second)
	conn, err := worker.Dial(dialCtx, cfg.GRPCAddr)
	dialCancel()
	if err != nil {
		slog.Error("grpc dial", "addr", cfg.GRPCAddr, "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	runner := worker.NewGRPCRunner(conn, cfg.WorkerID, cfg.TaskQueue, cfg.TaskLease, cfg.WorkerPollEvery)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	select {
	case <-sigCh:
		slog.Info("shutdown signal received")
		cancel()
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			slog.Error("worker stopped", "error", err)
			os.Exit(1)
		}
	}
}
