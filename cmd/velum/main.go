package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	velumv1 "github.com/0xrameshh/velum/gen/velum/v1"
	"github.com/0xrameshh/velum/internal/api"
	"github.com/0xrameshh/velum/internal/config"
	"github.com/0xrameshh/velum/internal/grpcserver"
	"github.com/0xrameshh/velum/internal/history"
	"github.com/0xrameshh/velum/internal/persistence"
	"github.com/0xrameshh/velum/internal/platform"
	"github.com/0xrameshh/velum/internal/scheduler"
	"github.com/0xrameshh/velum/internal/worker"
)

// velum is the all-in-one control plane for local development (API + matcher + scheduler).
func main() {
	platform.SetupLogger()

	cfg, err := config.Load()
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

	if err := platform.MaybeMigrate(ctx, pool, cfg.MigrateOnStartup); err != nil {
		slog.Error("migrate", "error", err)
		os.Exit(1)
	}

	store := persistence.NewStore(pool)
	hist := history.NewService(store)
	httpSrv := api.NewServer(hist)

	httpServer := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      httpSrv.Router(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	errCh := make(chan error, 4)

	if cfg.EnableGRPC {
		grpcSrv := grpc.NewServer()
		ws := grpcserver.New(store, hist, cfg.TaskLease)
		velumv1.RegisterWorkerServiceServer(grpcSrv, ws)
		reflection.Register(grpcSrv)

		lis, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			slog.Error("grpc listen", "error", err)
			os.Exit(1)
		}
		go ws.RunLeaseReclaimer(ctx, cfg.LeaseReclaimEvery)
		go func() {
			slog.Info("grpc listening", "addr", cfg.GRPCAddr)
			errCh <- grpcSrv.Serve(lis)
		}()
	}

	if cfg.EnableScheduler {
		sched := scheduler.New(store, hist, cfg.SchedulerPollEvery, cfg.SchedulerBatchSize)
		go func() {
			errCh <- sched.Run(ctx)
		}()
	}

	if cfg.EnableEmbeddedWorker {
		w := worker.NewEmbedded(store, hist, cfg.WorkerID, cfg.EmbeddedWorkerQueue, cfg.TaskLease, cfg.WorkerPollEvery)
		go func() {
			errCh <- w.Run(ctx)
		}()
	}

	platform.ListenHTTP(httpServer, errCh)

	select {
	case <-signalDone():
		slog.Info("shutdown")
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("runtime error", "error", err)
		}
	}

	cancel()
	platform.ShutdownHTTP(httpServer, 15*time.Second)
}

func signalDone() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		platform.WaitForSignal()
		close(ch)
	}()
	return ch
}
