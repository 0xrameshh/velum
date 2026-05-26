package main

import (
	"context"
	"log/slog"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	velumv1 "github.com/0xrameshh/velum/gen/velum/v1"
	"github.com/0xrameshh/velum/internal/config"
	"github.com/0xrameshh/velum/internal/grpcserver"
	"github.com/0xrameshh/velum/internal/persistence"
	"github.com/0xrameshh/velum/internal/platform"
)

func main() {
	platform.SetupLogger()

	cfg, err := config.LoadMatcher()
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

	grpcSrv := grpc.NewServer()
	ws := grpcserver.New(store, hist, cfg.TaskLease)
	velumv1.RegisterWorkerServiceServer(grpcSrv, ws)
	reflection.Register(grpcSrv)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		slog.Error("grpc listen", "error", err)
		os.Exit(1)
	}

	errCh := make(chan error, 1)
	go ws.RunLeaseReclaimer(ctx, cfg.LeaseReclaimEvery)
	go func() {
		slog.Info("grpc listening", "addr", cfg.GRPCAddr)
		errCh <- grpcSrv.Serve(lis)
	}()

	select {
	case <-signalDone():
		slog.Info("shutdown")
	case err := <-errCh:
		slog.Error("runtime error", "error", err)
	}

	cancel()
	grpcSrv.GracefulStop()
}

func signalDone() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		platform.WaitForSignal()
		close(ch)
	}()
	return ch
}
