package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/0xrameshh/velum/internal/api"
	"github.com/0xrameshh/velum/internal/config"
	"github.com/0xrameshh/velum/internal/platform"
)

func main() {
	platform.SetupLogger()

	cfg, err := config.LoadAPI()
	if err != nil {
		slog.Error("config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hist, closeHist, err := platform.ConnectHistory(ctx, cfg.HistoryGRPCAddr)
	if err != nil {
		slog.Error("history client", "error", err)
		os.Exit(1)
	}
	defer closeHist()

	httpSrv := api.NewServer(hist)
	httpServer := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      httpSrv.Router(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	errCh := make(chan error, 1)
	platform.ListenHTTP(httpServer, errCh)

	select {
	case <-signalDone():
		slog.Info("shutdown")
	case err := <-errCh:
		slog.Error("runtime error", "error", err)
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
