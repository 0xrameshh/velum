package platform

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/0xrameshh/velum/internal/persistence"
)

func SetupLogger() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
}

func ConnectDB(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		pool, err := pgxpool.New(ctx, databaseURL)
		if err != nil {
			lastErr = err
			time.Sleep(time.Second)
			continue
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			lastErr = err
			slog.Info("waiting for database", "attempt", attempt, "error", err)
			time.Sleep(time.Second)
			continue
		}
		return pool, nil
	}
	return nil, lastErr
}

func MaybeMigrate(ctx context.Context, pool *pgxpool.Pool, migrateOnStartup bool) error {
	if !migrateOnStartup {
		return nil
	}
	if err := persistence.Migrate(ctx, pool); err != nil {
		return err
	}
	slog.Info("migrations applied")
	return nil
}

func WaitForSignal() os.Signal {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("shutdown signal", "signal", sig.String())
	return sig
}

func ShutdownHTTP(server *http.Server, timeout time.Duration) {
	if server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Error("http shutdown", "error", err)
	}
}

func ListenHTTP(server *http.Server, errCh chan<- error) {
	go func() {
		slog.Info("http listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
}
