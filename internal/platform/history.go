package platform

import (
	"context"
	"log/slog"
	"time"

	"github.com/0xrameshh/velum/internal/history"
	"github.com/0xrameshh/velum/internal/historyrpc"
)

// ConnectHistory dials the history gRPC service with retries.
func ConnectHistory(ctx context.Context, addr string) (history.Client, func(), error) {
	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, err := historyrpc.Dial(dialCtx, addr)
		cancel()
		if err != nil {
			lastErr = err
			slog.Info("waiting for history service", "addr", addr, "attempt", attempt, "error", err)
			time.Sleep(time.Second)
			continue
		}
		client := historyrpc.NewGRPCClient(conn)
		return client, func() { _ = conn.Close() }, nil
	}
	return nil, nil, lastErr
}
