//go:build integration

// Benchmarks for task throughput using a real PostgreSQL instance.
// Requires VELUM_TEST_DATABASE_URL to point at your test DB.
// Run: go test -tags=integration -bench=. ./internal/persistence/
package persistence_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/0xrameshh/velum/internal/dispatch"
	"github.com/0xrameshh/velum/internal/persistence"
)

func benchPool(b *testing.B) *pgxpool.Pool {
	dsn := os.Getenv("VELUM_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://velum:velum@localhost:5432/velum?sslmode=disable"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		b.Fatalf("connect: %v", err)
	}
	b.Cleanup(pool.Close)
	if err := persistence.Migrate(ctx, pool); err != nil {
		b.Fatalf("migrate: %v", err)
	}
	return pool
}

func benchStore(b *testing.B) *persistence.Store {
	return persistence.NewStore(benchPool(b), dispatch.NewNoop())
}

// createBenchRun creates a workflow run for benchmarking and returns its ID.
func createBenchRun(b *testing.B, store *persistence.Store) uuid.UUID {
	b.Helper()
	runID, err := store.CreateRun(context.Background(), "bench", "benchmark", map[string]any{})
	if err != nil {
		b.Fatal(err)
	}
	return runID
}

// BenchmarkTaskThroughput measures tasks/second for create → poll → complete.
func BenchmarkTaskThroughput(b *testing.B) {
	store := benchStore(b)
	ctx := context.Background()
	runID := createBenchRun(b, store)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		taskID, err := store.CreateTaskQueued(ctx, runID, persistence.QueueDefault, "activity", "bench", map[string]any{"seq": i}, 1)
		if err != nil {
			b.Fatal(err)
		}

		task, err := store.PollTaskQueue(ctx, "bench-worker", persistence.QueueDefault, time.Now().Add(30*time.Second))
		if err != nil {
			b.Fatal(err)
		}
		if task == nil || task.ID != taskID {
			b.Fatal("expected matching task")
		}

		already, err := store.CompleteTaskIdempotent(ctx, "bench-worker", task.ID, persistence.IdempotencyKey(task.ID, 1, "complete"), map[string]any{"ok": true})
		if err != nil {
			b.Fatal(err)
		}
		if already {
			b.Fatal("unexpected idempotent repeat")
		}
	}
}

// BenchmarkAtomicStateUpdate measures throughput of serialized state updates.
func BenchmarkAtomicStateUpdate(b *testing.B) {
	store := benchStore(b)
	ctx := context.Background()
	runID := createBenchRun(b, store)

	type Counter struct {
		Val int `json:"val"`
	}
	if err := store.SetRunState(ctx, runID, Counter{Val: 0}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var c Counter
		err := store.AtomicUpdateRunState(ctx, runID, &c, func() error {
			c.Val++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
