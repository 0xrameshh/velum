//go:build integration

// Integration tests requiring a real PostgreSQL instance.
// Set VELUM_TEST_DATABASE_URL to point at your test DB.
// Run: go test -tags=integration ./internal/persistence/
package persistence_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/0xrameshh/velum/internal/dispatch"
	"github.com/0xrameshh/velum/internal/persistence"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("VELUM_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://velum:velum@localhost:5432/velum?sslmode=disable"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := persistence.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func testStore(t *testing.T) *persistence.Store {
	t.Helper()
	return persistence.NewStore(testPool(t), dispatch.NewNoop())
}

func TestIntegrationTaskLifecycle(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	queue := "lifecycle-" + uuid.New().String()[:8]

	runID, err := store.CreateRun(ctx, "test-ns", "greet", map[string]any{"name": "tester"})
	if err != nil {
		t.Fatal(err)
	}

	taskID, err := store.CreateTaskQueued(ctx, runID, queue, "activity", "greet", map[string]any{"name": "tester"}, 3)
	if err != nil {
		t.Fatal(err)
	}

	task, err := store.PollTaskQueue(ctx, "worker-1", queue, time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if task == nil {
		t.Fatal("expected a task")
	}
	if task.ID != taskID {
		t.Fatalf("expected task %s, got %s", taskID, task.ID)
	}
	if task.Status != "leased" {
		t.Fatalf("expected leased, got %s", task.Status)
	}

	already, err := store.CompleteTaskIdempotent(ctx, "worker-1", taskID, persistence.IdempotencyKey(taskID, 1, "complete"), map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	if already {
		t.Fatal("expected first completion")
	}

	already, err = store.CompleteTaskIdempotent(ctx, "worker-1", taskID, persistence.IdempotencyKey(taskID, 1, "complete"), map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	if !already {
		t.Fatal("expected idempotent repeat to return already=true")
	}

	task, err = store.GetTask(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != "completed" {
		t.Fatalf("expected completed, got %s", task.Status)
	}
}

func TestIntegrationTaskPollOrdering(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	queue := "order-" + uuid.New().String()[:8]

	runID, err := store.CreateRun(ctx, "test-ns", "test", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}

	var ids []uuid.UUID
	for i := 0; i < 5; i++ {
		id, err := store.CreateTaskQueued(ctx, runID, queue, "activity", "task", map[string]any{"seq": i}, 3)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	for i := 0; i < 5; i++ {
		task, err := store.PollTaskQueue(ctx, "worker-1", queue, time.Now().Add(30*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if task == nil {
			t.Fatalf("expected task %d", i)
		}
		if task.ID != ids[i] {
			t.Fatalf("poll %d: expected %s, got %s", i, ids[i], task.ID)
		}
	}
}

func TestIntegrationLeaseReclaim(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	queue := "reclaim-" + uuid.New().String()[:8]

	runID, err := store.CreateRun(ctx, "test-ns", "test", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}

	taskID, err := store.CreateTaskQueued(ctx, runID, queue, "activity", "task", map[string]any{}, 3)
	if err != nil {
		t.Fatal(err)
	}

	task, err := store.PollTaskQueue(ctx, "worker-1", queue, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if task == nil {
		t.Fatal("expected task")
	}
	if task.ID != taskID {
		t.Fatalf("expected task %s, got %s", taskID, task.ID)
	}

	n, err := store.ReclaimExpiredLeases(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 reclaimed, got %d", n)
	}

	task2, err := store.PollTaskQueue(ctx, "worker-2", queue, time.Now().Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if task2 == nil {
		t.Fatal("expected task after reclaim")
	}
	if task2.ID != taskID {
		t.Fatalf("expected task %s, got %s", taskID, task2.ID)
	}
}

func TestIntegrationTaskFailureRetry(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	queue := "retry-" + uuid.New().String()[:8]

	runID, err := store.CreateRun(ctx, "test-ns", "test", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}

	taskID, err := store.CreateTaskQueued(ctx, runID, queue, "activity", "task", map[string]any{}, 3)
	if err != nil {
		t.Fatal(err)
	}

	poll := func(worker string) *persistence.Task {
		task, err := store.PollTaskQueue(ctx, worker, queue, time.Now().Add(30*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		return task
	}

	task := poll("wrk-1")
	if task == nil {
		t.Fatal("expected task")
	}
	if task.ID != taskID {
		t.Fatalf("expected task %s, got %s", taskID, task.ID)
	}

	past := time.Now().Add(-time.Second)
	already, err := store.FailTaskIdempotent(ctx, "wrk-1", taskID, persistence.IdempotencyKey(taskID, 1, "fail"), "nope", true, &past)
	if err != nil {
		t.Fatal(err)
	}
	if already {
		t.Fatal("expected first failure")
	}

	task = poll("wrk-2")
	if task == nil {
		t.Fatal("expected task on retry")
	}
	if task.Attempt != 2 {
		t.Fatalf("expected attempt 2, got %d", task.Attempt)
	}
	if task.ID != taskID {
		t.Fatalf("expected same task ID after retry")
	}

	already, err = store.FailTaskIdempotent(ctx, "wrk-2", taskID, persistence.IdempotencyKey(taskID, 2, "fail"), "final nope", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if already {
		t.Fatal("expected second failure")
	}

	task, err = store.GetTask(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != "failed" {
		t.Fatalf("expected failed, got %s", task.Status)
	}
}

func TestIntegrationAtomicStateUpdate(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	runID, err := store.CreateRun(ctx, "test-ns", "test", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}

	type TestState struct {
		Counters map[string]int `json:"counters"`
	}
	if err := store.SetRunState(ctx, runID, TestState{Counters: map[string]int{}}); err != nil {
		t.Fatal(err)
	}

	const goroutines = 10
	const incrementsPer = 100
	errCh := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < incrementsPer; j++ {
				var st TestState
				err := store.AtomicUpdateRunState(ctx, runID, &st, func() error {
					if st.Counters == nil {
						st.Counters = map[string]int{}
					}
					st.Counters["counter"]++
					return nil
				})
				if err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}()
	}

	for i := 0; i < goroutines; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}

	var final TestState
	if err := store.GetRunState(ctx, runID, &final); err != nil {
		t.Fatal(err)
	}
	expected := goroutines * incrementsPer
	if final.Counters["counter"] != expected {
		t.Fatalf("expected counter=%d, got %d — state updates raced!", expected, final.Counters["counter"])
	}
}

func TestIntegrationTimerLifecycle(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	runID, err := store.CreateRun(ctx, "test-ns", "test", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}

	timerID, err := store.CreateTimer(ctx, runID, "test_timer", time.Now().Add(-time.Second), map[string]any{"foo": "bar"})
	if err != nil {
		t.Fatal(err)
	}

	fired, err := store.FireDueTimers(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != 1 {
		t.Fatalf("expected 1 fired timer, got %d", len(fired))
	}
	if fired[0].ID != timerID {
		t.Fatalf("expected timer %s, got %s", timerID, fired[0].ID)
	}

	fired, err = store.FireDueTimers(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != 0 {
		t.Fatalf("expected 0 fired timers on repeat, got %d", len(fired))
	}
}

func TestIntegrationWorkflowRunCRUD(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	runID, err := store.CreateRun(ctx, "test-ns", "greet", map[string]any{"name": "test"})
	if err != nil {
		t.Fatal(err)
	}

	run, err := store.GetRun(ctx, "test-ns", runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Namespace != "test-ns" {
		t.Fatalf("expected test-ns, got %s", run.Namespace)
	}
	if run.Status != "running" {
		t.Fatalf("expected running, got %s", run.Status)
	}

	if err := store.MarkRunCompleted(ctx, runID); err != nil {
		t.Fatal(err)
	}
	run, err = store.GetRun(ctx, "test-ns", runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	if run.CompletedAt == nil {
		t.Fatal("expected completed_at to be set")
	}

	evs, err := store.ListEvents(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 0 {
		t.Fatalf("expected 0 events, got %d", len(evs))
	}

	type ev struct{ Msg string }
	if err := store.AppendEvent(ctx, runID, "test_event", ev{Msg: "hello"}); err != nil {
		t.Fatal(err)
	}
	evs, err = store.ListEvents(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evs))
	}
	if evs[0].EventType != "test_event" {
		t.Fatalf("expected test_event, got %s", evs[0].EventType)
	}
	var decoded ev
	if err := json.Unmarshal(evs[0].Payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Msg != "hello" {
		t.Fatalf("expected hello, got %s", decoded.Msg)
	}
}
