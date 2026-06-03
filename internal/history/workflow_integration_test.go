//go:build integration

// Integration tests exercising full workflow execution through history.Service.
// Requires VELUM_TEST_DATABASE_URL to point at your test DB.
// Run: go test -tags=integration ./internal/history/
package history_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/0xrameshh/velum/internal/dispatch"
	"github.com/0xrameshh/velum/internal/history"
	"github.com/0xrameshh/velum/internal/persistence"
)

const testWorker = "w-test"

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
	for _, tbl := range []string{"task_completions", "task_failures", "tasks", "timers", "events", "workflow_runs"} {
		if _, err := pool.Exec(ctx, "DELETE FROM "+tbl); err != nil {
			t.Fatalf("clean %s: %v", tbl, err)
		}
	}
	return pool
}

func testSvc(t *testing.T) *history.Service {
	t.Helper()
	return history.NewService(persistence.NewStore(testPool(t), dispatch.NewNoop()))
}

func testPollStore(t *testing.T) *persistence.Store {
	t.Helper()
	return persistence.NewStore(testPool(t), dispatch.NewNoop())
}

func poll(t *testing.T, store *persistence.Store, queues ...string) *persistence.Task {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 25; i++ {
		for _, q := range queues {
			task, err := store.PollTaskQueue(ctx, testWorker, q, time.Now().Add(30*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if task != nil {
				return task
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

func complete(t *testing.T, store *persistence.Store, svc *history.Service, task *persistence.Task, result any) {
	t.Helper()
	ctx := context.Background()
	already, err := store.CompleteTaskIdempotent(ctx, testWorker, task.ID, persistence.IdempotencyKey(task.ID, task.Attempt, "complete"), result)
	if err != nil {
		t.Fatal(err)
	}
	if already {
		t.Fatal("unexpected idempotent repeat")
	}
	if err := svc.OnActivityCompleted(ctx, task.RunID, task.ActivityName, task.ID, result); err != nil {
		t.Fatal(err)
	}
}

func failTerminal(t *testing.T, store *persistence.Store, svc *history.Service, task *persistence.Task, msg string) {
	t.Helper()
	ctx := context.Background()
	already, err := store.FailTaskIdempotent(ctx, testWorker, task.ID, persistence.IdempotencyKey(task.ID, task.Attempt, "fail"), msg, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if already {
		t.Fatal("unexpected idempotent repeat")
	}
	if err := svc.OnActivityFailed(ctx, task.RunID, task.ActivityName, task.ID, msg, false); err != nil {
		t.Fatal(err)
	}
	if err := svc.HandleTerminalFailure(ctx, task.RunID, task.ActivityName, msg); err != nil {
		t.Fatal(err)
	}
}

func eventTypes(events []persistence.Event) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.EventType
	}
	return out
}

func TestIntegrationGreetWorkflow(t *testing.T) {
	svc := testSvc(t)
	store := testPollStore(t)
	ctx := context.Background()

	runID, err := svc.StartWorkflow(ctx, "test-ns", "greet", map[string]any{"name": "Ramesh"})
	if err != nil {
		t.Fatal(err)
	}

	task := poll(t, store, persistence.QueueDefault)
	if task == nil || task.ActivityName != "greet" {
		t.Fatal("expected greet task on default queue")
	}
	complete(t, store, svc, task, map[string]any{"message": "Hello, Ramesh!"})

	task = poll(t, store, persistence.QueueEmail)
	if task == nil || task.ActivityName != "send_email" {
		t.Fatal("expected send_email task on email queue")
	}
	complete(t, store, svc, task, map[string]any{"sent": true, "subject": "Velum greeting", "body": "Hello, Ramesh!"})

	run, events, err := svc.GetRun(ctx, "test-ns", runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	types := eventTypes(events)
	if len(types) != 6 {
		t.Fatalf("expected 6 events, got %d\n  %v", len(types), types)
	}
	if types[len(types)-1] != "WorkflowExecutionCompleted" {
		t.Fatalf("expected WorkflowExecutionCompleted, got %s", types[len(types)-1])
	}
}

func TestIntegrationDelayedGreetWorkflow(t *testing.T) {
	svc := testSvc(t)
	store := testPollStore(t)
	ctx := context.Background()

	runID, err := svc.StartWorkflow(ctx, "test-ns", "delayed_greet", map[string]any{"name": "Ramesh", "sleep_seconds": 0.001})
	if err != nil {
		t.Fatal(err)
	}

	task := poll(t, store, persistence.QueueDefault)
	if task == nil || task.ActivityName != "greet" {
		t.Fatal("expected greet task")
	}
	complete(t, store, svc, task, map[string]any{"message": "Hello, Ramesh!"})

	// Fire timer (sleep_seconds=0.001 so the timer comes due almost immediately)
	var fired []persistence.Timer
	for i := 0; i < 20; i++ {
		fired, err = store.FireDueTimers(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(fired) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != 1 {
		t.Fatalf("expected 1 fired timer, got %d", len(fired))
	}
	if err := svc.OnTimerFired(ctx, runID, fired[0].ID, fired[0].TimerName, nil); err != nil {
		t.Fatal(err)
	}

	task = poll(t, store, persistence.QueueEmail)
	if task == nil || task.ActivityName != "send_email" {
		t.Fatal("expected send_email task after timer")
	}
	complete(t, store, svc, task, map[string]any{"sent": true})

	run, events, err := svc.GetRun(ctx, "test-ns", runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	types := eventTypes(events)
	expected := []string{
		"WorkflowExecutionStarted",
		"ActivityTaskScheduled",
		"ActivityTaskCompleted",
		"TimerStarted",
		"TimerFired",
		"ActivityTaskScheduled",
		"ActivityTaskCompleted",
		"WorkflowExecutionCompleted",
	}
	if len(types) != len(expected) {
		t.Fatalf("event count mismatch:\n  got:  %v\n  want: %v", types, expected)
	}
}

func TestIntegrationOrderSagaHappyPath(t *testing.T) {
	svc := testSvc(t)
	store := testPollStore(t)
	ctx := context.Background()

	runID, err := svc.StartWorkflow(ctx, "test-ns", "order_saga", map[string]any{
		"order_id":  "ord-int-1",
		"fail_ship": false,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Poll both parallel tasks
	byName := map[string]*persistence.Task{}
	for i := 0; i < 2; i++ {
		task := poll(t, store, persistence.QueueDefault, persistence.QueuePayments)
		if task == nil {
			t.Fatal("expected parallel branch task")
		}
		byName[task.ActivityName] = task
	}

	if _, ok := byName["charge_card"]; !ok {
		t.Fatal("missing charge_card task")
	}
	if _, ok := byName["reserve_stock"]; !ok {
		t.Fatal("missing reserve_stock task")
	}

	// Complete both — this triggers the atomic state update
	complete(t, store, svc, byName["charge_card"], map[string]any{"charged": true, "amount": 99.99, "payment_id": "pay-int-1"})
	complete(t, store, svc, byName["reserve_stock"], map[string]any{"reserved": true, "sku": "SKU-42", "units": 1})

	// Ship task should appear on default queue after both prep tasks complete
	var shipTask *persistence.Task
	for i := 0; i < 25; i++ {
		shipTask = poll(t, store, persistence.QueueDefault)
		if shipTask != nil {
			break
		}
	}
	if shipTask == nil {
		t.Fatal("expected ship_order task — parallel branches may have raced")
	}
	if shipTask.ActivityName != "ship_order" {
		t.Fatalf("expected ship_order, got %s", shipTask.ActivityName)
	}
	complete(t, store, svc, shipTask, map[string]any{"shipped": true, "tracking": "TRACK-42"})

	run, events, err := svc.GetRun(ctx, "test-ns", runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s — saga transition failed", run.Status)
	}

	types := eventTypes(events)
	hasParallelBranchCompleted := false
	for _, et := range types {
		if et == "ParallelBranchCompleted" {
			hasParallelBranchCompleted = true
			break
		}
	}
	if !hasParallelBranchCompleted {
		t.Fatal("missing ParallelBranchCompleted event — race condition!")
	}
}

func TestIntegrationOrderSagaCompensation(t *testing.T) {
	svc := testSvc(t)
	store := testPollStore(t)
	ctx := context.Background()

	runID, err := svc.StartWorkflow(ctx, "test-ns", "order_saga", map[string]any{
		"order_id":  "ord-int-fail",
		"fail_ship": true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Poll both parallel tasks
	byName := map[string]*persistence.Task{}
	for i := 0; i < 2; i++ {
		task := poll(t, store, persistence.QueueDefault, persistence.QueuePayments)
		if task == nil {
			t.Fatal("expected parallel task")
		}
		byName[task.ActivityName] = task
	}

	complete(t, store, svc, byName["charge_card"], map[string]any{"charged": true, "amount": 99.99, "payment_id": "pay-fail"})
	complete(t, store, svc, byName["reserve_stock"], map[string]any{"reserved": true, "sku": "SKU-FAIL", "units": 1})

	// Ship task
	var shipTask *persistence.Task
	for i := 0; i < 25; i++ {
		shipTask = poll(t, store, persistence.QueueDefault)
		if shipTask != nil {
			break
		}
	}
	if shipTask == nil || shipTask.ActivityName != "ship_order" {
		t.Fatal("expected ship_order task")
	}

	// Fail ship without retry → triggers compensation
	failTerminal(t, store, svc, shipTask, "ship failed")

	// Compensation tasks: refund_payment (payments) + release_stock (default)
	time.Sleep(200 * time.Millisecond)

	compNames := map[string]bool{"refund_payment": true, "release_stock": true}
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		task := poll(t, store, persistence.QueueDefault, persistence.QueuePayments)
		if task == nil {
			t.Fatal("expected compensation task")
		}
		if !compNames[task.ActivityName] {
			t.Fatalf("unexpected compensation activity: %s", task.ActivityName)
		}
		if seen[task.ActivityName] {
			t.Fatalf("duplicate compensation: %s", task.ActivityName)
		}
		seen[task.ActivityName] = true

		switch task.ActivityName {
		case "refund_payment":
			complete(t, store, svc, task, map[string]any{"refunded": true})
		case "release_stock":
			complete(t, store, svc, task, map[string]any{"released": true})
		}
	}

	run, events, err := svc.GetRun(ctx, "test-ns", runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "failed" {
		t.Fatalf("expected failed, got %s", run.Status)
	}

	types := eventTypes(events)
	hasCompensation := false
	hasFailed := false
	for _, et := range types {
		switch et {
		case "CompensationScheduled":
			hasCompensation = true
		case "WorkflowExecutionFailed":
			hasFailed = true
		}
	}
	if !hasCompensation {
		t.Fatal("missing CompensationScheduled event")
	}
	if !hasFailed {
		t.Fatal("missing WorkflowExecutionFailed event")
	}
}
