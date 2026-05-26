package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/0xrameshh/velum/internal/history"
	"github.com/0xrameshh/velum/internal/persistence"
	"github.com/0xrameshh/velum/internal/workflow"
)

type Executor struct {
	store   *persistence.Store
	history history.Client
}

func New(store *persistence.Store, hist history.Client) *Executor {
	return &Executor{store: store, history: hist}
}

func (e *Executor) RunActivity(ctx context.Context, task *persistence.Task) (any, error) {
	activity, ok := workflow.Registry[task.ActivityName]
	if !ok {
		return nil, fmt.Errorf("unknown activity %q", task.ActivityName)
	}
	return activity(ctx, task.Payload)
}

func (e *Executor) Complete(ctx context.Context, workerID string, task *persistence.Task, result any) (alreadyApplied bool, err error) {
	key := persistence.IdempotencyKey(task.ID, task.Attempt, "complete")
	alreadyApplied, err = e.store.CompleteTaskIdempotent(ctx, workerID, task.ID, key, result)
	if err != nil {
		return false, err
	}
	if alreadyApplied {
		return true, nil
	}
	return false, e.history.OnActivityCompleted(ctx, task.RunID, task.ActivityName, task.ID, result)
}

func (e *Executor) Fail(ctx context.Context, workerID string, task *persistence.Task, execErr error, willRetry bool) (alreadyApplied bool, err error) {
	errMsg := execErr.Error()
	key := persistence.IdempotencyKey(task.ID, task.Attempt, "fail")

	var retryAt *time.Time
	if willRetry {
		t := time.Now().Add(persistence.Backoff(task.Attempt))
		retryAt = &t
	}

	alreadyApplied, err = e.store.FailTaskIdempotent(ctx, workerID, task.ID, key, errMsg, willRetry, retryAt)
	if err != nil {
		return false, err
	}
	if alreadyApplied {
		return true, nil
	}

	if err := e.history.OnActivityFailed(ctx, task.RunID, task.ActivityName, task.ID, errMsg, willRetry); err != nil {
		return false, err
	}
	if !willRetry {
		return false, e.history.HandleTerminalFailure(ctx, task.RunID, task.ActivityName, errMsg)
	}
	return false, nil
}
