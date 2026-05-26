package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/0xrameshh/velum/internal/executor"
	"github.com/0xrameshh/velum/internal/history"
	"github.com/0xrameshh/velum/internal/persistence"
)

// Embedded polls the database in-process (Phase 1 dev mode).
// Production workers should use GRPCRunner via cmd/velum-worker.
type Embedded struct {
	store     *persistence.Store
	executor  *executor.Executor
	workerID  string
	taskQueue string
	lease     time.Duration
	pollEvery time.Duration
}

func NewEmbedded(store *persistence.Store, hist history.Client, workerID, taskQueue string, lease, pollEvery time.Duration) *Embedded {
	return &Embedded{
		store:     store,
		executor:  executor.New(store, hist),
		workerID:  workerID,
		taskQueue: persistence.NormalizeQueue(taskQueue),
		lease:     lease,
		pollEvery: pollEvery,
	}
}

func (w *Embedded) Run(ctx context.Context) error {
	slog.Info("embedded worker started", "worker_id", w.workerID, "task_queue", w.taskQueue)
	ticker := time.NewTicker(w.pollEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("embedded worker stopped", "worker_id", w.workerID)
			return ctx.Err()
		case <-ticker.C:
			if err := w.pollOnce(ctx); err != nil {
				slog.Error("embedded worker poll", "error", err)
			}
		}
	}
}

func (w *Embedded) pollOnce(ctx context.Context) error {
	task, err := w.store.PollTaskQueue(ctx, w.workerID, w.taskQueue, time.Now().Add(w.lease))
	if err != nil {
		return err
	}
	if task == nil {
		return nil
	}

	result, err := w.executor.RunActivity(ctx, task)
	if err != nil {
		willRetry := task.Attempt < task.MaxAttempts
		_, err := w.executor.Fail(ctx, w.workerID, task, err, willRetry)
		return err
	}
	_, err = w.executor.Complete(ctx, w.workerID, task, result)
	return err
}

// New is an alias for NewEmbedded (backward compatible).
func New(store *persistence.Store, hist history.Client, workerID string, lease, pollEvery time.Duration) *Embedded {
	return NewEmbedded(store, hist, workerID, persistence.QueueDefault, lease, pollEvery)
}
