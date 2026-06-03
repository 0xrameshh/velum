package dispatch

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Notifier wakes matchers and schedulers when Postgres rows are inserted.
// Postgres remains the source of truth; Redis is an optional wake signal only.
type Notifier interface {
	NotifyTask(ctx context.Context, queue string, taskID uuid.UUID) error
	NotifyTimer(ctx context.Context, timerID uuid.UUID, fireAt time.Time) error
	WaitTask(ctx context.Context, queue string, timeout time.Duration) error
	WaitTimer(ctx context.Context, timeout time.Duration) error
	Close() error
}

func TaskChannel(queue string) string {
	return "velum:tasks:" + queue
}

const TimerWakeChannel = "velum:timer-wake"
