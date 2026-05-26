package history

import (
	"context"

	"github.com/google/uuid"
	"github.com/0xrameshh/velum/internal/persistence"
)

// Client is the workflow history API used by api, matcher, and scheduler.
type Client interface {
	StartWorkflow(ctx context.Context, namespace, workflowName string, input any) (uuid.UUID, error)
	GetRun(ctx context.Context, namespace string, runID uuid.UUID) (*persistence.WorkflowRun, []persistence.Event, error)
	OnActivityCompleted(ctx context.Context, runID uuid.UUID, activityName string, taskID uuid.UUID, result any) error
	OnActivityFailed(ctx context.Context, runID uuid.UUID, activityName string, taskID uuid.UUID, errMsg string, willRetry bool) error
	HandleTerminalFailure(ctx context.Context, runID uuid.UUID, activityName, errMsg string) error
	OnTimerFired(ctx context.Context, runID, timerID uuid.UUID, timerName string, payload map[string]any) error
}

var _ Client = (*Service)(nil)
