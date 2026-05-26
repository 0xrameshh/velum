package history

import (
	"context"

	"github.com/google/uuid"
	"github.com/0xrameshh/velum/internal/events"
)

type parallelActivity struct {
	Name    string
	Queue   string
	Payload map[string]any
}

func (s *Service) scheduleActivity(ctx context.Context, runID uuid.UUID, queue, activityName string, payload any) (uuid.UUID, error) {
	taskID, err := s.store.CreateTaskQueued(ctx, runID, queue, "activity", activityName, payload, 5)
	if err != nil {
		return uuid.Nil, err
	}
	return taskID, s.store.AppendEvent(ctx, runID, events.ActivityTaskScheduled, events.ActivityTaskScheduledPayload{
		TaskID:       taskID.String(),
		ActivityName: activityName,
		TaskQueue:    queue,
		Attempt:      1,
	})
}

func (s *Service) scheduleParallel(ctx context.Context, runID uuid.UUID, groupID string, activities []parallelActivity) error {
	names := make([]string, len(activities))
	for i, a := range activities {
		names[i] = a.Name
	}
	if err := s.store.AppendEvent(ctx, runID, events.ParallelBranchStarted, events.ParallelBranchStartedPayload{
		GroupID:    groupID,
		Activities: names,
	}); err != nil {
		return err
	}
	for _, a := range activities {
		if _, err := s.scheduleActivity(ctx, runID, a.Queue, a.Name, a.Payload); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) HandleTerminalFailure(ctx context.Context, runID uuid.UUID, activityName, errMsg string) error {
	run, err := s.store.GetRunByID(ctx, runID)
	if err != nil {
		return err
	}
	switch run.WorkflowName {
	case "order_saga":
		return s.orderSagaTerminalFailure(ctx, runID, activityName, errMsg)
	default:
		return s.FailWorkflow(ctx, runID, errMsg)
	}
}
