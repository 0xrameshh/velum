package history

import (
	"context"

	"github.com/google/uuid"
	"github.com/0xrameshh/velum/internal/events"
	"github.com/0xrameshh/velum/internal/persistence"
)

type Service struct {
	store *persistence.Store
}

func NewService(store *persistence.Store) *Service {
	return &Service{store: store}
}

func (s *Service) StartWorkflow(ctx context.Context, namespace, workflowName string, input any) (uuid.UUID, error) {
	if err := persistence.ValidateWorkflow(workflowName); err != nil {
		return uuid.Nil, err
	}

	runID, err := s.store.CreateRun(ctx, namespace, workflowName, input)
	if err != nil {
		return uuid.Nil, err
	}

	if err := s.store.AppendEvent(ctx, runID, events.WorkflowExecutionStarted, events.WorkflowExecutionStartedPayload{
		Namespace:    namespace,
		WorkflowName: workflowName,
		Input:        input,
	}); err != nil {
		return uuid.Nil, err
	}

	_, err = s.scheduleFirstStep(ctx, runID, workflowName, input)
	return runID, err
}

func (s *Service) scheduleFirstStep(ctx context.Context, runID uuid.UUID, workflowName string, input any) (uuid.UUID, error) {
	switch workflowName {
	case "greet":
		return s.scheduleActivity(ctx, runID, persistence.QueueDefault, "greet", map[string]any{
			"input": input,
		})
	case "delayed_greet":
		return s.scheduleDelayedGreetFirst(ctx, runID, input)
	case "order_saga":
		return uuid.Nil, s.startOrderSaga(ctx, runID, input)
	default:
		return uuid.Nil, persistence.ValidateWorkflow(workflowName)
	}
}

func (s *Service) OnActivityCompleted(ctx context.Context, runID uuid.UUID, activityName string, taskID uuid.UUID, result any) error {
	if err := s.store.AppendEvent(ctx, runID, events.ActivityTaskCompleted, events.ActivityTaskCompletedPayload{
		TaskID:       taskID.String(),
		ActivityName: activityName,
		Result:       result,
	}); err != nil {
		return err
	}

	run, err := s.store.GetRunByID(ctx, runID)
	if err != nil {
		return err
	}

	switch run.WorkflowName {
	case "greet":
		return s.advanceGreet(ctx, runID, activityName, result)
	case "delayed_greet":
		return s.advanceDelayedGreet(ctx, runID, activityName, result)
	case "order_saga":
		return s.advanceOrderSaga(ctx, runID, activityName, result)
	default:
		return nil
	}
}

func (s *Service) advanceGreet(ctx context.Context, runID uuid.UUID, completedActivity string, result any) error {
	switch completedActivity {
	case "greet":
		_, err := s.scheduleActivity(ctx, runID, persistence.QueueEmail, "send_email", map[string]any{
			"greet_result": result,
		})
		return err
	case "send_email":
		if err := s.store.AppendEvent(ctx, runID, events.WorkflowExecutionCompleted, events.WorkflowExecutionCompletedPayload{
			Result: result,
		}); err != nil {
			return err
		}
		return s.store.MarkRunCompleted(ctx, runID)
	default:
		return nil
	}
}

func (s *Service) OnActivityFailed(ctx context.Context, runID uuid.UUID, activityName string, taskID uuid.UUID, errMsg string, willRetry bool) error {
	return s.store.AppendEvent(ctx, runID, events.ActivityTaskFailed, events.ActivityTaskFailedPayload{
		TaskID:       taskID.String(),
		ActivityName: activityName,
		Error:        errMsg,
		WillRetry:    willRetry,
	})
}

func (s *Service) FailWorkflow(ctx context.Context, runID uuid.UUID, errMsg string) error {
	if err := s.store.AppendEvent(ctx, runID, events.WorkflowExecutionFailed, events.WorkflowExecutionFailedPayload{
		Error: errMsg,
	}); err != nil {
		return err
	}
	return s.store.MarkRunFailed(ctx, runID)
}

func (s *Service) GetRun(ctx context.Context, namespace string, runID uuid.UUID) (*persistence.WorkflowRun, []persistence.Event, error) {
	run, err := s.store.GetRun(ctx, namespace, runID)
	if err != nil {
		return nil, nil, err
	}
	evs, err := s.store.ListEvents(ctx, runID)
	if err != nil {
		return nil, nil, err
	}
	return run, evs, nil
}
