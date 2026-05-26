package history

import (
	"context"

	"github.com/google/uuid"
	"github.com/0xrameshh/velum/internal/events"
	"github.com/0xrameshh/velum/internal/persistence"
)

const timerPostGreetSleep = "post_greet_sleep"

func (s *Service) scheduleDelayedGreetFirst(ctx context.Context, runID uuid.UUID, input any) (uuid.UUID, error) {
	return s.scheduleActivity(ctx, runID, persistence.QueueDefault, "greet", map[string]any{
		"input": input,
	})
}

func (s *Service) advanceDelayedGreet(ctx context.Context, runID uuid.UUID, completedActivity string, result any) error {
	switch completedActivity {
	case "greet":
		run, err := s.store.GetRunByID(ctx, runID)
		if err != nil {
			return err
		}
		input := parseRunInput(run.Input)
		duration := sleepDurationFromInput(input)
		_, err = s.ScheduleTimer(ctx, runID, timerPostGreetSleep, duration, map[string]any{
			"greet_result": result,
			"duration":     duration.String(),
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

func (s *Service) onDelayedGreetTimer(ctx context.Context, runID uuid.UUID, timerName string, payload map[string]any) error {
	if timerName != timerPostGreetSleep {
		return nil
	}
	greetResult := payload["greet_result"]
	_, err := s.scheduleActivity(ctx, runID, persistence.QueueEmail, "send_email", map[string]any{
		"greet_result": greetResult,
	})
	return err
}
