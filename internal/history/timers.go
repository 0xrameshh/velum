package history

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/0xrameshh/velum/internal/events"
)

func (s *Service) ScheduleTimer(ctx context.Context, runID uuid.UUID, timerName string, duration time.Duration, payload any) (uuid.UUID, error) {
	fireAt := time.Now().UTC().Add(duration)
	timerID, err := s.store.CreateTimer(ctx, runID, timerName, fireAt, payload)
	if err != nil {
		return uuid.Nil, err
	}
	if err := s.store.AppendEvent(ctx, runID, events.TimerStarted, events.TimerStartedPayload{
		TimerID:   timerID.String(),
		TimerName: timerName,
		FireAt:    fireAt.Format(time.RFC3339Nano),
	}); err != nil {
		return uuid.Nil, err
	}
	return timerID, nil
}

func (s *Service) OnTimerFired(ctx context.Context, runID, timerID uuid.UUID, timerName string, payload map[string]any) error {
	if err := s.store.AppendEvent(ctx, runID, events.TimerFired, events.TimerFiredPayload{
		TimerID:   timerID.String(),
		TimerName: timerName,
	}); err != nil {
		return err
	}

	run, err := s.store.GetRunByID(ctx, runID)
	if err != nil {
		return err
	}

	switch run.WorkflowName {
	case "delayed_greet":
		return s.onDelayedGreetTimer(ctx, runID, timerName, payload)
	default:
		return nil
	}
}

func parseRunInput(raw json.RawMessage) map[string]any {
	var input map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &input)
	}
	if input == nil {
		input = map[string]any{}
	}
	return input
}

func sleepDurationFromInput(input map[string]any) time.Duration {
	switch v := input["sleep_seconds"].(type) {
	case float64:
		if v > 0 {
			return time.Duration(v * float64(time.Second))
		}
	case int:
		if v > 0 {
			return time.Duration(v) * time.Second
		}
	}
	return 5 * time.Second
}
