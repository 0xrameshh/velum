package history

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/0xrameshh/velum/internal/events"
	"github.com/0xrameshh/velum/internal/persistence"
)

func (s *Service) startOrderSaga(ctx context.Context, runID uuid.UUID, input any) error {
	in := inputMap(input)
	orderID, _ := in["order_id"].(string)
	if orderID == "" {
		orderID = "demo-order"
	}
	base := map[string]any{"order_id": orderID, "input": in}

	state := OrderSagaState{
		Phase: orderPhasePrepParallel,
		Parallel: &ParallelGate{
			GroupID:   "prep",
			Expected:  []string{"charge_card", "reserve_stock"},
			Completed: map[string]any{},
		},
		PrepResults: map[string]any{},
	}
	if err := s.store.SetRunState(ctx, runID, state); err != nil {
		return err
	}

	return s.scheduleParallel(ctx, runID, "prep", []parallelActivity{
		{Name: "charge_card", Queue: persistence.QueuePayments, Payload: base},
		{Name: "reserve_stock", Queue: persistence.QueueDefault, Payload: base},
	})
}

func (s *Service) advanceOrderSaga(ctx context.Context, runID uuid.UUID, activityName string, result any) error {
	// Fast path: ship and compensation phases don't race (single path/task at a time).
	{
		var state OrderSagaState
		if err := s.store.GetRunState(ctx, runID, &state); err != nil {
			return err
		}
		state.normalize()
		switch state.Phase {
		case orderPhaseShip:
			return s.orderSagaShipComplete(ctx, runID, result)
		case orderPhaseCompensating:
			return s.advanceSagaCompensation(ctx, runID, activityName)
		}
	}

	// prep_parallel phase: concurrent branches may race. Use atomic read-modify-write
	// with row-level locking (FOR UPDATE) to serialize state mutations.
	var state OrderSagaState
	var transition bool
	err := s.store.AtomicUpdateRunState(ctx, runID, &state, func() error {
		state.normalize()

		if state.Phase != orderPhasePrepParallel {
			return nil // already moved on
		}
		if state.Parallel == nil {
			return fmt.Errorf("order_saga: missing parallel gate")
		}

		state.Parallel.Completed[activityName] = result
		state.PrepResults[activityName] = result

		if len(state.Parallel.Completed) < len(state.Parallel.Expected) {
			return nil // not all done yet; atomic update auto-saves
		}

		// All parallel branches completed — transition to ship
		state.Phase = orderPhaseShip
		state.Parallel = nil
		transition = true
		return nil
	})
	if err != nil {
		return err
	}

	if !transition {
		return nil
	}

	// Side effects after state is safely committed
	if err := s.store.AppendEvent(ctx, runID, events.ParallelBranchCompleted, events.ParallelBranchCompletedPayload{
		GroupID: "prep",
		Results: state.PrepResults,
	}); err != nil {
		return err
	}

	return s.scheduleShipOrder(ctx, runID, state.PrepResults)
}

func (s *Service) scheduleShipOrder(ctx context.Context, runID uuid.UUID, prepResults map[string]any) error {
	run, err := s.store.GetRunByID(ctx, runID)
	if err != nil {
		return err
	}
	in := parseRunInput(run.Input)
	_, err = s.scheduleActivity(ctx, runID, persistence.QueueDefault, "ship_order", map[string]any{
		"order_id":     in["order_id"],
		"input":        in,
		"prep_results": prepResults,
	})
	return err
}

func (s *Service) orderSagaShipComplete(ctx context.Context, runID uuid.UUID, result any) error {
	if err := s.store.AppendEvent(ctx, runID, events.WorkflowExecutionCompleted, events.WorkflowExecutionCompletedPayload{
		Result: result,
	}); err != nil {
		return err
	}
	return s.store.MarkRunCompleted(ctx, runID)
}

func (s *Service) orderSagaTerminalFailure(ctx context.Context, runID uuid.UUID, activityName, errMsg string) error {
	var state OrderSagaState
	if err := s.store.GetRunState(ctx, runID, &state); err != nil {
		return err
	}
	state.normalize()

	if activityName == "ship_order" && state.Phase == orderPhaseShip {
		return s.beginOrderSagaCompensation(ctx, runID, &state, errMsg)
	}
	return s.FailWorkflow(ctx, runID, errMsg)
}

func (s *Service) beginOrderSagaCompensation(ctx context.Context, runID uuid.UUID, state *OrderSagaState, errMsg string) error {
	run, err := s.store.GetRunByID(ctx, runID)
	if err != nil {
		return err
	}
	in := parseRunInput(run.Input)
	orderID, _ := in["order_id"].(string)
	base := map[string]any{"order_id": orderID, "input": in}

	var steps []CompensationStep
	if charge, ok := state.PrepResults["charge_card"]; ok {
		steps = append(steps, CompensationStep{
			Activity: "refund_payment",
			Queue:    persistence.QueuePayments,
			Payload:  mergeMaps(base, map[string]any{"charge_result": charge}),
		})
	}
	if stock, ok := state.PrepResults["reserve_stock"]; ok {
		steps = append(steps, CompensationStep{
			Activity: "release_stock",
			Queue:    persistence.QueueDefault,
			Payload:  mergeMaps(base, map[string]any{"reserve_result": stock}),
		})
	}

	names := make([]string, len(steps))
	for i, st := range steps {
		names[i] = st.Activity
	}
	if err := s.store.AppendEvent(ctx, runID, events.CompensationScheduled, events.CompensationScheduledPayload{
		Activities: names,
		Reason:     errMsg,
	}); err != nil {
		return err
	}

	state.Phase = orderPhaseCompensating
	state.Compensations = steps
	state.CompIndex = 0
	state.FailureReason = errMsg
	if err := s.store.SetRunState(ctx, runID, state); err != nil {
		return err
	}

	if len(steps) == 0 {
		return s.FailWorkflow(ctx, runID, errMsg)
	}
	_, err = s.scheduleActivity(ctx, runID, steps[0].Queue, steps[0].Activity, steps[0].Payload)
	return err
}

func (s *Service) advanceSagaCompensation(ctx context.Context, runID uuid.UUID, activityName string) error {
	var state OrderSagaState
	if err := s.store.GetRunState(ctx, runID, &state); err != nil {
		return err
	}
	state.normalize()

	if state.Phase != orderPhaseCompensating {
		return nil
	}

	if state.CompIndex >= len(state.Compensations)-1 {
		return s.FailWorkflow(ctx, runID, state.FailureReason)
	}
	state.CompIndex++
	if err := s.store.SetRunState(ctx, runID, state); err != nil {
		return err
	}
	next := state.Compensations[state.CompIndex]
	_, err := s.scheduleActivity(ctx, runID, next.Queue, next.Activity, next.Payload)
	return err
}

func inputMap(input any) map[string]any {
	if m, ok := input.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func mergeMaps(base map[string]any, extra map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}
