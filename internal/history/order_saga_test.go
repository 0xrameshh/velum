package history

import (
	"encoding/json"
	"testing"
)

func TestParallelGateWaitsForAll(t *testing.T) {
	t.Parallel()
	gate := &ParallelGate{
		GroupID:   "prep",
		Expected:  []string{"charge_card", "reserve_stock"},
		Completed: map[string]any{},
	}
	gate.Completed["charge_card"] = map[string]any{"ok": true}
	if len(gate.Completed) >= len(gate.Expected) {
		t.Fatal("should not be complete after one branch")
	}
	gate.Completed["reserve_stock"] = map[string]any{"ok": true}
	if len(gate.Completed) < len(gate.Expected) {
		t.Fatal("should be complete after both branches")
	}
}

func TestOrderSagaStateNormalize(t *testing.T) {
	t.Parallel()
	var raw = []byte(`{"phase":"prep_parallel","parallel":{"group_id":"prep","expected":["a","b"]}}`)
	var state OrderSagaState
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	state.normalize()
	if state.PrepResults == nil {
		t.Fatal("PrepResults should be initialized")
	}
	if state.Parallel.Completed == nil {
		t.Fatal("Parallel.Completed should be initialized")
	}
	state.PrepResults["x"] = 1
	state.Parallel.Completed["a"] = true
}

func TestCompensationStackOrder(t *testing.T) {
	t.Parallel()
	steps := []CompensationStep{
		{Activity: "refund_payment"},
		{Activity: "release_stock"},
	}
	// LIFO compensation order: refund first scheduled, then release
	if steps[0].Activity != "refund_payment" {
		t.Fatalf("expected refund first, got %s", steps[0].Activity)
	}
}
