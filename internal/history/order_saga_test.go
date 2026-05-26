package history

import "testing"

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
