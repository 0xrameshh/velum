package history

// OrderSagaState is persisted in workflow_runs.state_json for order_saga runs.
type OrderSagaState struct {
	Phase string `json:"phase"`

	Parallel *ParallelGate `json:"parallel,omitempty"`

	PrepResults map[string]any `json:"prep_results,omitempty"`

	Compensations []CompensationStep `json:"compensations,omitempty"`
	CompIndex     int                `json:"comp_index,omitempty"`

	FailureReason string `json:"failure_reason,omitempty"`
}

type ParallelGate struct {
	GroupID   string            `json:"group_id"`
	Expected  []string          `json:"expected"`
	Completed map[string]any    `json:"completed"`
}

type CompensationStep struct {
	Activity string         `json:"activity"`
	Queue    string         `json:"queue"`
	Payload  map[string]any `json:"payload"`
}

const (
	orderPhasePrepParallel  = "prep_parallel"
	orderPhaseShip          = "ship"
	orderPhaseCompensating  = "compensating"
)
