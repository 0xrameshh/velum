package events

const (
	WorkflowExecutionStarted   = "WorkflowExecutionStarted"
	ActivityTaskScheduled      = "ActivityTaskScheduled"
	ActivityTaskStarted        = "ActivityTaskStarted"
	ActivityTaskCompleted      = "ActivityTaskCompleted"
	ActivityTaskFailed         = "ActivityTaskFailed"
	TimerStarted               = "TimerStarted"
	TimerFired                 = "TimerFired"
	ParallelBranchStarted      = "ParallelBranchStarted"
	ParallelBranchCompleted    = "ParallelBranchCompleted"
	CompensationScheduled      = "CompensationScheduled"
	WorkflowExecutionCompleted = "WorkflowExecutionCompleted"
	WorkflowExecutionFailed    = "WorkflowExecutionFailed"
)

type WorkflowExecutionStartedPayload struct {
	Namespace    string `json:"namespace"`
	WorkflowName string `json:"workflow_name"`
	Input        any    `json:"input,omitempty"`
}

type ActivityTaskScheduledPayload struct {
	TaskID        string `json:"task_id"`
	ActivityName  string `json:"activity_name"`
	TaskQueue     string `json:"task_queue"`
	Attempt       int    `json:"attempt"`
}

type ActivityTaskCompletedPayload struct {
	TaskID       string `json:"task_id"`
	ActivityName string `json:"activity_name"`
	Result       any    `json:"result,omitempty"`
}

type ActivityTaskFailedPayload struct {
	TaskID       string `json:"task_id"`
	ActivityName string `json:"activity_name"`
	Error        string `json:"error"`
	WillRetry    bool   `json:"will_retry"`
}

type WorkflowExecutionCompletedPayload struct {
	Result any `json:"result,omitempty"`
}

type WorkflowExecutionFailedPayload struct {
	Error string `json:"error"`
}

type TimerStartedPayload struct {
	TimerID   string `json:"timer_id"`
	TimerName string `json:"timer_name"`
	FireAt    string `json:"fire_at"`
}

type TimerFiredPayload struct {
	TimerID   string `json:"timer_id"`
	TimerName string `json:"timer_name"`
}

type ParallelBranchStartedPayload struct {
	GroupID    string   `json:"group_id"`
	Activities []string `json:"activities"`
}

type ParallelBranchCompletedPayload struct {
	GroupID string         `json:"group_id"`
	Results map[string]any `json:"results"`
}

type CompensationScheduledPayload struct {
	Activities []string `json:"activities"`
	Reason     string   `json:"reason"`
}
