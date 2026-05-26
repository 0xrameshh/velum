package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

type WorkflowRun struct {
	ID           uuid.UUID
	Namespace    string
	WorkflowName string
	Status       string
	Input        json.RawMessage
	CreatedAt    time.Time
	UpdatedAt    time.Time
	CompletedAt  *time.Time
}

type Task struct {
	ID             uuid.UUID
	RunID          uuid.UUID
	TaskQueue      string
	TaskType       string
	ActivityName   string
	Status         string
	Payload        json.RawMessage
	Attempt        int
	MaxAttempts    int
	LeaseOwner     *string
	LeaseExpiresAt *time.Time
	ScheduledAt    time.Time
}

type Event struct {
	ID        int64
	RunID     uuid.UUID
	EventType string
	Payload   json.RawMessage
	CreatedAt time.Time
}

func (s *Store) CreateRun(ctx context.Context, namespace, workflowName string, input any) (uuid.UUID, error) {
	runID := uuid.New()
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return uuid.Nil, err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO workflow_runs (id, namespace, workflow_name, status, input_json)
		VALUES ($1, $2, $3, 'running', $4::jsonb)
	`, runID, namespace, workflowName, string(inputJSON))
	return runID, err
}

func (s *Store) AppendEvent(ctx context.Context, runID uuid.UUID, eventType string, payload any) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO events (run_id, event_type, payload_json)
		VALUES ($1, $2, $3::jsonb)
	`, runID, eventType, string(payloadJSON))
	return err
}

// CreateTask schedules on the default task queue.
func (s *Store) CreateTask(ctx context.Context, runID uuid.UUID, taskType, activityName string, payload any, maxAttempts int) (uuid.UUID, error) {
	return s.CreateTaskQueued(ctx, runID, QueueDefault, taskType, activityName, payload, maxAttempts)
}

func (s *Store) GetRun(ctx context.Context, namespace string, runID uuid.UUID) (*WorkflowRun, error) {
	var r WorkflowRun
	var completedAt *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT id, namespace, workflow_name, status, input_json, created_at, updated_at, completed_at
		FROM workflow_runs
		WHERE id = $1 AND namespace = $2
	`, runID, namespace).Scan(
		&r.ID, &r.Namespace, &r.WorkflowName, &r.Status, &r.Input,
		&r.CreatedAt, &r.UpdatedAt, &completedAt,
	)
	if err != nil {
		return nil, err
	}
	r.CompletedAt = completedAt
	return &r, nil
}

func (s *Store) ListEvents(ctx context.Context, runID uuid.UUID) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, run_id, event_type, payload_json, created_at
		FROM events
		WHERE run_id = $1
		ORDER BY id
	`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.RunID, &e.EventType, &e.Payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) MarkRunCompleted(ctx context.Context, runID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE workflow_runs
		SET status = 'completed', completed_at = NOW(), updated_at = NOW()
		WHERE id = $1
	`, runID)
	return err
}

func (s *Store) MarkRunFailed(ctx context.Context, runID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE workflow_runs
		SET status = 'failed', completed_at = NOW(), updated_at = NOW()
		WHERE id = $1
	`, runID)
	return err
}

func (s *Store) GetRunByID(ctx context.Context, runID uuid.UUID) (*WorkflowRun, error) {
	var r WorkflowRun
	var completedAt *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT id, namespace, workflow_name, status, input_json, created_at, updated_at, completed_at
		FROM workflow_runs
		WHERE id = $1
	`, runID).Scan(
		&r.ID, &r.Namespace, &r.WorkflowName, &r.Status, &r.Input,
		&r.CreatedAt, &r.UpdatedAt, &completedAt,
	)
	if err != nil {
		return nil, err
	}
	r.CompletedAt = completedAt
	return &r, nil
}

func (s *Store) WithTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	sec := 1 << min(attempt, 6)
	return time.Duration(sec) * time.Second
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func ValidateWorkflow(name string) error {
	switch name {
	case "greet", "delayed_greet", "order_saga":
		return nil
	default:
		return fmt.Errorf("unknown workflow %q", name)
	}
}
