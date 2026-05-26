package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	ErrTaskNotFound      = errors.New("task not found")
	ErrTaskNotOwned      = errors.New("task not leased to worker")
	ErrInvalidTaskState  = errors.New("invalid task state")
)

func (s *Store) ReclaimExpiredLeases(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE tasks
		SET status = 'pending',
		    lease_owner = NULL,
		    lease_expires_at = NULL,
		    updated_at = NOW()
		WHERE status = 'leased'
		  AND lease_expires_at IS NOT NULL
		  AND lease_expires_at < NOW()
	`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) CreateTaskQueued(ctx context.Context, runID uuid.UUID, taskQueue, taskType, activityName string, payload any, maxAttempts int) (uuid.UUID, error) {
	taskID := uuid.New()
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return uuid.Nil, err
	}
	if maxAttempts < 1 {
		maxAttempts = 5
	}
	queue := NormalizeQueue(taskQueue)
	_, err = s.pool.Exec(ctx, `
		INSERT INTO tasks (id, run_id, task_queue, task_type, activity_name, status, payload_json, max_attempts)
		VALUES ($1, $2, $3, $4, $5, 'pending', $6::jsonb, $7)
	`, taskID, runID, queue, taskType, activityName, string(payloadJSON), maxAttempts)
	return taskID, err
}

func (s *Store) PollTaskQueue(ctx context.Context, workerID, taskQueue string, leaseUntil time.Time) (*Task, error) {
	if _, err := s.ReclaimExpiredLeases(ctx); err != nil {
		return nil, err
	}

	queue := NormalizeQueue(taskQueue)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var t Task
	var activityName *string
	err = tx.QueryRow(ctx, `
		SELECT id, run_id, task_type, activity_name, status, payload_json, attempt, max_attempts, scheduled_at
		FROM tasks
		WHERE status = 'pending'
		  AND task_queue = $1
		  AND scheduled_at <= NOW()
		ORDER BY scheduled_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`, queue).Scan(&t.ID, &t.RunID, &t.TaskType, &activityName, &t.Status, &t.Payload, &t.Attempt, &t.MaxAttempts, &t.ScheduledAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if activityName != nil {
		t.ActivityName = *activityName
	}
	t.TaskQueue = queue

	_, err = tx.Exec(ctx, `
		UPDATE tasks
		SET status = 'leased',
		    lease_owner = $2,
		    lease_expires_at = $3,
		    started_at = COALESCE(started_at, NOW()),
		    updated_at = NOW()
		WHERE id = $1
	`, t.ID, workerID, leaseUntil)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	owner := workerID
	t.LeaseOwner = &owner
	exp := leaseUntil
	t.LeaseExpiresAt = &exp
	t.Status = "leased"
	return &t, nil
}

func (s *Store) HeartbeatTask(ctx context.Context, workerID string, taskID uuid.UUID, leaseUntil time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE tasks
		SET lease_expires_at = $3,
		    updated_at = NOW()
		WHERE id = $1
		  AND lease_owner = $2
		  AND status = 'leased'
	`, taskID, workerID, leaseUntil)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrTaskNotOwned
	}
	return nil
}

func (s *Store) GetTask(ctx context.Context, taskID uuid.UUID) (*Task, error) {
	var t Task
	var activityName *string
	var queue string
	err := s.pool.QueryRow(ctx, `
		SELECT id, run_id, task_queue, task_type, activity_name, status, payload_json, attempt, max_attempts, scheduled_at
		FROM tasks
		WHERE id = $1
	`, taskID).Scan(
		&t.ID, &t.RunID, &queue, &t.TaskType, &activityName, &t.Status,
		&t.Payload, &t.Attempt, &t.MaxAttempts, &t.ScheduledAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, ErrTaskNotFound
		}
		return nil, err
	}
	t.TaskQueue = queue
	if activityName != nil {
		t.ActivityName = *activityName
	}
	return &t, nil
}

func (s *Store) CompleteTaskIdempotent(ctx context.Context, workerID string, taskID uuid.UUID, idempotencyKey string, result any) (alreadyApplied bool, err error) {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return false, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var exists bool
	err = tx.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM task_completions WHERE task_id = $1 AND idempotency_key = $2
		)
	`, taskID, idempotencyKey).Scan(&exists)
	if err != nil {
		return false, err
	}
	if exists {
		return true, tx.Commit(ctx)
	}

	var status string
	var owner *string
	err = tx.QueryRow(ctx, `
		SELECT status, lease_owner FROM tasks WHERE id = $1 FOR UPDATE
	`, taskID).Scan(&status, &owner)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, ErrTaskNotFound
		}
		return false, err
	}
	if status == "completed" {
		return true, tx.Commit(ctx)
	}
	if status != "leased" || owner == nil || *owner != workerID {
		return false, ErrTaskNotOwned
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO task_completions (task_id, idempotency_key, result_json)
		VALUES ($1, $2, $3::jsonb)
	`, taskID, idempotencyKey, string(resultJSON))
	if err != nil {
		return false, err
	}

	_, err = tx.Exec(ctx, `
		UPDATE tasks
		SET status = 'completed',
		    completed_at = NOW(),
		    lease_owner = NULL,
		    lease_expires_at = NULL,
		    updated_at = NOW()
		WHERE id = $1
	`, taskID)
	if err != nil {
		return false, err
	}

	return false, tx.Commit(ctx)
}

func (s *Store) GetCompletionResult(ctx context.Context, taskID uuid.UUID, idempotencyKey string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := s.pool.QueryRow(ctx, `
		SELECT result_json FROM task_completions
		WHERE task_id = $1 AND idempotency_key = $2
	`, taskID, idempotencyKey).Scan(&raw)
	return raw, err
}

func (s *Store) FailTaskIdempotent(ctx context.Context, workerID string, taskID uuid.UUID, idempotencyKey, errMsg string, willRetry bool, retryAt *time.Time) (alreadyApplied bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var exists bool
	err = tx.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM task_failures WHERE task_id = $1 AND idempotency_key = $2
		)
	`, taskID, idempotencyKey).Scan(&exists)
	if err != nil {
		return false, err
	}
	if exists {
		return true, tx.Commit(ctx)
	}

	var status string
	var owner *string
	err = tx.QueryRow(ctx, `
		SELECT status, lease_owner FROM tasks WHERE id = $1 FOR UPDATE
	`, taskID).Scan(&status, &owner)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, ErrTaskNotFound
		}
		return false, err
	}
	if status == "failed" || (status == "pending" && !willRetry) {
		return true, tx.Commit(ctx)
	}
	if status != "leased" || owner == nil || *owner != workerID {
		return false, ErrTaskNotOwned
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO task_failures (task_id, idempotency_key, error_message, will_retry)
		VALUES ($1, $2, $3, $4)
	`, taskID, idempotencyKey, errMsg, willRetry)
	if err != nil {
		return false, err
	}

	if willRetry && retryAt != nil {
		_, err = tx.Exec(ctx, `
			UPDATE tasks
			SET status = 'pending',
			    attempt = attempt + 1,
			    last_error = $2,
			    scheduled_at = $3,
			    lease_owner = NULL,
			    lease_expires_at = NULL,
			    updated_at = NOW()
			WHERE id = $1
		`, taskID, errMsg, *retryAt)
	} else {
		_, err = tx.Exec(ctx, `
			UPDATE tasks
			SET status = 'failed',
			    last_error = $2,
			    completed_at = NOW(),
			    lease_owner = NULL,
			    lease_expires_at = NULL,
			    updated_at = NOW()
			WHERE id = $1
		`, taskID, errMsg)
	}
	if err != nil {
		return false, err
	}

	return false, tx.Commit(ctx)
}

func IdempotencyKey(taskID uuid.UUID, attempt int, suffix string) string {
	return fmt.Sprintf("%s:%d:%s", taskID.String(), attempt, suffix)
}
