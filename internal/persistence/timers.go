package persistence

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const (
	TimerStatusPending = "pending"
	TimerStatusFired   = "fired"
)

type Timer struct {
	ID        uuid.UUID
	RunID     uuid.UUID
	TimerName string
	FireAt    time.Time
	Status    string
	Payload   json.RawMessage
	CreatedAt time.Time
	FiredAt   *time.Time
}

func (s *Store) CreateTimer(ctx context.Context, runID uuid.UUID, timerName string, fireAt time.Time, payload any) (uuid.UUID, error) {
	timerID := uuid.New()
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return uuid.Nil, err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO timers (id, run_id, timer_name, fire_at, status, payload_json)
		VALUES ($1, $2, $3, $4, 'pending', $5::jsonb)
	`, timerID, runID, timerName, fireAt, string(payloadJSON))
	return timerID, err
}

// FireDueTimers atomically marks due timers as fired and returns them.
func (s *Store) FireDueTimers(ctx context.Context, limit int) ([]Timer, error) {
	if limit < 1 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		UPDATE timers
		SET status = 'fired',
		    fired_at = NOW()
		WHERE id IN (
			SELECT id FROM timers
			WHERE status = 'pending'
			  AND fire_at <= NOW()
			ORDER BY fire_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		RETURNING id, run_id, timer_name, fire_at, status, payload_json, created_at, fired_at
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Timer
	for rows.Next() {
		var t Timer
		var firedAt *time.Time
		if err := rows.Scan(
			&t.ID, &t.RunID, &t.TimerName, &t.FireAt, &t.Status,
			&t.Payload, &t.CreatedAt, &firedAt,
		); err != nil {
			return nil, err
		}
		t.FiredAt = firedAt
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) ListPendingTimers(ctx context.Context, runID uuid.UUID) ([]Timer, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, run_id, timer_name, fire_at, status, payload_json, created_at, fired_at
		FROM timers
		WHERE run_id = $1 AND status = 'pending'
		ORDER BY fire_at
	`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Timer
	for rows.Next() {
		var t Timer
		var firedAt *time.Time
		if err := rows.Scan(
			&t.ID, &t.RunID, &t.TimerName, &t.FireAt, &t.Status,
			&t.Payload, &t.CreatedAt, &firedAt,
		); err != nil {
			return nil, err
		}
		t.FiredAt = firedAt
		out = append(out, t)
	}
	return out, rows.Err()
}
