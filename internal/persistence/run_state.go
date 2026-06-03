package persistence

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Store) GetRunState(ctx context.Context, runID uuid.UUID, dest any) error {
	var raw json.RawMessage
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(state_json, '{}'::jsonb) FROM workflow_runs WHERE id = $1
	`, runID).Scan(&raw)
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	return json.Unmarshal(raw, dest)
}

func (s *Store) SetRunState(ctx context.Context, runID uuid.UUID, state any) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		UPDATE workflow_runs SET state_json = $2::jsonb, updated_at = NOW() WHERE id = $1
	`, runID, string(raw))
	return err
}

// AtomicUpdateRunState reads, modifies, and writes the run state in a single
// transaction with row-level locking (FOR UPDATE). This prevents race conditions
// when concurrent operations (e.g., parallel saga branches completing) try to
// read-modify-write the same state simultaneously.
//
// The fn callback receives a pointer to dest with the current state already
// loaded. Any changes made to dest are automatically persisted after fn returns.
func (s *Store) AtomicUpdateRunState(ctx context.Context, runID uuid.UUID, dest any, fn func() error) error {
	return s.WithTx(ctx, func(tx pgx.Tx) error {
		// Lock the row so concurrent updates serialize
		var raw json.RawMessage
		err := tx.QueryRow(ctx, `
			SELECT COALESCE(state_json, '{}'::jsonb) FROM workflow_runs WHERE id = $1 FOR UPDATE
		`, runID).Scan(&raw)
		if err != nil {
			return err
		}
		if len(raw) == 0 {
			raw = json.RawMessage(`{}`)
		}
		if err := json.Unmarshal(raw, dest); err != nil {
			return err
		}

		// Let the caller modify dest
		if err := fn(); err != nil {
			return err
		}

		// Write back, still inside the transaction
		raw, err = json.Marshal(dest)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			UPDATE workflow_runs SET state_json = $2::jsonb, updated_at = NOW() WHERE id = $1
		`, runID, string(raw))
		return err
	})
}
