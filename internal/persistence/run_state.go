package persistence

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
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
