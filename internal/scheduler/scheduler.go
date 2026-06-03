package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/0xrameshh/velum/internal/dispatch"
	"github.com/0xrameshh/velum/internal/history"
	"github.com/0xrameshh/velum/internal/persistence"
)

type Scheduler struct {
	store           *persistence.Store
	history         history.Client
	every           time.Duration
	batch           int
	dispatch        dispatch.Notifier
	dispatchEnabled bool
}

func New(store *persistence.Store, hist history.Client, every time.Duration, batch int, d dispatch.Notifier, dispatchEnabled bool) *Scheduler {
	if batch < 1 {
		batch = 50
	}
	if d == nil {
		d = dispatch.NewNoop()
	}
	return &Scheduler{
		store:           store,
		history:         hist,
		every:           every,
		batch:           batch,
		dispatch:        d,
		dispatchEnabled: dispatchEnabled,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	slog.Info("timer scheduler started", "poll_every", s.every.String(), "redis_wake", s.dispatchEnabled)
	for {
		if err := s.tick(ctx); err != nil {
			slog.Error("scheduler tick", "error", err)
		}
		select {
		case <-ctx.Done():
			slog.Info("timer scheduler stopped")
			return ctx.Err()
		default:
		}
		if s.dispatchEnabled {
			if err := s.dispatch.WaitTimer(ctx, s.every); err != nil {
				return err
			}
			continue
		}
		select {
		case <-ctx.Done():
			slog.Info("timer scheduler stopped")
			return ctx.Err()
		case <-time.After(s.every):
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) error {
	timers, err := s.store.FireDueTimers(ctx, s.batch)
	if err != nil {
		return err
	}
	for _, t := range timers {
		var payload map[string]any
		if len(t.Payload) > 0 {
			_ = json.Unmarshal(t.Payload, &payload)
		}
		if err := s.history.OnTimerFired(ctx, t.RunID, t.ID, t.TimerName, payload); err != nil {
			slog.Error("timer fired handler", "timer_id", t.ID, "run_id", t.RunID, "error", err)
		}
	}
	if len(timers) > 0 {
		slog.Info("fired timers", "count", len(timers))
	}
	return nil
}
