package dispatch

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Noop struct{}

func NewNoop() *Noop { return &Noop{} }

func (n *Noop) NotifyTask(_ context.Context, _ string, _ uuid.UUID) error { return nil }
func (n *Noop) NotifyTimer(_ context.Context, _ uuid.UUID, _ time.Time) error {
	return nil
}
func (n *Noop) WaitTask(_ context.Context, _ string, timeout time.Duration) error {
	if timeout > 0 {
		time.Sleep(timeout)
	}
	return nil
}
func (n *Noop) WaitTimer(_ context.Context, timeout time.Duration) error {
	if timeout > 0 {
		time.Sleep(timeout)
	}
	return nil
}
func (n *Noop) Close() error { return nil }
