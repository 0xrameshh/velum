package dispatch_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"

	"github.com/0xrameshh/velum/internal/dispatch"
)

func TestRedisTaskWake(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	n, err := dispatch.NewRedis(mr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	ctx := context.Background()
	taskID := uuid.New()

	done := make(chan struct{})
	go func() {
		_ = n.WaitTask(ctx, "default", 2*time.Second)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	if err := n.NotifyTask(ctx, "default", taskID); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("WaitTask did not wake")
	}
}

func TestRedisTimerWake(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	n, err := dispatch.NewRedis(mr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	ctx := context.Background()
	timerID := uuid.New()
	fireAt := time.Now().UTC().Add(5 * time.Second)

	done := make(chan struct{})
	go func() {
		_ = n.WaitTimer(ctx, 2*time.Second)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	if err := n.NotifyTimer(ctx, timerID, fireAt); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("WaitTimer did not wake")
	}
}

func TestFactoryPostgresMode(t *testing.T) {
	n, err := dispatch.NewFromConfig("postgres", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
}
