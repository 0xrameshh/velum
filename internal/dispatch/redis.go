package dispatch

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Redis struct {
	client *redis.Client
}

func NewRedis(addr string) (*Redis, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Redis{client: client}, nil
}

func (r *Redis) NotifyTask(ctx context.Context, queue string, taskID uuid.UUID) error {
	return r.client.LPush(ctx, TaskChannel(queue), taskID.String()).Err()
}

func (r *Redis) NotifyTimer(ctx context.Context, timerID uuid.UUID, fireAt time.Time) error {
	pipe := r.client.Pipeline()
	pipe.ZAdd(ctx, "velum:timers", redis.Z{
		Score:  float64(fireAt.UnixNano()),
		Member: timerID.String(),
	})
	pipe.Publish(ctx, TimerWakeChannel, timerID.String())
	_, err := pipe.Exec(ctx)
	return err
}

func (r *Redis) WaitTask(ctx context.Context, queue string, timeout time.Duration) error {
	if timeout <= 0 {
		return nil
	}
	_, err := r.client.BRPop(ctx, timeout, TaskChannel(queue)).Result()
	if err == redis.Nil {
		return nil
	}
	return err
}

func (r *Redis) WaitTimer(ctx context.Context, timeout time.Duration) error {
	if timeout <= 0 {
		return nil
	}
	sub := r.client.Subscribe(ctx, TimerWakeChannel)
	defer sub.Close()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(timeout):
		return nil
	case <-sub.Channel():
		return nil
	}
}

func (r *Redis) Close() error {
	if r.client == nil {
		return nil
	}
	return r.client.Close()
}
