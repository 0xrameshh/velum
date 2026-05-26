package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/google/uuid"

	velumv1 "github.com/0xrameshh/velum/gen/velum/v1"
	"github.com/0xrameshh/velum/internal/persistence"
	"github.com/0xrameshh/velum/internal/workflow"
)

type GRPCRunner struct {
	client    velumv1.WorkerServiceClient
	workerID  string
	taskQueue string
	lease     time.Duration
	pollEvery time.Duration
}

func NewGRPCRunner(conn *grpc.ClientConn, workerID, taskQueue string, lease, pollEvery time.Duration) *GRPCRunner {
	return &GRPCRunner{
		client:    velumv1.NewWorkerServiceClient(conn),
		workerID:  workerID,
		taskQueue: persistence.NormalizeQueue(taskQueue),
		lease:     lease,
		pollEvery: pollEvery,
	}
}

func Dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
}

func (r *GRPCRunner) Run(ctx context.Context) error {
	slog.Info("grpc worker started",
		"worker_id", r.workerID,
		"task_queue", r.taskQueue,
	)
	ticker := time.NewTicker(r.pollEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("grpc worker stopped", "worker_id", r.workerID)
			return ctx.Err()
		case <-ticker.C:
			if err := r.pollOnce(ctx); err != nil {
				slog.Error("grpc worker poll", "error", err)
			}
		}
	}
}

func (r *GRPCRunner) pollOnce(ctx context.Context) error {
	resp, err := r.client.PollTask(ctx, &velumv1.PollTaskRequest{
		WorkerId:      r.workerID,
		TaskQueue:     r.taskQueue,
		LeaseSeconds:  int64(r.lease.Seconds()),
	})
	if err != nil {
		return err
	}
	if !resp.GetHasTask() {
		return nil
	}
	return r.execute(ctx, resp.GetTask())
}

func (r *GRPCRunner) execute(ctx context.Context, pt *velumv1.Task) error {
	task := protoToTask(pt)
	hbCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go r.heartbeatLoop(hbCtx, pt.GetId())

	activity, ok := workflow.Registry[task.ActivityName]
	if !ok {
		err := fmt.Errorf("unknown activity %q", task.ActivityName)
		willRetry := task.Attempt < task.MaxAttempts
		_, ferr := r.client.FailTask(ctx, &velumv1.FailTaskRequest{
			WorkerId:       r.workerID,
			TaskId:         pt.GetId(),
			IdempotencyKey: persistence.IdempotencyKey(task.ID, task.Attempt, "fail"),
			ErrorMessage:   err.Error(),
			WillRetry:      willRetry,
		})
		return ferr
	}
	result, err := activity(ctx, task.Payload)
	cancel()

	completeKey := persistence.IdempotencyKey(task.ID, task.Attempt, "complete")
	failKey := persistence.IdempotencyKey(task.ID, task.Attempt, "fail")

	if err != nil {
		willRetry := task.Attempt < task.MaxAttempts
		_, err := r.client.FailTask(ctx, &velumv1.FailTaskRequest{
			WorkerId:       r.workerID,
			TaskId:         pt.GetId(),
			IdempotencyKey: failKey,
			ErrorMessage:   err.Error(),
			WillRetry:      willRetry,
		})
		return err
	}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}
	_, err = r.client.CompleteTask(ctx, &velumv1.CompleteTaskRequest{
		WorkerId:       r.workerID,
		TaskId:         pt.GetId(),
		IdempotencyKey: completeKey,
		ResultJson:     resultJSON,
	})
	return err
}

func (r *GRPCRunner) heartbeatLoop(ctx context.Context, taskID string) {
	ticker := time.NewTicker(r.lease / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := r.client.RecordHeartbeat(ctx, &velumv1.RecordHeartbeatRequest{
				WorkerId:            r.workerID,
				TaskId:              taskID,
				ExtendLeaseSeconds:  int64(r.lease.Seconds()),
			})
			if err != nil {
				slog.Warn("heartbeat failed", "task_id", taskID, "error", err)
			}
		}
	}
}

func protoToTask(pt *velumv1.Task) *persistence.Task {
	id, _ := uuid.Parse(pt.GetId())
	runID, _ := uuid.Parse(pt.GetRunId())
	return &persistence.Task{
		ID:           id,
		RunID:        runID,
		TaskQueue:    pt.GetTaskQueue(),
		TaskType:     pt.GetTaskType(),
		ActivityName: pt.GetActivityName(),
		Payload:      pt.GetPayloadJson(),
		Attempt:      int(pt.GetAttempt()),
		MaxAttempts:  int(pt.GetMaxAttempts()),
	}
}
