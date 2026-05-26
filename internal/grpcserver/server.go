package grpcserver

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	velumv1 "github.com/0xrameshh/velum/gen/velum/v1"
	"github.com/0xrameshh/velum/internal/history"
	"github.com/0xrameshh/velum/internal/persistence"
)

type Server struct {
	velumv1.UnimplementedWorkerServiceServer
	store   *persistence.Store
	history history.Client
	lease   time.Duration
}

func New(store *persistence.Store, hist history.Client, lease time.Duration) *Server {
	return &Server{
		store:   store,
		history: hist,
		lease:   lease,
	}
}

func (s *Server) PollTask(ctx context.Context, req *velumv1.PollTaskRequest) (*velumv1.PollTaskResponse, error) {
	if req.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id required")
	}
	lease := s.lease
	if req.GetLeaseSeconds() > 0 {
		lease = time.Duration(req.GetLeaseSeconds()) * time.Second
	}

	task, err := s.store.PollTaskQueue(ctx, req.GetWorkerId(), req.GetTaskQueue(), time.Now().Add(lease))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "poll task: %v", err)
	}
	if task == nil {
		return &velumv1.PollTaskResponse{HasTask: false}, nil
	}
	return &velumv1.PollTaskResponse{
		HasTask: true,
		Task:    toProtoTask(task),
	}, nil
}

func (s *Server) RecordHeartbeat(ctx context.Context, req *velumv1.RecordHeartbeatRequest) (*velumv1.RecordHeartbeatResponse, error) {
	taskID, err := uuid.Parse(req.GetTaskId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid task_id")
	}
	extend := s.lease
	if req.GetExtendLeaseSeconds() > 0 {
		extend = time.Duration(req.GetExtendLeaseSeconds()) * time.Second
	}
	err = s.store.HeartbeatTask(ctx, req.GetWorkerId(), taskID, time.Now().Add(extend))
	if err != nil {
		if errors.Is(err, persistence.ErrTaskNotOwned) {
			return &velumv1.RecordHeartbeatResponse{Ok: false}, nil
		}
		return nil, status.Errorf(codes.Internal, "heartbeat: %v", err)
	}
	return &velumv1.RecordHeartbeatResponse{Ok: true}, nil
}

func (s *Server) CompleteTask(ctx context.Context, req *velumv1.CompleteTaskRequest) (*velumv1.CompleteTaskResponse, error) {
	taskID, err := uuid.Parse(req.GetTaskId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid task_id")
	}
	if req.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key required")
	}

	var result any
	if len(req.GetResultJson()) > 0 {
		if err := json.Unmarshal(req.GetResultJson(), &result); err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid result_json")
		}
	}

	already, err := s.store.CompleteTaskIdempotent(ctx, req.GetWorkerId(), taskID, req.GetIdempotencyKey(), result)
	if err != nil {
		if errors.Is(err, persistence.ErrTaskNotOwned) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "complete: %v", err)
	}
	if already {
		return &velumv1.CompleteTaskResponse{AlreadyApplied: true}, nil
	}

	task, err := s.store.GetTask(ctx, taskID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load task: %v", err)
	}
	if err := s.history.OnActivityCompleted(ctx, task.RunID, task.ActivityName, task.ID, result); err != nil {
		slog.Error("history on complete", "error", err, "task_id", taskID)
		return nil, status.Errorf(codes.Internal, "advance workflow: %v", err)
	}
	return &velumv1.CompleteTaskResponse{AlreadyApplied: false}, nil
}

func (s *Server) FailTask(ctx context.Context, req *velumv1.FailTaskRequest) (*velumv1.FailTaskResponse, error) {
	taskID, err := uuid.Parse(req.GetTaskId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid task_id")
	}
	if req.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key required")
	}

	task, err := s.store.GetTask(ctx, taskID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load task: %v", err)
	}

	var retryAt *time.Time
	if req.GetWillRetry() {
		t := time.Now().Add(persistence.Backoff(task.Attempt))
		retryAt = &t
	}

	already, err := s.store.FailTaskIdempotent(ctx, req.GetWorkerId(), taskID, req.GetIdempotencyKey(), req.GetErrorMessage(), req.GetWillRetry(), retryAt)
	if err != nil {
		if errors.Is(err, persistence.ErrTaskNotOwned) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "fail: %v", err)
	}
	if already {
		return &velumv1.FailTaskResponse{AlreadyApplied: true}, nil
	}

	if err := s.history.OnActivityFailed(ctx, task.RunID, task.ActivityName, task.ID, req.GetErrorMessage(), req.GetWillRetry()); err != nil {
		return nil, status.Errorf(codes.Internal, "history fail: %v", err)
	}
	if !req.GetWillRetry() {
		if err := s.history.HandleTerminalFailure(ctx, task.RunID, task.ActivityName, req.GetErrorMessage()); err != nil {
			return nil, status.Errorf(codes.Internal, "terminal failure: %v", err)
		}
	}
	return &velumv1.FailTaskResponse{AlreadyApplied: false}, nil
}

func toProtoTask(t *persistence.Task) *velumv1.Task {
	return &velumv1.Task{
		Id:           t.ID.String(),
		RunId:        t.RunID.String(),
		TaskType:     t.TaskType,
		ActivityName: t.ActivityName,
		TaskQueue:    t.TaskQueue,
		PayloadJson:  t.Payload,
		Attempt:      int32(t.Attempt),
		MaxAttempts:  int32(t.MaxAttempts),
	}
}

func (s *Server) RunLeaseReclaimer(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := s.store.ReclaimExpiredLeases(ctx)
			if err != nil {
				slog.Error("reclaim leases", "error", err)
				continue
			}
			if n > 0 {
				slog.Info("reclaimed expired task leases", "count", n)
			}
		}
	}
}
