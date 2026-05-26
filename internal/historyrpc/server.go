package historyrpc

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	velumv1 "github.com/0xrameshh/velum/gen/velum/v1"
	"github.com/0xrameshh/velum/internal/history"
	"github.com/0xrameshh/velum/internal/persistence"
)

type Server struct {
	velumv1.UnimplementedHistoryServiceServer
	svc *history.Service
}

func NewServer(svc *history.Service) *Server {
	return &Server{svc: svc}
}

func (s *Server) StartWorkflow(ctx context.Context, req *velumv1.StartWorkflowRequest) (*velumv1.StartWorkflowResponse, error) {
	var input any
	if len(req.GetInputJson()) > 0 {
		if err := json.Unmarshal(req.GetInputJson(), &input); err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid input_json")
		}
	} else {
		input = map[string]any{}
	}
	runID, err := s.svc.StartWorkflow(ctx, req.GetNamespace(), req.GetWorkflowName(), input)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &velumv1.StartWorkflowResponse{RunId: runID.String()}, nil
}

func (s *Server) GetRun(ctx context.Context, req *velumv1.GetRunRequest) (*velumv1.GetRunResponse, error) {
	runID, err := uuid.Parse(req.GetRunId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid run_id")
	}
	run, events, err := s.svc.GetRun(ctx, req.GetNamespace(), runID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	return &velumv1.GetRunResponse{
		Run:    toProtoRun(run),
		Events: toProtoEvents(events),
	}, nil
}

func (s *Server) OnActivityCompleted(ctx context.Context, req *velumv1.OnActivityCompletedRequest) (*velumv1.OnActivityCompletedResponse, error) {
	runID, taskID, err := parseRunAndTask(req.GetRunId(), req.GetTaskId())
	if err != nil {
		return nil, err
	}
	var result any
	if len(req.GetResultJson()) > 0 {
		if err := json.Unmarshal(req.GetResultJson(), &result); err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid result_json")
		}
	}
	if err := s.svc.OnActivityCompleted(ctx, runID, req.GetActivityName(), taskID, result); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &velumv1.OnActivityCompletedResponse{}, nil
}

func (s *Server) OnActivityFailed(ctx context.Context, req *velumv1.OnActivityFailedRequest) (*velumv1.OnActivityFailedResponse, error) {
	runID, taskID, err := parseRunAndTask(req.GetRunId(), req.GetTaskId())
	if err != nil {
		return nil, err
	}
	if err := s.svc.OnActivityFailed(ctx, runID, req.GetActivityName(), taskID, req.GetErrorMessage(), req.GetWillRetry()); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &velumv1.OnActivityFailedResponse{}, nil
}

func (s *Server) HandleTerminalFailure(ctx context.Context, req *velumv1.HandleTerminalFailureRequest) (*velumv1.HandleTerminalFailureResponse, error) {
	runID, err := uuid.Parse(req.GetRunId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid run_id")
	}
	if err := s.svc.HandleTerminalFailure(ctx, runID, req.GetActivityName(), req.GetErrorMessage()); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &velumv1.HandleTerminalFailureResponse{}, nil
}

func (s *Server) OnTimerFired(ctx context.Context, req *velumv1.OnTimerFiredRequest) (*velumv1.OnTimerFiredResponse, error) {
	runID, err := uuid.Parse(req.GetRunId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid run_id")
	}
	timerID, err := uuid.Parse(req.GetTimerId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid timer_id")
	}
	var payload map[string]any
	if len(req.GetPayloadJson()) > 0 {
		if err := json.Unmarshal(req.GetPayloadJson(), &payload); err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid payload_json")
		}
	}
	if err := s.svc.OnTimerFired(ctx, runID, timerID, req.GetTimerName(), payload); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &velumv1.OnTimerFiredResponse{}, nil
}

func parseRunAndTask(runIDStr, taskIDStr string) (uuid.UUID, uuid.UUID, error) {
	runID, err := uuid.Parse(runIDStr)
	if err != nil {
		return uuid.Nil, uuid.Nil, status.Error(codes.InvalidArgument, "invalid run_id")
	}
	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		return uuid.Nil, uuid.Nil, status.Error(codes.InvalidArgument, "invalid task_id")
	}
	return runID, taskID, nil
}

func toProtoRun(r *persistence.WorkflowRun) *velumv1.WorkflowRun {
	out := &velumv1.WorkflowRun{
		Id:                 r.ID.String(),
		Namespace:          r.Namespace,
		WorkflowName:       r.WorkflowName,
		Status:             r.Status,
		InputJson:          r.Input,
		CreatedAtUnixNano:  r.CreatedAt.UnixNano(),
		UpdatedAtUnixNano:  r.UpdatedAt.UnixNano(),
	}
	if r.CompletedAt != nil {
		n := r.CompletedAt.UnixNano()
		out.CompletedAtUnixNano = &n
	}
	return out
}

func toProtoEvents(events []persistence.Event) []*velumv1.Event {
	out := make([]*velumv1.Event, 0, len(events))
	for _, e := range events {
		out = append(out, &velumv1.Event{
			Id:                e.ID,
			EventType:         e.EventType,
			PayloadJson:       e.Payload,
			CreatedAtUnixNano: e.CreatedAt.UnixNano(),
		})
	}
	return out
}
