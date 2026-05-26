package historyrpc

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	velumv1 "github.com/0xrameshh/velum/gen/velum/v1"
	"github.com/0xrameshh/velum/internal/history"
	"github.com/0xrameshh/velum/internal/persistence"
)

// GRPCClient implements history.Client over HistoryService gRPC.
type GRPCClient struct {
	client velumv1.HistoryServiceClient
}

func NewGRPCClient(conn *grpc.ClientConn) *GRPCClient {
	return &GRPCClient{client: velumv1.NewHistoryServiceClient(conn)}
}

func Dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
}

var _ history.Client = (*GRPCClient)(nil)

func (c *GRPCClient) StartWorkflow(ctx context.Context, namespace, workflowName string, input any) (uuid.UUID, error) {
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return uuid.Nil, err
	}
	resp, err := c.client.StartWorkflow(ctx, &velumv1.StartWorkflowRequest{
		Namespace:    namespace,
		WorkflowName: workflowName,
		InputJson:    inputJSON,
	})
	if err != nil {
		return uuid.Nil, err
	}
	return uuid.Parse(resp.GetRunId())
}

func (c *GRPCClient) GetRun(ctx context.Context, namespace string, runID uuid.UUID) (*persistence.WorkflowRun, []persistence.Event, error) {
	resp, err := c.client.GetRun(ctx, &velumv1.GetRunRequest{
		Namespace: namespace,
		RunId:     runID.String(),
	})
	if err != nil {
		return nil, nil, err
	}
	return fromProtoRun(resp.GetRun()), fromProtoEvents(runID, resp.GetEvents()), nil
}

func (c *GRPCClient) OnActivityCompleted(ctx context.Context, runID uuid.UUID, activityName string, taskID uuid.UUID, result any) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = c.client.OnActivityCompleted(ctx, &velumv1.OnActivityCompletedRequest{
		RunId:        runID.String(),
		ActivityName: activityName,
		TaskId:       taskID.String(),
		ResultJson:   resultJSON,
	})
	return err
}

func (c *GRPCClient) OnActivityFailed(ctx context.Context, runID uuid.UUID, activityName string, taskID uuid.UUID, errMsg string, willRetry bool) error {
	_, err := c.client.OnActivityFailed(ctx, &velumv1.OnActivityFailedRequest{
		RunId:        runID.String(),
		ActivityName: activityName,
		TaskId:       taskID.String(),
		ErrorMessage: errMsg,
		WillRetry:    willRetry,
	})
	return err
}

func (c *GRPCClient) HandleTerminalFailure(ctx context.Context, runID uuid.UUID, activityName, errMsg string) error {
	_, err := c.client.HandleTerminalFailure(ctx, &velumv1.HandleTerminalFailureRequest{
		RunId:        runID.String(),
		ActivityName: activityName,
		ErrorMessage: errMsg,
	})
	return err
}

func (c *GRPCClient) OnTimerFired(ctx context.Context, runID, timerID uuid.UUID, timerName string, payload map[string]any) error {
	var payloadJSON []byte
	if payload != nil {
		var err error
		payloadJSON, err = json.Marshal(payload)
		if err != nil {
			return err
		}
	}
	_, err := c.client.OnTimerFired(ctx, &velumv1.OnTimerFiredRequest{
		RunId:       runID.String(),
		TimerId:     timerID.String(),
		TimerName:   timerName,
		PayloadJson: payloadJSON,
	})
	return err
}

func fromProtoRun(r *velumv1.WorkflowRun) *persistence.WorkflowRun {
	if r == nil {
		return nil
	}
	id, _ := uuid.Parse(r.GetId())
	out := &persistence.WorkflowRun{
		ID:           id,
		Namespace:    r.GetNamespace(),
		WorkflowName: r.GetWorkflowName(),
		Status:       r.GetStatus(),
		Input:        r.GetInputJson(),
		CreatedAt:    time.Unix(0, r.GetCreatedAtUnixNano()).UTC(),
		UpdatedAt:    time.Unix(0, r.GetUpdatedAtUnixNano()).UTC(),
	}
	if r.CompletedAtUnixNano != nil {
		t := time.Unix(0, r.GetCompletedAtUnixNano()).UTC()
		out.CompletedAt = &t
	}
	return out
}

func fromProtoEvents(runID uuid.UUID, events []*velumv1.Event) []persistence.Event {
	out := make([]persistence.Event, 0, len(events))
	for _, e := range events {
		out = append(out, persistence.Event{
			ID:        e.GetId(),
			RunID:     runID,
			EventType: e.GetEventType(),
			Payload:   e.GetPayloadJson(),
			CreatedAt: time.Unix(0, e.GetCreatedAtUnixNano()).UTC(),
		})
	}
	return out
}
