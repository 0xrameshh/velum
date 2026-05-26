package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

type ActivityFunc func(ctx context.Context, payload json.RawMessage) (any, error)

var Registry = map[string]ActivityFunc{
	"greet":      activityGreet,
	"send_email": activitySendEmail,
}

func activityGreet(ctx context.Context, payload json.RawMessage) (any, error) {
	var p struct {
		Input map[string]any `json:"input"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, err
	}
	name, _ := p.Input["name"].(string)
	if name == "" {
		name = "world"
	}
	slog.InfoContext(ctx, "greet activity", "name", name)
	time.Sleep(200 * time.Millisecond)
	return map[string]any{
		"message": fmt.Sprintf("Hello, %s!", name),
	}, nil
}

func activitySendEmail(ctx context.Context, payload json.RawMessage) (any, error) {
	var p struct {
		GreetResult map[string]any `json:"greet_result"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, err
	}
	msg, _ := p.GreetResult["message"].(string)
	slog.InfoContext(ctx, "send_email activity (simulated)", "body", msg)
	time.Sleep(300 * time.Millisecond)
	return map[string]any{
		"sent":    true,
		"subject": "Velum greeting",
		"body":    msg,
	}, nil
}
