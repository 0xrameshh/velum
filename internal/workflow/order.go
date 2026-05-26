package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

func init() {
	Registry["charge_card"] = activityChargeCard
	Registry["reserve_stock"] = activityReserveStock
	Registry["ship_order"] = activityShipOrder
	Registry["refund_payment"] = activityRefundPayment
	Registry["release_stock"] = activityReleaseStock
}

func orderPayload(payload json.RawMessage) (orderID string, input map[string]any, err error) {
	var p struct {
		OrderID string         `json:"order_id"`
		Input   map[string]any `json:"input"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return "", nil, err
	}
	if p.Input == nil {
		p.Input = map[string]any{}
	}
	return p.OrderID, p.Input, nil
}

func inputBool(input map[string]any, key string) bool {
	v, _ := input[key].(bool)
	return v
}

func activityChargeCard(ctx context.Context, payload json.RawMessage) (any, error) {
	orderID, input, err := orderPayload(payload)
	if err != nil {
		return nil, err
	}
	if inputBool(input, "fail_charge") {
		return nil, errors.New("charge declined (simulated)")
	}
	slog.InfoContext(ctx, "charge_card", "order_id", orderID)
	time.Sleep(150 * time.Millisecond)
	return map[string]any{
		"charged":    true,
		"amount":     99.99,
		"payment_id": fmt.Sprintf("pay-%s", orderID),
	}, nil
}

func activityReserveStock(ctx context.Context, payload json.RawMessage) (any, error) {
	orderID, input, err := orderPayload(payload)
	if err != nil {
		return nil, err
	}
	if inputBool(input, "fail_reserve") {
		return nil, errors.New("inventory unavailable (simulated)")
	}
	slog.InfoContext(ctx, "reserve_stock", "order_id", orderID)
	time.Sleep(150 * time.Millisecond)
	return map[string]any{
		"reserved": true,
		"sku":      "SKU-42",
		"units":    1,
	}, nil
}

func activityShipOrder(ctx context.Context, payload json.RawMessage) (any, error) {
	orderID, input, err := orderPayload(payload)
	if err != nil {
		return nil, err
	}
	if inputBool(input, "fail_ship") {
		return nil, errors.New("carrier rejected shipment (simulated)")
	}
	slog.InfoContext(ctx, "ship_order", "order_id", orderID)
	time.Sleep(200 * time.Millisecond)
	return map[string]any{
		"shipped":     true,
		"tracking_id": fmt.Sprintf("TRK-%s", orderID),
	}, nil
}

func activityRefundPayment(ctx context.Context, payload json.RawMessage) (any, error) {
	orderID, _, err := orderPayload(payload)
	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "refund_payment (compensation)", "order_id", orderID)
	time.Sleep(100 * time.Millisecond)
	return map[string]any{"refunded": true}, nil
}

func activityReleaseStock(ctx context.Context, payload json.RawMessage) (any, error) {
	orderID, _, err := orderPayload(payload)
	if err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "release_stock (compensation)", "order_id", orderID)
	time.Sleep(100 * time.Millisecond)
	return map[string]any{"released": true}, nil
}
