package history

import (
	"testing"
	"time"
)

func TestSleepDurationFromInput(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input map[string]any
		want  time.Duration
	}{
		{"default", map[string]any{"name": "x"}, 5 * time.Second},
		{"float seconds", map[string]any{"sleep_seconds": float64(10)}, 10 * time.Second},
		{"int seconds", map[string]any{"sleep_seconds": 3}, 3 * time.Second},
		{"zero uses default", map[string]any{"sleep_seconds": float64(0)}, 5 * time.Second},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := sleepDurationFromInput(tc.input)
			if got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}
