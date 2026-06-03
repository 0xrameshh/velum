package config_test

import (
	"testing"

	"github.com/0xrameshh/velum/internal/config"
)

func TestParseMatcherQueues(t *testing.T) {
	if config.ParseMatcherQueues("") != nil {
		t.Fatal("empty should serve all queues")
	}
	q := config.ParseMatcherQueues("default, email ,payments")
	if len(q) != 3 {
		t.Fatalf("expected 3 queues, got %d", len(q))
	}
	if _, ok := q["default"]; !ok {
		t.Fatal("missing default")
	}
	if _, ok := q["email"]; !ok {
		t.Fatal("missing email")
	}
}
