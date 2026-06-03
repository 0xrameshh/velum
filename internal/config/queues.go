package config

import (
	"strings"

	"github.com/0xrameshh/velum/internal/persistence"
)

// ParseMatcherQueues returns nil when all queues are served.
func ParseMatcherQueues(raw string) map[string]struct{} {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	out := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		q := persistence.NormalizeQueue(strings.TrimSpace(part))
		if q != "" {
			out[q] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
