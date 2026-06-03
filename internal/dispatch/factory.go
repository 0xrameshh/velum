package dispatch

import (
	"fmt"
	"strings"
)

func NewFromConfig(mode, redisAddr string) (Notifier, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "postgres", "none", "noop":
		return NewNoop(), nil
	case "redis":
		if redisAddr == "" {
			return nil, fmt.Errorf("VELUM_REDIS_ADDR is required when VELUM_DISPATCH=redis")
		}
		return NewRedis(redisAddr)
	default:
		return nil, fmt.Errorf("unknown VELUM_DISPATCH %q (use postgres or redis)", mode)
	}
}
