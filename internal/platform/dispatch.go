package platform

import (
	"github.com/0xrameshh/velum/internal/dispatch"
)

func OpenDispatch(mode, redisAddr string) (dispatch.Notifier, error) {
	return dispatch.NewFromConfig(mode, redisAddr)
}
