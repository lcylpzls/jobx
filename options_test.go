package jobx

import (
	"testing"

	"github.com/lcylpzls/testx"
)

// TestNewDispatcherErrors 覆盖全部配置校验分支。
func TestNewDispatcherErrors(t *testing.T) {
	cases := []struct {
		name string
		opt  Option
	}{
		{"零 worker", WithWorkers(0)},
		{"零队列", WithQueueSize(0)},
		{"非法队列策略", WithQueueFullPolicy(QueueFullPolicy(99))},
		{"零载荷上限", WithMaxPayloadBytes(0)},
		{"空时间源", WithClock(nil)},
	}
	for _, tc := range cases {
		_, err := NewDispatcher(tc.opt)
		testx.RequireErrCode(t, err, CodeInvalidConfig)
	}
}
