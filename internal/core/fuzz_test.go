package core

import (
	"context"
	testx "github.com/lcylpzls/testx"
	"strings"
	"testing"
)

// FuzzSubmit 模糊测试任务提交路径，确保任意输入不 panic。
func FuzzSubmit(f *testing.F) {
	f.Add("task", []byte("payload"))
	f.Add("", []byte{})
	f.Add(strings.Repeat("x", 200), make([]byte, 4096))
	f.Fuzz(func(t *testing.T, name string, payload []byte) {
		if len(name) > 300 || len(payload) > 8192 {
			t.Skip("输入过大")
		}
		d, err := NewDispatcher(WithWorkers(1), WithQueueSize(8))
		testx.RequireNoError(t, err)

		defer d.Shutdown(context.Background())
		_ = d.Handle("task", func(context.Context, Job) error { return nil })
		_, _ = d.Submit(context.Background(), name, payload)
	})
}
