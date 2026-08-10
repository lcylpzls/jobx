package jobx

import (
	"context"
	"errors"
	testx "github.com/lcylpzls/testx"
	"testing"
	"time"
)

// TestObservabilityLogs 覆盖跳过/替换/关闭日志的带日志器路径。
func TestObservabilityLogs(t *testing.T) {
	// 跳过。
	d, err := NewDispatcher(WithWorkers(1), WithLogger(testLogger()))
	testx.RequireNoError(t, err)

	release := make(chan struct{})
	started := make(chan struct{})
	_ = d.Handle("dup", blockHandler(release, started))
	if _, err := d.Submit(context.Background(), "dup", nil); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := d.Submit(context.Background(), "dup", nil); !errors.Is(err, ErrSkipped) {
		t.Fatalf("应跳过：%v", err)
	}
	close(release)
	d.Shutdown(context.Background())

	// 替换 + 关闭。
	d2, err := NewDispatcher(WithWorkers(1), WithLogger(testLogger()),
		WithConflictPolicy(ConflictReplace))
	testx.RequireNoError(t, err)

	_ = d2.Handle("r", func(context.Context, Job) error { return nil })
	if _, err := d2.SubmitAfter(context.Background(), "r", nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := d2.Submit(context.Background(), "r", nil); err != nil {
		t.Fatal(err)
	}
	if err := d2.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
