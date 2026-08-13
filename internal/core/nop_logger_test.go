package core

import (
	"context"
	"errors"
	"testing"

	"github.com/lcylpzls/logx"
	"github.com/lcylpzls/testx"
)

// panicLogger 是指针接收者实现的假日志器：任何方法被调用都会因 nil
// 接收者解引用而 panic，用于验证类型化 nil 已被归一为 no-op。
type panicLogger struct{ n int }

func (p *panicLogger) IsDebugEnabled() bool                        { _ = p.n; return false }
func (p *panicLogger) Debug(msg string, fields logx.FieldGroup)    { _ = p.n }
func (p *panicLogger) Info(msg string, fields logx.FieldGroup)     { _ = p.n }
func (p *panicLogger) Warn(msg string, fields logx.FieldGroup)     { _ = p.n }
func (p *panicLogger) Error(msg string, fields logx.FieldGroup)    { _ = p.n }
func (p *panicLogger) Panic(msg string, fields logx.FieldGroup)    { _ = p.n }
func (p *panicLogger) Fatal(msg string, fields logx.FieldGroup)    { _ = p.n }
func (p *panicLogger) Debugf(format string, args ...any)           { _ = p.n }
func (p *panicLogger) Infof(format string, args ...any)            { _ = p.n }
func (p *panicLogger) Warnf(format string, args ...any)            { _ = p.n }
func (p *panicLogger) Errorf(format string, args ...any)           { _ = p.n }
func (p *panicLogger) Panicf(format string, args ...any)           { _ = p.n }
func (p *panicLogger) Fatalf(format string, args ...any)           { _ = p.n }
func (p *panicLogger) WithContext(ctx context.Context) logx.Logger { _ = p.n; return p }
func (p *panicLogger) WithField(key string, val any) logx.Logger   { _ = p.n; return p }
func (p *panicLogger) Sync() error                                 { _ = p.n; return nil }
func (p *panicLogger) Close() error                                { _ = p.n; return nil }
func (p *panicLogger) SafeExit(f func())                           { _ = p.n; f() }

// TestNormalizeLogger 覆盖三种输入的归一行为。
func TestNormalizeLogger(t *testing.T) {
	if got := normalizeLogger(nil); got == nil {
		t.Fatal("未类型化 nil 应归一为非 nil")
	}
	var typed *panicLogger
	if got := normalizeLogger(typed); got == nil {
		t.Fatal("类型化 nil 应归一为非 nil")
	}
	real := testLogger()
	if got := normalizeLogger(real); got != real {
		t.Fatal("有效 logger 应原样返回")
	}
}

// TestDispatcherNormalizeLogger 覆盖执行器构造期归一与全流程回归。
func TestDispatcherNormalizeLogger(t *testing.T) {
	cases := []struct {
		name string
		opts []Option
	}{
		{"默认不传", nil},
		{"未类型化 nil", []Option{WithLogger(nil)}},
		{"类型化 nil", []Option{WithLogger((*panicLogger)(nil))}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, err := NewDispatcher(c.opts...)
			testx.RequireNoError(t, err)
			testx.RequireTrue(t, d.cfg.logger != nil)
			testx.RequireNoError(t, d.Handle("t", func(ctx context.Context, job Job) error {
				return nil
			}))
			if _, err := d.Submit(context.Background(), "t", nil); err != nil {
				t.Fatalf("提交失败：%v", err)
			}
			testx.RequireNoError(t, d.Shutdown(context.Background()))
		})
	}
}

// TestSchedulerNormalizeLogger 覆盖调度器构造期归一与日志路径。
func TestSchedulerNormalizeLogger(t *testing.T) {
	d, err := NewDispatcher()
	testx.RequireNoError(t, err)
	defer func() { _ = d.Shutdown(context.Background()) }()
	cases := []struct {
		name string
		opts []SchedulerOption
	}{
		{"默认不传", nil},
		{"未类型化 nil", []SchedulerOption{WithSchedulerLogger(nil)}},
		{"类型化 nil", []SchedulerOption{WithSchedulerLogger((*panicLogger)(nil))}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := NewScheduler(d, c.opts...)
			testx.RequireNoError(t, err)
			testx.RequireTrue(t, s.logger != nil)
			s.logWarn("jobx：测试告警", "t", errors.New("测试错误"))
			_ = s.Shutdown(context.Background())
		})
	}
}
