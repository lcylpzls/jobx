package jobx

import (
	"time"

	"github.com/lcylpzls/jobx/internal/core"
	"github.com/lcylpzls/logx"
)

const (
	QueueFullBlock = core.QueueFullBlock
	QueueFullDrop  = core.QueueFullDrop
)

const (
	ConflictSkip    = core.ConflictSkip
	ConflictReplace = core.ConflictReplace
	ConflictAllow   = core.ConflictAllow
)

const (
	StatusQueued    = core.StatusQueued
	StatusDelayed   = core.StatusDelayed
	StatusRunning   = core.StatusRunning
	StatusSucceeded = core.StatusSucceeded
	StatusFailed    = core.StatusFailed
	StatusCancelled = core.StatusCancelled
)

const (
	CodeInvalidConfig    = core.CodeInvalidConfig
	CodeHandlerNotFound  = core.CodeHandlerNotFound
	CodeHandlerConflict  = core.CodeHandlerConflict
	CodeJobInvalid       = core.CodeJobInvalid
	CodeJobNotFound      = core.CodeJobNotFound
	CodeQueueFull        = core.CodeQueueFull
	CodeShuttingDown     = core.CodeShuttingDown
	CodeTimeout          = core.CodeTimeout
	CodeRetryExhausted   = core.CodeRetryExhausted
	CodeExecutionFailed  = core.CodeExecutionFailed
	CodeSkipped          = core.CodeSkipped
	CodeReplaced         = core.CodeReplaced
	CodeCronInvalid      = core.CodeCronInvalid
	CodeSchedulerStopped = core.CodeSchedulerStopped
	CodeStoreInvalid     = core.CodeStoreInvalid
	CodeIDGenerateFailed = core.CodeIDGenerateFailed
	CodeJobCancelled     = core.CodeJobCancelled
)

type (
	QueueFullPolicy = core.QueueFullPolicy
	ConflictPolicy  = core.ConflictPolicy
	Metrics         = core.Metrics
	Option          = core.Option
	Job             = core.Job
	Handler         = core.Handler
	SubmitOption    = core.SubmitOption
	Status          = core.Status
	Dispatcher      = core.Dispatcher
	TaskEvent       = core.TaskEvent
	EventHook       = core.EventHook
	TraceAttr       = core.TraceAttr
	TraceHook       = core.TraceHook
	Store           = core.Store
	Scheduler       = core.Scheduler
	Schedule        = core.Schedule
	SchedulerOption = core.SchedulerOption
)

func NewDispatcher(opts ...Option) (*Dispatcher, error) { return core.NewDispatcher(opts...) }
func WithWorkers(n int) Option                          { return core.WithWorkers(n) }
func WithQueueSize(n int) Option                        { return core.WithQueueSize(n) }
func WithQueueFullPolicy(p QueueFullPolicy) Option      { return core.WithQueueFullPolicy(p) }
func WithConflictPolicy(p ConflictPolicy) Option        { return core.WithConflictPolicy(p) }
func WithMaxPayloadBytes(n int) Option                  { return core.WithMaxPayloadBytes(n) }
func WithLogger(logger logx.Logger) Option              { return core.WithLogger(logger) }
func WithMetrics(m Metrics) Option                      { return core.WithMetrics(m) }
func WithStore(store Store) Option                      { return core.WithStore(store) }
func WithClock(now func() time.Time) Option             { return core.WithClock(now) }
func WithTraceHook(h TraceHook) Option                  { return core.WithTraceHook(h) }
func WithEventHook(h EventHook) Option                  { return core.WithEventHook(h) }
func NewScheduler(dispatcher *Dispatcher, opts ...SchedulerOption) (*Scheduler, error) {
	return core.NewScheduler(dispatcher, opts...)
}
func WithLocation(loc *time.Location) SchedulerOption { return core.WithLocation(loc) }
func WithSchedulerLogger(logger logx.Logger) SchedulerOption {
	return core.WithSchedulerLogger(logger)
}
func WithTimeout(timeout time.Duration) SubmitOption { return core.WithTimeout(timeout) }
func WithRunAt(at time.Time) SubmitOption            { return core.WithRunAt(at) }
func WithRetry(maxRetries int, delay time.Duration) SubmitOption {
	return core.WithRetry(maxRetries, delay)
}
