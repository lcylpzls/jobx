package core

import (
	"context"
	"sync"
)

// readyQueue 就绪任务队列：支持阻塞/丢弃入队、出队与按 ID 物理移除。
type readyQueue struct {
	mu       sync.Mutex
	items    []*jobEntry
	cap      int
	closed   bool
	notify   chan struct{}
	closedCh chan struct{}
}

// newReadyQueue 构造就绪队列。
func newReadyQueue(cap int) *readyQueue {
	return &readyQueue{cap: cap, notify: make(chan struct{}, 1), closedCh: make(chan struct{})}
}

// push 入队；drop 为 true 时队列满直接返回 ErrQueueFull，
// 否则阻塞等待空间（响应 ctx 取消与关闭）。
func (q *readyQueue) push(ctx context.Context, entry *jobEntry, drop bool) error {
	for {
		q.mu.Lock()
		if len(q.items) < q.cap {
			q.items = append(q.items, entry)
			q.mu.Unlock()
			q.signal()
			return nil
		}
		closed := q.closed
		q.mu.Unlock()
		if drop {
			return ErrQueueFull
		}
		if closed {
			return ErrShuttingDown
		}
		select {
		case <-q.notify:
		case <-q.closedCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// pop 出队；队列关闭且为空时返回 false。
func (q *readyQueue) pop() (*jobEntry, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			entry := q.items[0]
			q.items = q.items[1:]
			q.mu.Unlock()
			q.signal()
			return entry, true
		}
		closed := q.closed
		q.mu.Unlock()
		if closed {
			return nil, false
		}
		select {
		case <-q.notify:
		case <-q.closedCh:
		}
	}
}

// remove 按 ID 物理移除条目，返回是否移除成功。
func (q *readyQueue) remove(id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, entry := range q.items {
		if entry.job.ID == id {
			q.items = append(q.items[:i], q.items[i+1:]...)
			return true
		}
	}
	return false
}

// close 关闭队列：唤醒阻塞的 pop 与 push。
func (q *readyQueue) close() {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		close(q.closedCh)
	}
	q.mu.Unlock()
	q.signal()
}

// signal 唤醒一个等待者（容量 1，非阻塞）。
func (q *readyQueue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}
