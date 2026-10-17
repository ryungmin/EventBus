package EventBus

import (
	"context"
	"sync"
)

// asyncTracker counts running async callbacks.
// Unlike sync.WaitGroup, waiting on it can be abandoned via ctx without leaking a goroutine.
type asyncTracker struct {
	lock    sync.Mutex
	pending int
	idle    chan struct{} // closed when pending drops to 0
}

func (t *asyncTracker) add() {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.pending == 0 {
		t.idle = make(chan struct{})
	}
	t.pending++
}

func (t *asyncTracker) done() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.pending--
	if t.pending == 0 {
		close(t.idle)
	}
}

// wait blocks until no callbacks are running or ctx is done.
func (t *asyncTracker) wait(ctx context.Context) error {
	t.lock.Lock()
	if t.pending == 0 {
		t.lock.Unlock()
		return nil
	}
	idle := t.idle
	t.lock.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
