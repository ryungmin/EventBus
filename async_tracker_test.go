package EventBus

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestAsyncTrackerIdle(t *testing.T) {
	var tracker asyncTracker
	if err := tracker.wait(context.Background()); err != nil {
		t.Errorf("got %v on an idle tracker, want nil", err)
	}
	// idle wins over a done ctx
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tracker.wait(ctx); err != nil {
		t.Errorf("got %v on an idle tracker with a canceled ctx, want nil", err)
	}
}

func TestAsyncTrackerWaitsForAll(t *testing.T) {
	var tracker asyncTracker
	tracker.add()
	tracker.add()
	waited := make(chan error, 1)
	go func() { waited <- tracker.wait(context.Background()) }()

	tracker.done()
	select {
	case <-waited:
		t.Fatal("wait returned with one callback still pending")
	case <-time.After(20 * time.Millisecond):
	}

	tracker.done()
	select {
	case err := <-waited:
		if err != nil {
			t.Errorf("got %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not return after all callbacks were done")
	}
}

func TestAsyncTrackerReusedAfterIdle(t *testing.T) {
	var tracker asyncTracker
	tracker.add()
	tracker.done()

	// a new busy period needs a new idle channel
	tracker.add()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := tracker.wait(ctx); err != context.DeadlineExceeded {
		t.Errorf("got %v while busy again, want %v", err, context.DeadlineExceeded)
	}
	tracker.done()
	if err := tracker.wait(context.Background()); err != nil {
		t.Errorf("got %v, want nil", err)
	}
}

func TestAsyncTrackerConcurrent(t *testing.T) {
	var tracker asyncTracker
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				tracker.add()
				go tracker.done()
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
				tracker.wait(ctx)
				cancel()
			}
		}()
	}
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tracker.wait(ctx); err != nil {
		t.Errorf("got %v after all callbacks were done, want nil", err)
	}
}
