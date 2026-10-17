package EventBus

import (
	"context"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var _ Bus = (*EventBus)(nil)

// receiveWithin fails the test if ch does not deliver within a second.
func receiveWithin[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func TestNew(t *testing.T) {
	bus := New()
	if bus == nil {
		t.Log("New EventBus not created!")
		t.Fail()
	}
}

func TestHasCallback(t *testing.T) {
	bus := New()
	bus.Subscribe("topic", func() {})
	if bus.HasCallback("topic_topic") {
		t.Fail()
	}
	if !bus.HasCallback("topic") {
		t.Fail()
	}
}

func TestSubscribe(t *testing.T) {
	bus := New()
	if bus.Subscribe("topic", func() {}) != nil {
		t.Fail()
	}
	if bus.Subscribe("topic", "String") == nil {
		t.Fail()
	}
}

func TestSubscribeOnce(t *testing.T) {
	bus := New()
	if bus.SubscribeOnce("topic", func() {}) != nil {
		t.Fail()
	}
	if bus.SubscribeOnce("topic", "String") == nil {
		t.Fail()
	}
}

func TestSubscribeOnceAndManySubscribe(t *testing.T) {
	bus := New()
	event := "topic"
	flag := 0
	fn := func() { flag += 1 }
	bus.SubscribeOnce(event, fn)
	bus.Subscribe(event, fn)
	bus.Subscribe(event, fn)
	bus.Publish(event)

	if flag != 3 {
		t.Fail()
	}
}

func TestManySubscribeOnce(t *testing.T) {
	bus := New()
	event := "topic"
	calls := 0
	fn := func() { calls++ }
	bus.SubscribeOnce(event, fn)
	bus.Subscribe(event, fn)
	bus.SubscribeOnce(event, fn)
	bus.SubscribeOnce(event, fn)
	bus.Publish(event)
	bus.Publish(event)

	// 4 handlers on the first publish, only the Subscribe one on the second
	if calls != 5 {
		t.Errorf("got %d calls, want 5", calls)
	}
}

func TestUnsubscribe(t *testing.T) {
	bus := New()
	handler := func() {}
	bus.Subscribe("topic", handler)
	if bus.Unsubscribe("topic", handler) != nil {
		t.Fail()
	}
	if bus.Unsubscribe("topic", handler) == nil {
		t.Fail()
	}
}

type handler struct {
	val int
}

func (h *handler) Handle() {
	h.val++
}

func TestUnsubscribeMethod(t *testing.T) {
	bus := New()
	h := &handler{val: 0}

	bus.Subscribe("topic", h.Handle)
	bus.Publish("topic")
	if bus.Unsubscribe("topic", h.Handle) != nil {
		t.Fail()
	}
	if bus.Unsubscribe("topic", h.Handle) == nil {
		t.Fail()
	}
	bus.Publish("topic")
	bus.WaitAsync()

	if h.val != 1 {
		t.Fail()
	}
}

func TestPublishContext(t *testing.T) {
	bus := New()
	bus.Subscribe("topic", func(a int, err error) {
		if a != 10 {
			t.Fail()
		}

		if err != nil {
			t.Fail()
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()
	bus.PublishContext(ctx, "topic", 10, nil)
}

func TestPublishContextFailed(t *testing.T) {
	bus := New()
	bus.Subscribe("topic", func(a int, err error) {
		if a != 10 {
			t.Fail()
		}

		if err != nil {
			t.Fail()
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()
	bus.PublishContext(ctx, "topic", 10, nil)
}

func TestSubcribeOnceAsync(t *testing.T) {
	results := make([]int, 0)

	bus := New()
	bus.SubscribeOnceAsync("topic", func(a int, out *[]int) {
		*out = append(*out, a)
	})

	bus.Publish("topic", 10, &results)
	bus.Publish("topic", 10, &results)

	bus.WaitAsync()

	if len(results) != 1 {
		t.Fail()
	}

	if bus.HasCallback("topic") {
		t.Fail()
	}
}

func TestSubscribeAsyncTransactional(t *testing.T) {
	results := make([]int, 0)

	bus := New()
	bus.SubscribeAsync("topic", func(a int, out *[]int, dur string) {
		sleep, _ := time.ParseDuration(dur)
		time.Sleep(sleep)
		*out = append(*out, a)
	}, true)

	bus.Publish("topic", 1, &results, "1s")
	bus.Publish("topic", 2, &results, "0s")

	bus.WaitAsync()

	if len(results) != 2 {
		t.Fail()
	}

	if results[0] != 1 || results[1] != 2 {
		t.Fail()
	}
}

func TestSubscribeAsync(t *testing.T) {
	results := make(chan int)

	bus := New()
	bus.SubscribeAsync("topic", func(a int, out chan<- int) {
		out <- a
	}, false)

	bus.Publish("topic", 1, results)
	bus.Publish("topic", 2, results)

	counted := make(chan int)
	go func() {
		numResults := 0
		for range results {
			numResults++
		}
		counted <- numResults
	}()

	bus.WaitAsync()
	close(results)

	if numResults := <-counted; numResults != 2 {
		t.Errorf("got %d results, want 2", numResults)
	}
}

func TestPublishCanceledContext(t *testing.T) {
	bus := New()
	called := false
	bus.Subscribe("topic", func() { called = true })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if bus.PublishContext(ctx, "topic") != context.Canceled {
		t.Fail()
	}
	if called {
		t.Fail()
	}
}

type ctxKey struct{}

func TestPublishPassesContext(t *testing.T) {
	bus := New()
	var got any
	var gotA int
	bus.Subscribe("topic", func(ctx context.Context, a int, err error) {
		got = ctx.Value(ctxKey{})
		gotA = a
	})
	ctx := context.WithValue(t.Context(), ctxKey{}, "v")
	bus.PublishContext(ctx, "topic", 10, nil)
	if got != "v" || gotA != 10 {
		t.Fail()
	}
}

func TestWaitAsyncContext(t *testing.T) {
	bus := New()
	release := make(chan struct{})
	bus.SubscribeAsync("topic", func() { <-release }, false)
	bus.Publish("topic")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if bus.WaitAsyncContext(ctx) != context.DeadlineExceeded {
		t.Fail()
	}

	close(release)
	if bus.WaitAsyncContext(t.Context()) != nil {
		t.Fail()
	}
}

func TestTransactionalPublishCanceledWhileWaiting(t *testing.T) {
	bus := New()
	release := make(chan struct{})
	calls := 0
	bus.SubscribeAsync("topic", func() {
		calls++
		<-release
	}, true)
	bus.Publish("topic")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	// blocks on handler lock until ctx is done
	if bus.PublishContext(ctx, "topic") != context.DeadlineExceeded {
		t.Fail()
	}

	close(release)
	bus.WaitAsync()
	if calls != 1 {
		t.Fail()
	}
}

func TestWaitAsyncContextNoGoroutineLeak(t *testing.T) {
	bus := New()
	release := make(chan struct{})
	bus.SubscribeAsync("topic", func() { <-release }, false)
	bus.Publish("topic")

	before := runtime.NumGoroutine()
	for range 100 {
		ctx, cancel := context.WithTimeout(t.Context(), time.Microsecond)
		if bus.WaitAsyncContext(ctx) != context.DeadlineExceeded {
			t.Fail()
		}
		cancel()
	}
	// goroutines left by earlier tests may still be exiting, so give the count time to settle;
	// a leak would leave about one goroutine per WaitAsyncContext call
	after := runtime.NumGoroutine()
	for deadline := time.Now().Add(time.Second); after > before && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
		after = runtime.NumGoroutine()
	}
	if after > before {
		t.Errorf("goroutines grew from %d to %d", before, after)
	}

	close(release)
	bus.WaitAsync()
}

func TestWaitAsyncConcurrentWithPublish(t *testing.T) {
	bus := New()
	bus.SubscribeAsync("topic", func() {}, false)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				bus.Publish("topic")
			}
		}
	}()
	for range 1000 {
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
		bus.WaitAsyncContext(ctx)
		cancel()
	}
	close(stop)
	<-done
	bus.WaitAsync()
}

func TestHandlerUsesBus(t *testing.T) {
	bus := New()
	calls := 0
	bus.Subscribe("inner", func() { calls++ })
	bus.Subscribe("outer", func() {
		bus.Publish("inner")
		bus.Subscribe("inner", func() {})
		bus.HasCallback("inner")
	})
	done := make(chan struct{})
	go func() {
		bus.Publish("outer")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler calling the bus deadlocked")
	}
	if calls != 1 {
		t.Errorf("got %d inner calls, want 1", calls)
	}
}

func TestUnsubscribeClosure(t *testing.T) {
	bus := New()
	var got []int
	newHandler := func(n int) func() { return func() { got = append(got, n) } }
	first, second := newHandler(1), newHandler(2)
	bus.Subscribe("topic", first)
	bus.Subscribe("topic", second)
	bus.Unsubscribe("topic", second)
	bus.Publish("topic")
	if len(got) != 1 || got[0] != 1 {
		t.Errorf("got calls %v, want [1]", got)
	}
}

func TestPublishBadArgsPanicsInCaller(t *testing.T) {
	tests := []struct {
		name string
		args []any
	}{
		{"too few", nil},
		{"too many", []any{1, 2}},
		{"wrong type", []any{"one"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bus := New()
			bus.SubscribeAsync("topic", func(a int) {}, false)
			defer func() {
				if recover() == nil {
					t.Error("Publish did not panic")
				}
				bus.WaitAsync()
			}()
			bus.Publish("topic", tt.args...)
		})
	}
}

func TestPublishVariadic(t *testing.T) {
	bus := New()
	var got []int
	bus.Subscribe("topic", func(ctx context.Context, prefix string, rest ...int) {
		got = rest
	})
	bus.Publish("topic", "p")
	bus.Publish("topic", "p", 1, 2)
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Errorf("got %v, want [1 2]", got)
	}
}

type counter struct{ n int }

func (c *counter) inc() { c.n++ }

func TestUnsubscribeMethodValue(t *testing.T) {
	bus := New()
	c := &counter{}
	bus.Subscribe("topic", c.inc)
	// c.inc evaluates to a new func value each time
	if bus.Unsubscribe("topic", c.inc) != nil {
		t.Fail()
	}
	bus.Publish("topic")
	if c.n != 0 {
		t.Errorf("got %d calls after Unsubscribe, want 0", c.n)
	}
}

func TestSubscribeRejectsNonFunc(t *testing.T) {
	bus := New()
	subscribers := map[string]func(fn any) error{
		"Subscribe":          func(fn any) error { return bus.Subscribe("topic", fn) },
		"SubscribeAsync":     func(fn any) error { return bus.SubscribeAsync("topic", fn, false) },
		"SubscribeOnce":      func(fn any) error { return bus.SubscribeOnce("topic", fn) },
		"SubscribeOnceAsync": func(fn any) error { return bus.SubscribeOnceAsync("topic", fn) },
	}
	for name, subscribe := range subscribers {
		for _, fn := range []any{1, "string", struct{}{}, []func(){}} {
			if subscribe(fn) == nil {
				t.Errorf("%s(%T) returned nil, want error", name, fn)
			}
		}
	}
	if bus.HasCallback("topic") {
		t.Error("rejected handlers were subscribed")
	}
}

func TestHasCallbackLifecycle(t *testing.T) {
	bus := New()
	handler := func() {}
	if bus.HasCallback("topic") {
		t.Error("unknown topic has a callback")
	}
	bus.SubscribeOnce("topic", handler)
	if !bus.HasCallback("topic") {
		t.Error("subscribed topic has no callback")
	}
	bus.Publish("topic")
	if bus.HasCallback("topic") {
		t.Error("topic still has a callback after its once handler ran")
	}
	bus.Subscribe("topic", handler)
	bus.Unsubscribe("topic", handler)
	if bus.HasCallback("topic") {
		t.Error("topic still has a callback after Unsubscribe")
	}
}

func TestUnsubscribeErrors(t *testing.T) {
	bus := New()
	handler := func() {}
	if bus.Unsubscribe("unknown", handler) == nil {
		t.Error("Unsubscribe of an unknown topic returned nil")
	}
	bus.Subscribe("topic", handler)
	bus.Unsubscribe("topic", handler)
	if bus.Unsubscribe("topic", handler) == nil {
		t.Error("Unsubscribe of an emptied topic returned nil")
	}
}

func TestUnsubscribeRemovesOneOfDuplicates(t *testing.T) {
	bus := New()
	calls := 0
	handler := func() { calls++ }
	bus.Subscribe("topic", handler)
	bus.Subscribe("topic", handler)
	bus.Unsubscribe("topic", handler)
	bus.Publish("topic")
	if calls != 1 {
		t.Errorf("got %d calls, want 1", calls)
	}
}

func TestUnsubscribeMethodValueOfOtherReceiverKeepsExact(t *testing.T) {
	// the exact func value wins over a handler with the same code
	bus := New()
	a, b := &counter{}, &counter{}
	incB := b.inc
	bus.Subscribe("topic", a.inc)
	bus.Subscribe("topic", incB)
	bus.Unsubscribe("topic", incB)
	bus.Publish("topic")
	if a.n != 1 || b.n != 0 {
		t.Errorf("got a=%d b=%d, want a=1 b=0", a.n, b.n)
	}
}

func TestPublishNoHandlers(t *testing.T) {
	bus := New()
	bus.Publish("nobody")
	if err := bus.PublishContext(context.Background(), "nobody"); err != nil {
		t.Errorf("got %v, want nil", err)
	}
}

func TestPublishOrderAndTopicIsolation(t *testing.T) {
	bus := New()
	var got []string
	for _, name := range []string{"a", "b", "c"} {
		bus.Subscribe("topic", func() { got = append(got, name) })
	}
	bus.Subscribe("other", func() { got = append(got, "other") })
	bus.Publish("topic")
	if want := []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestPublishPassesArgs(t *testing.T) {
	bus := New()
	var gotErr error
	var gotPtr *int
	var gotN int
	bus.Subscribe("topic", func(n int, e error, p *int) { gotN, gotErr, gotPtr = n, e, p })
	bus.Publish("topic", 7, nil, nil)
	if gotN != 7 || gotErr != nil || gotPtr != nil {
		t.Errorf("got (%d, %v, %v), want (7, nil, nil)", gotN, gotErr, gotPtr)
	}
}

func TestPublishSnapshot(t *testing.T) {
	bus := New()
	var got []string
	late := func() { got = append(got, "late") }
	removed := func() { got = append(got, "removed") }
	bus.Subscribe("topic", func() {
		got = append(got, "first")
		bus.Subscribe("topic", late)
		bus.Unsubscribe("topic", removed)
	})
	bus.Subscribe("topic", removed)

	bus.Publish("topic")
	if want := []string{"first", "removed"}; !slices.Equal(got, want) {
		t.Errorf("first publish: got %v, want %v", got, want)
	}
	got = nil
	bus.Publish("topic")
	if want := []string{"first", "late"}; !slices.Equal(got, want) {
		t.Errorf("second publish: got %v, want %v", got, want)
	}
}

func TestOnceHandlerPublishingItsOwnTopic(t *testing.T) {
	bus := New()
	calls := 0
	bus.SubscribeOnce("topic", func() {
		calls++
		bus.Publish("topic")
	})
	bus.Publish("topic")
	if calls != 1 {
		t.Errorf("got %d calls, want 1", calls)
	}
}

func TestSubscribeOnceConcurrentPublish(t *testing.T) {
	for _, async := range []bool{false, true} {
		bus := New()
		var calls atomic.Int32
		if async {
			bus.SubscribeOnceAsync("topic", func() { calls.Add(1) })
		} else {
			bus.SubscribeOnce("topic", func() { calls.Add(1) })
		}
		var wg sync.WaitGroup
		for range 50 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				bus.Publish("topic")
			}()
		}
		wg.Wait()
		bus.WaitAsync()
		if n := calls.Load(); n != 1 {
			t.Errorf("async=%v: got %d calls, want 1", async, n)
		}
	}
}

func TestPanickingHandlerLeavesBusUsable(t *testing.T) {
	bus := New()
	bus.SubscribeOnce("topic", func() { panic("boom") })
	func() {
		defer func() {
			if r := recover(); r != "boom" {
				t.Errorf("got panic %v, want boom", r)
			}
		}()
		bus.Publish("topic")
	}()
	if bus.HasCallback("topic") {
		t.Error("panicking once handler is still subscribed")
	}
	calls := 0
	done := make(chan struct{})
	go func() {
		bus.Subscribe("topic", func() { calls++ })
		bus.Publish("topic")
		close(done)
	}()
	receiveWithin(t, done, "the bus after a handler panicked")
	if calls != 1 {
		t.Errorf("got %d calls, want 1", calls)
	}
}

func TestPublishContextCanceledByHandler(t *testing.T) {
	bus := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	onceCalls := 0
	bus.Subscribe("topic", func() { cancel() })
	bus.SubscribeOnce("topic", func() { onceCalls++ })

	if err := bus.PublishContext(ctx, "topic"); err != context.Canceled {
		t.Errorf("got %v, want %v", err, context.Canceled)
	}
	if onceCalls != 0 {
		t.Error("handler after the cancel ran")
	}
	// the skipped once handler was not used up
	bus.Publish("topic")
	if onceCalls != 1 {
		t.Errorf("got %d once calls on the next publish, want 1", onceCalls)
	}
}

func TestAsyncHandlerReceivesContext(t *testing.T) {
	bus := New()
	type ctxKey struct{}
	got := make(chan any, 1)
	bus.SubscribeAsync("topic", func(ctx context.Context) { got <- ctx.Value(ctxKey{}) }, false)
	bus.PublishContext(context.WithValue(context.Background(), ctxKey{}, "v"), "topic")
	if v := receiveWithin(t, got, "the async handler"); v != "v" {
		t.Errorf("got %v, want v", v)
	}
	bus.WaitAsync()
}

func TestAsyncHandlerSeesCancel(t *testing.T) {
	bus := New()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	result := make(chan error, 1)
	bus.SubscribeAsync("topic", func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		result <- ctx.Err()
	}, false)
	bus.PublishContext(ctx, "topic")
	receiveWithin(t, started, "the async handler to start")
	cancel()
	if err := receiveWithin(t, result, "the async handler to see the cancel"); err != context.Canceled {
		t.Errorf("got %v, want %v", err, context.Canceled)
	}
	bus.WaitAsync()
}

// startedTogether subscribes a handler that blocks until n calls are running at once,
// publishes n times from separate goroutines and reports whether that happened.
func startedTogether(t *testing.T, subscribe func(bus Bus, fn any), n int) bool {
	t.Helper()
	bus := New()
	running := make(chan struct{}, n)
	release := make(chan struct{})
	subscribe(bus, func() {
		running <- struct{}{}
		<-release
	})
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			bus.PublishContext(ctx, "topic")
		}()
	}
	together := true
	for range n {
		select {
		case <-running:
		case <-time.After(100 * time.Millisecond):
			together = false
		}
		if !together {
			break
		}
	}
	close(release)
	wg.Wait()
	bus.WaitAsync()
	return together
}

func TestHandlersRunConcurrently(t *testing.T) {
	if !startedTogether(t, func(bus Bus, fn any) { bus.Subscribe("topic", fn) }, 3) {
		t.Error("sync handlers published from separate goroutines did not run concurrently")
	}
	if !startedTogether(t, func(bus Bus, fn any) { bus.SubscribeAsync("topic", fn, false) }, 3) {
		t.Error("non-transactional async handlers did not run concurrently")
	}
	if startedTogether(t, func(bus Bus, fn any) { bus.SubscribeAsync("topic", fn, true) }, 2) {
		t.Error("transactional async handlers ran concurrently")
	}
}

func TestTransactionalSerialAndOrdered(t *testing.T) {
	bus := New()
	var running atomic.Int32
	var overlapped atomic.Bool
	var got []int
	bus.SubscribeAsync("topic", func(n int) {
		if running.Add(1) > 1 {
			overlapped.Store(true)
		}
		got = append(got, n)
		runtime.Gosched()
		running.Add(-1)
	}, true)
	want := make([]int, 100)
	for i := range want {
		want[i] = i
		bus.Publish("topic", i)
	}
	bus.WaitAsync()
	if overlapped.Load() {
		t.Error("transactional callbacks overlapped")
	}
	if !slices.Equal(got, want) {
		t.Errorf("callbacks ran out of publish order: %v", got)
	}
}

func TestTransactionalCanceledBeforeWaitDoesNotRun(t *testing.T) {
	bus := New()
	release := make(chan struct{})
	var calls atomic.Int32
	bus.SubscribeAsync("topic", func() {
		calls.Add(1)
		<-release
	}, true)
	bus.Publish("topic")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bus.PublishContext(ctx, "topic"); err != context.Canceled {
		t.Errorf("got %v, want %v", err, context.Canceled)
	}
	close(release)
	bus.WaitAsync()
	if n := calls.Load(); n != 1 {
		t.Errorf("got %d calls, want 1", n)
	}
}

func TestWaitAsyncWaitsForAll(t *testing.T) {
	bus := New()
	bus.WaitAsync() // nothing pending
	var done atomic.Int32
	bus.SubscribeAsync("topic", func() {
		time.Sleep(time.Millisecond)
		done.Add(1)
	}, false)
	for range 100 {
		bus.Publish("topic")
	}
	bus.WaitAsync()
	if n := done.Load(); n != 100 {
		t.Errorf("WaitAsync returned with %d of 100 callbacks done", n)
	}
}

func TestConcurrentBusUse(t *testing.T) {
	bus := New()
	var calls atomic.Int64
	handler := func(n int) { calls.Add(1) }
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				switch (g + i) % 6 {
				case 0:
					bus.Subscribe("topic", handler)
				case 1:
					bus.SubscribeAsync("topic", handler, i%2 == 0)
				case 2:
					bus.SubscribeOnce("topic", handler)
				case 3:
					bus.Unsubscribe("topic", handler)
				case 4:
					bus.HasCallback("topic")
				case 5:
					bus.Publish("topic", i)
				}
			}
		}()
	}
	wg.Wait()
	bus.WaitAsync()
	if calls.Load() == 0 {
		t.Error("no handler ran")
	}
}
