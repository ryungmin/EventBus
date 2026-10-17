package EventBus

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"sync"
)

// EventBus - box for handlers and callbacks.
type EventBus struct {
	handlers map[string][]*eventHandler
	lock     sync.Mutex // a lock for the map
	async    asyncTracker
}

// New returns new EventBus with empty handlers.
func New() Bus {
	b := &EventBus{
		make(map[string][]*eventHandler),
		sync.Mutex{},
		asyncTracker{},
	}
	return Bus(b)
}

// doSubscribe handles the subscription logic and is utilized by the public Subscribe functions
func (bus *EventBus) doSubscribe(topic string, fn any, handler *eventHandler) error {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	if !(reflect.TypeOf(fn).Kind() == reflect.Func) {
		return fmt.Errorf("%s is not of type reflect.Func", reflect.TypeOf(fn).Kind())
	}
	bus.handlers[topic] = append(bus.handlers[topic], handler)
	return nil
}

// Subscribe subscribes to a topic.
// Returns error if `fn` is not a function.
func (bus *EventBus) Subscribe(topic string, fn any) error {
	return bus.doSubscribe(topic, fn, &eventHandler{
		reflect.ValueOf(fn), funcDataOf(fn), false, false, false, make(chan struct{}, 1),
	})
}

// SubscribeAsync subscribes to a topic with an asynchronous callback
// Transactional determines whether subsequent callbacks for a topic are
// run serially (true) or concurrently (false)
// Returns error if `fn` is not a function.
func (bus *EventBus) SubscribeAsync(topic string, fn any, transactional bool) error {
	return bus.doSubscribe(topic, fn, &eventHandler{
		reflect.ValueOf(fn), funcDataOf(fn), false, true, transactional, make(chan struct{}, 1),
	})
}

// SubscribeOnce subscribes to a topic once. Handler will be removed after executing.
// Returns error if `fn` is not a function.
func (bus *EventBus) SubscribeOnce(topic string, fn any) error {
	return bus.doSubscribe(topic, fn, &eventHandler{
		reflect.ValueOf(fn), funcDataOf(fn), true, false, false, make(chan struct{}, 1),
	})
}

// SubscribeOnceAsync subscribes to a topic once with an asynchronous callback
// Handler will be removed after executing.
// Returns error if `fn` is not a function.
func (bus *EventBus) SubscribeOnceAsync(topic string, fn any) error {
	return bus.doSubscribe(topic, fn, &eventHandler{
		reflect.ValueOf(fn), funcDataOf(fn), true, true, false, make(chan struct{}, 1),
	})
}

// HasCallback returns true if exists any callback subscribed to the topic.
func (bus *EventBus) HasCallback(topic string) bool {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	_, ok := bus.handlers[topic]
	if ok {
		return len(bus.handlers[topic]) > 0
	}
	return false
}

// Unsubscribe removes callback defined for a topic.
// Returns error if there are no callbacks subscribed to the topic.
func (bus *EventBus) Unsubscribe(topic string, handler any) error {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	if _, ok := bus.handlers[topic]; ok && len(bus.handlers[topic]) > 0 {
		bus.removeHandler(topic, bus.findHandlerIdx(topic, handler))
		return nil
	}
	return fmt.Errorf("topic %s doesn't exist", topic)
}

// Publish executes callback defined for a topic. Any additional argument will be transferred to the callback.
func (bus *EventBus) Publish(topic string, args ...any) {
	bus.PublishContext(context.Background(), topic, args...)
}

// PublishContext is like Publish, but remaining handlers are skipped once ctx is done.
// Handlers whose first parameter is a context.Context receive ctx.
// Async handlers receive the same ctx, so canceling it also cancels their ctx.
// Returns ctx.Err() if any handler was skipped, nil otherwise.
//
// Handlers run without holding the bus lock, so they may call the bus themselves.
// Handlers subscribed or unsubscribed during a publish do not affect that publish.
// Panics if args do not match a handler's parameters.
func (bus *EventBus) PublishContext(ctx context.Context, topic string, args ...any) error {
	bus.lock.Lock()
	handlers := slices.Clone(bus.handlers[topic])
	bus.lock.Unlock()
	for _, handler := range handlers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if handler.flagOnce && !bus.claimOnce(topic, handler) {
			continue // another publish already ran it
		}
		// Build the arguments here so a mismatch panics in the caller, not in an async goroutine.
		passedArguments := handler.arguments(ctx, topic, args)
		if !handler.async {
			handler.callBack.Call(passedArguments)
			continue
		}
		if handler.transactional && !handler.acquire(ctx) {
			return ctx.Err()
		}
		bus.async.add()
		go bus.callAsync(handler, passedArguments)
	}
	return nil
}

func (bus *EventBus) callAsync(handler *eventHandler, passedArguments []reflect.Value) {
	defer bus.async.done()
	if handler.transactional {
		defer handler.release()
	}
	handler.callBack.Call(passedArguments)
}

// claimOnce removes a once handler and reports whether this call removed it.
func (bus *EventBus) claimOnce(topic string, handler *eventHandler) bool {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	idx := slices.Index(bus.handlers[topic], handler)
	if idx < 0 {
		return false
	}
	bus.removeHandler(topic, idx)
	return true
}

func (bus *EventBus) removeHandler(topic string, idx int) {
	if _, ok := bus.handlers[topic]; !ok {
		return
	}
	l := len(bus.handlers[topic])

	if !(0 <= idx && idx < l) {
		return
	}

	copy(bus.handlers[topic][idx:], bus.handlers[topic][idx+1:])
	bus.handlers[topic][l-1] = nil // or the zero value of T
	bus.handlers[topic] = bus.handlers[topic][:l-1]
}

// findHandlerIdx returns the index of the handler subscribed with fn, or -1.
// The exact func value is preferred. Otherwise the first handler with the same code
// is used, which covers method values: each evaluation of obj.Method is a new func value.
func (bus *EventBus) findHandlerIdx(topic string, fn any) int {
	data := funcDataOf(fn)
	callback := reflect.ValueOf(fn)
	sameCode := -1
	for idx, handler := range bus.handlers[topic] {
		if handler.callBack.Type() != callback.Type() {
			continue
		}
		if handler.funcData == data {
			return idx
		}
		if sameCode < 0 && handler.callBack.Pointer() == callback.Pointer() {
			sameCode = idx
		}
	}
	return sameCode
}

// WaitAsync waits for all async callbacks to complete
func (bus *EventBus) WaitAsync() {
	bus.async.wait(context.Background())
}

// WaitAsyncContext waits for all async callbacks to complete or ctx to be done.
// Returns ctx.Err() if ctx is done first; running callbacks are not stopped.
func (bus *EventBus) WaitAsyncContext(ctx context.Context) error {
	return bus.async.wait(ctx)
}
