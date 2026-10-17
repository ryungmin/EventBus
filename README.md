EventBus
======

Package EventBus is the little and lightweight eventbus with async compatibility for GoLang.

This is a fork of [asaskevich/EventBus](https://github.com/asaskevich/EventBus) that adds `context.Context` support.

#### Requirements
Go 1.24 or later.

#### Installation
Type the following command in your terminal:

	go get github.com/ryungmin/EventBus/v2

#### Import package in your project
Add following line in your `*.go` file:
```go
import "github.com/ryungmin/EventBus/v2"
```
The package name is `EventBus`. If you want a shorter name, use an alias:
```go
import (
	evbus "github.com/ryungmin/EventBus/v2"
)
```

#### Migrating from v1
* Change the import path to `github.com/ryungmin/EventBus/v2`.
* A handler whose first parameter is a `context.Context` now receives the context from the bus.
  Do not pass the context in `args` any more:
  ```go
  bus.Subscribe("topic", func(ctx context.Context, s string) { ... })

  bus.Publish("topic", ctx, "x")          // v1: ctx passed by hand; v2: panics, too many args
  bus.PublishContext(ctx, "topic", "x")   // v2
  ```
* Handlers run without holding the bus lock.
  A handler can now call `Publish`, `Subscribe` or `Unsubscribe` on its own bus; in v1 this deadlocked.
  Synchronous handlers published from several goroutines can run concurrently; in v1 they ran one at a time.
* When `args` do not match a handler's parameters, `Publish` panics in the caller with a message naming the topic.
  For async handlers v1 panicked inside the handler's goroutine, which could not be recovered.
* `Client.Subscribe` and `Client.SubscribeOnce` now return an error when the server cannot be reached.
  In v1 the error was printed and dropped.
* `Server`, `Client` and `NetworkBus` serve on their own `http.ServeMux` instead of `http.DefaultServeMux`.
  `Stop` closes the listener, so a stopped service can be started again.
  The `/debug` + path page of net/rpc is no longer served.

#### Example
```go
func calculator(a int, b int) {
	fmt.Printf("%d\n", a+b)
}

func main() {
	bus := EventBus.New()
	bus.Subscribe("main:calculator", calculator)
	bus.Publish("main:calculator", 20, 40)
	bus.Unsubscribe("main:calculator", calculator)
}
```

#### Implemented methods
* **New()**
* **Subscribe()**
* **SubscribeOnce()**
* **HasCallback()**
* **Unsubscribe()**
* **Publish()**
* **PublishContext()**
* **SubscribeAsync()**
* **SubscribeOnceAsync()**
* **WaitAsync()**
* **WaitAsyncContext()**

#### New()
New returns new EventBus with empty handlers.
```go
bus := EventBus.New()
```

#### Subscribe(topic string, fn any) error
Subscribe to a topic. Returns error if `fn` is not a function.
```go
func Handler() { ... }
...
bus.Subscribe("topic:handler", Handler)
```

#### SubscribeOnce(topic string, fn any) error
Subscribe to a topic once. Handler will be removed after executing. Returns error if `fn` is not a function.
```go
func HelloWorld() { ... }
...
bus.SubscribeOnce("topic:handler", HelloWorld)
```

#### Unsubscribe(topic string, fn any) error
Remove callback defined for a topic. Returns error if there are no callbacks subscribed to the topic.
Pass the same func value that you subscribed. Closures made from the same func literal are told apart.
For a method value such as `obj.Method`, the first handler with the same method is removed.
```go
bus.Unsubscribe("topic:handler", HelloWorld)
```

#### HasCallback(topic string) bool
Returns true if exists any callback subscribed to the topic.

#### Publish(topic string, args ...any)
Publish executes callback defined for a topic. Any additional argument will be transferred to the callback.
```go
func Handler(str string) { ... }
...
bus.Subscribe("topic:handler", Handler)
...
bus.Publish("topic:handler", "Hello, World!")
```
Publish panics if `args` do not match a handler's parameters. A `nil` arg becomes the zero value of its parameter.

Handlers run without holding the bus lock, so a handler can call the bus.
Handlers subscribed or unsubscribed while Publish runs do not affect that Publish.

#### PublishContext(ctx context.Context, topic string, args ...any) error
PublishContext works like Publish, but it stops calling the remaining handlers once `ctx` is done.
It returns `ctx.Err()` if any handler was skipped, and `nil` otherwise.

If the first parameter of a handler is a `context.Context`, the handler receives `ctx` there and `args` fill the remaining parameters.
Handlers without a `context.Context` parameter work as before.
`Publish` passes `context.Background()` to such handlers.
```go
func Handler(ctx context.Context, str string) {
	select {
	case <-ctx.Done():
		return
	default:
	}
	...
}
...
bus.Subscribe("topic:handler", Handler)
...
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()
if err := bus.PublishContext(ctx, "topic:handler", "Hello, World!"); err != nil {
	// some handlers were skipped
}
```
For a transactional async handler, PublishContext waits for the previous callback of that handler to finish.
If `ctx` is done while it waits, PublishContext returns `ctx.Err()`.

Async handlers receive the same `ctx`. If you cancel `ctx` right after PublishContext returns, for example with `defer cancel()`, async handlers see a canceled `ctx`.

#### SubscribeAsync(topic string, fn any, transactional bool) error
Subscribe to a topic with an asynchronous callback. Returns error if `fn` is not a function.
```go
func slowCalculator(a, b int) {
	time.Sleep(3 * time.Second)
	fmt.Printf("%d\n", a+b)
}

bus := EventBus.New()
bus.SubscribeAsync("main:slow_calculator", slowCalculator, false)

bus.Publish("main:slow_calculator", 20, 60)

fmt.Println("start: do some stuff while waiting for a result")
fmt.Println("end: do some stuff while waiting for a result")

bus.WaitAsync() // wait for all async callbacks to complete

fmt.Println("do some stuff after waiting for result")
```
Transactional determines whether subsequent callbacks for a topic are run serially (true) or concurrently (false).

#### SubscribeOnceAsync(topic string, fn any) error
SubscribeOnceAsync works like SubscribeOnce except the callback is executed asynchronously.

#### WaitAsync()
WaitAsync waits for all async callbacks to complete.

#### WaitAsyncContext(ctx context.Context) error
WaitAsyncContext waits for all async callbacks to complete or for `ctx` to be done.
It returns `ctx.Err()` if `ctx` is done first. Running callbacks are not stopped.
```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := bus.WaitAsyncContext(ctx); err != nil {
	// timed out; callbacks are still running
}
```

#### Cross Process Events
Works with two rpc services:
- a client service to listen to remotely published events from a server
- a server service to listen to client subscriptions

server.go
```go
func main() {
	server := EventBus.NewServer(":2010", "/_server_bus_", EventBus.New())
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
	// ...
	server.EventBus().Publish("main:calculator", 4, 6)
	// ...
	server.Stop()
}
```

client.go
```go
func main() {
	client := EventBus.NewClient(":2015", "/_client_bus_", EventBus.New())
	if err := client.Start(); err != nil {
		log.Fatal(err)
	}
	if err := client.Subscribe("main:calculator", calculator, ":2010", "/_server_bus_"); err != nil {
		log.Fatal(err)
	}
	// ...
	client.Stop()
}
```
`Start` returns an error if the address cannot be listened on. With port `0`, such as `"localhost:0"`, a free port is picked and the address is updated to it.

The server delivers each event with an rpc call to the client from inside `Publish`, so `Publish` waits for the clients.
If a client cannot be reached, the error is logged with the `log` package and the other handlers still run.

#### Support
If you do have a contribution for the package feel free to put up a Pull Request or open Issue.

#### Special thanks to [contributors](https://github.com/asaskevich/EventBus/graphs/contributors)
* [Brian Downs](https://github.com/briandowns)
* [Dominik Schulz](https://github.com/gittex)
* [bennAH](https://github.com/bennAH)
* [John Noble](https://github.com/gaxunil)
* [Evan Borgstrom](https://github.com/borgstrom)
