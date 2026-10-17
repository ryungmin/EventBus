package EventBus

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"testing"
)

// anyPort asks the system for a free port; Start replaces it with the real address.
const anyPort = "localhost:0"

func startServer(t *testing.T, path string) *Server {
	t.Helper()
	server := NewServer(anyPort, path, New())
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Stop)
	return server
}

func startClient(t *testing.T, path string) *Client {
	t.Helper()
	client := NewClient(anyPort, path, New())
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Stop)
	return client
}

func startNetworkBus(t *testing.T, path string) *NetworkBus {
	t.Helper()
	bus := NewNetworkBus(anyPort, path)
	if err := bus.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bus.Stop)
	return bus
}

// canDial reports whether something listens on address.
func canDial(address string) bool {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// subscriberCount returns how many remote subscribers the server holds for topic.
func subscriberCount(server *Server, topic string) int {
	server.lock.Lock()
	defer server.lock.Unlock()
	return len(server.subscribers[topic])
}

type starter interface {
	Start() error
	Stop()
}

func TestStartStopRestart(t *testing.T) {
	starters := map[string]func() (starter, func() string){
		"Server": func() (starter, func() string) {
			s := NewServer(anyPort, "/_server_bus_", New())
			return s, func() string { return s.address }
		},
		"Client": func() (starter, func() string) {
			c := NewClient(anyPort, "/_client_bus_", New())
			return c, func() string { return c.address }
		},
		"NetworkBus": func() (starter, func() string) {
			b := NewNetworkBus(anyPort, "/_net_bus_")
			return b, func() string { return b.address }
		},
	}
	for name, newStarter := range starters {
		t.Run(name, func(t *testing.T) {
			s, address := newStarter()
			for round := range 2 {
				if err := s.Start(); err != nil {
					t.Fatalf("round %d: Start: %v", round, err)
				}
				if !canDial(address()) {
					t.Errorf("round %d: nothing listens on %s after Start", round, address())
				}
				if s.Start() == nil {
					t.Errorf("round %d: second Start returned nil", round)
				}
				s.Stop()
				if canDial(address()) {
					t.Errorf("round %d: %s still listens after Stop", round, address())
				}
			}
			s.Stop() // stopping twice is harmless
		})
	}
}

func TestStartListenError(t *testing.T) {
	busy, err := net.Listen("tcp", anyPort)
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	address := busy.Addr().String()

	starters := map[string]starter{
		"Server":     NewServer(address, "/_server_bus_", New()),
		"Client":     NewClient(address, "/_client_bus_", New()),
		"NetworkBus": NewNetworkBus(address, "/_net_bus_"),
	}
	for name, s := range starters {
		if s.Start() == nil {
			t.Errorf("%s: Start on a busy port returned nil", name)
		}
		// a failed Start leaves it stopped, so it can be started again
		if s.Start() == nil {
			t.Errorf("%s: retried Start on a busy port returned nil", name)
		}
	}
}

func TestRegister(t *testing.T) {
	server := NewServer(anyPort, "/_server_bus_", New())
	arg := &SubscribeArg{"localhost:1", "/_client_bus_", PublishService, Subscribe, "topic"}
	var reply bool
	if err := server.service.Register(arg, &reply); err != nil || !reply {
		t.Fatalf("got (%v, %v), want (nil, true)", err, reply)
	}
	if server.eventBus.HasCallback("topic_topic") {
		t.Fail()
	}
	if !server.eventBus.HasCallback("topic") {
		t.Fail()
	}
	if !server.HasClientSubscribed(arg) {
		t.Error("registered client is not subscribed")
	}
}

func TestRegisterKeepsAllSubscribers(t *testing.T) {
	server := NewServer(anyPort, "/_server_bus_", New())
	first := &SubscribeArg{"localhost:1", "/_client_bus_", PublishService, Subscribe, "topic"}
	second := &SubscribeArg{"localhost:2", "/_client_bus_", PublishService, Subscribe, "topic"}
	var reply bool
	server.service.Register(first, &reply)
	server.service.Register(second, &reply)
	again := *first
	server.service.Register(&again, &reply) // same client again is ignored

	if n := subscriberCount(server, "topic"); n != 2 {
		t.Errorf("got %d subscribers, want 2", n)
	}
	if !server.HasClientSubscribed(first) || !server.HasClientSubscribed(second) {
		t.Error("a registered client is missing")
	}
}

func TestRegisterUnknownSubscribeType(t *testing.T) {
	server := NewServer(anyPort, "/_server_bus_", New())
	arg := &SubscribeArg{"localhost:1", "/_client_bus_", PublishService, SubscribeType(9), "topic"}
	var reply bool
	if server.service.Register(arg, &reply) == nil || reply {
		t.Error("Register with an unknown subscribe type succeeded")
	}
	if server.HasClientSubscribed(arg) {
		t.Error("rejected client is subscribed")
	}
}

func TestRegisterConcurrent(t *testing.T) {
	server := NewServer(anyPort, "/_server_bus_", New())
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			arg := &SubscribeArg{fmt.Sprintf("localhost:%d", i+1), "/_client_bus_", PublishService, Subscribe, "topic"}
			var reply bool
			server.service.Register(arg, &reply)
			server.HasClientSubscribed(arg)
		}()
	}
	wg.Wait()
	if n := subscriberCount(server, "topic"); n != 50 {
		t.Errorf("got %d subscribers, want 50", n)
	}
}

func TestPushEvent(t *testing.T) {
	client := NewClient(anyPort, "/_client_bus_", New())
	got := 0
	client.eventBus.Subscribe("topic", func(a int) { got = a })
	var reply bool
	if err := client.service.PushEvent(&ClientArg{[]any{10}, "topic"}, &reply); err != nil || !reply {
		t.Fatalf("got (%v, %v), want (nil, true)", err, reply)
	}
	if got != 10 {
		t.Errorf("got %d, want 10", got)
	}
}

func TestPushEventBadArgsReturnsError(t *testing.T) {
	client := NewClient(anyPort, "/_client_bus_", New())
	client.eventBus.Subscribe("topic", func(a int) {})
	var reply bool
	if err := client.service.PushEvent(&ClientArg{[]any{"not an int"}, "topic"}, &reply); err == nil || reply {
		t.Errorf("got (%v, %v), want an error and false", err, reply)
	}
}

func TestServerPublish(t *testing.T) {
	server := startServer(t, "/_server_bus_")
	client := startClient(t, "/_client_bus_")

	got := make(chan int, 2)
	if err := client.Subscribe("topic", func(a int) { got <- a }, server.address, server.path); err != nil {
		t.Fatal(err)
	}
	server.EventBus().Publish("topic", 10)
	server.EventBus().Publish("topic", 20)
	if a, b := receiveWithin(t, got, "the first event"), receiveWithin(t, got, "the second event"); a != 10 || b != 20 {
		t.Errorf("got %d and %d, want 10 and 20", a, b)
	}
}

func TestServerPublishOnce(t *testing.T) {
	server := startServer(t, "/_server_bus_")
	client := startClient(t, "/_client_bus_")

	got := make(chan int, 3)
	handler := func(a int) { got <- a }
	if err := client.SubscribeOnce("topic", handler, server.address, server.path); err != nil {
		t.Fatal(err)
	}
	server.EventBus().Publish("topic", 1)
	server.EventBus().Publish("topic", 2)
	if a := receiveWithin(t, got, "the once event"); a != 1 {
		t.Errorf("got %d, want 1", a)
	}
	if server.EventBus().HasCallback("topic") || subscriberCount(server, "topic") != 0 {
		t.Error("server still holds the once subscription")
	}

	// once the event was delivered, the client can subscribe once again
	if err := client.SubscribeOnce("topic", handler, server.address, server.path); err != nil {
		t.Fatal(err)
	}
	server.EventBus().Publish("topic", 3)
	if a := receiveWithin(t, got, "the event after subscribing again"); a != 3 {
		t.Errorf("got %d, want 3", a)
	}
	select {
	case a := <-got:
		t.Errorf("got unexpected event %d", a)
	default:
	}
}

func TestClientSubscribeUnreachableServer(t *testing.T) {
	client := NewClient(anyPort, "/_client_bus_", New())
	stopped := startServer(t, "/_server_bus_")
	stopped.Stop()

	if client.Subscribe("topic", func() {}, stopped.address, stopped.path) == nil {
		t.Error("Subscribe to a stopped server returned nil")
	}
	if client.EventBus().HasCallback("topic") {
		t.Error("handler stays subscribed locally after the remote subscribe failed")
	}
}

func TestClientSubscribeNonFunc(t *testing.T) {
	server := startServer(t, "/_server_bus_")
	client := NewClient(anyPort, "/_client_bus_", New())
	if client.Subscribe("topic", 1, server.address, server.path) == nil {
		t.Error("Subscribe with a non-func returned nil")
	}
	if subscriberCount(server, "topic") != 0 {
		t.Error("server registered a client whose handler was rejected")
	}
}

func TestServerPublishToGoneClient(t *testing.T) {
	server := startServer(t, "/_server_bus_")
	client := startClient(t, "/_client_bus_")
	if err := client.Subscribe("topic", func(int) {}, server.address, server.path); err != nil {
		t.Fatal(err)
	}
	client.Stop()

	done := make(chan struct{})
	go func() {
		// the push fails and is logged, it must not panic
		server.EventBus().Publish("topic", 10)
		close(done)
	}()
	receiveWithin(t, done, "Publish to a stopped client")
}

func TestServerPublishBadArgsToClient(t *testing.T) {
	server := startServer(t, "/_server_bus_")
	client := startClient(t, "/_client_bus_")
	if err := client.Subscribe("topic", func(int) {}, server.address, server.path); err != nil {
		t.Fatal(err)
	}
	// the client recovers from the mismatch and keeps serving
	server.EventBus().Publish("topic", "not an int")

	got := make(chan int, 1)
	if err := client.Subscribe("other", func(a int) { got <- a }, server.address, server.path); err != nil {
		t.Fatal(err)
	}
	server.EventBus().Publish("other", 10)
	if a := receiveWithin(t, got, "an event after a bad one"); a != 10 {
		t.Errorf("got %d, want 10", a)
	}
}

func TestDebugPathNotServed(t *testing.T) {
	// only the rpc path is served, nothing is registered on http.DefaultServeMux
	server := startServer(t, "/_server_bus_")
	if _, err := rpc.DialHTTPPath("tcp", server.address, "/debug/_server_bus_"); err == nil {
		t.Error("debug path is served")
	}
}

func TestNetworkBus(t *testing.T) {
	networkBusA := startNetworkBus(t, "/_net_bus_A")
	networkBusB := startNetworkBus(t, "/_net_bus_B")

	gotA := make(chan int, 1)
	if err := networkBusA.Subscribe("topic-A", func(a int) { gotA <- a }, networkBusB.address, networkBusB.path); err != nil {
		t.Fatal(err)
	}
	networkBusB.EventBus().Publish("topic-A", 10)
	if a := receiveWithin(t, gotA, "topic-A on bus A"); a != 10 {
		t.Errorf("got %d, want 10", a)
	}

	gotB := make(chan int, 1)
	if err := networkBusB.Subscribe("topic-B", func(a int) { gotB <- a }, networkBusA.address, networkBusA.path); err != nil {
		t.Fatal(err)
	}
	networkBusA.EventBus().Publish("topic-B", 20)
	if b := receiveWithin(t, gotB, "topic-B on bus B"); b != 20 {
		t.Errorf("got %d, want 20", b)
	}
}

func TestNetworkBusRestart(t *testing.T) {
	server := NewNetworkBus(anyPort, "/_net_bus_A")
	client := startNetworkBus(t, "/_net_bus_B")
	for round := range 2 {
		if err := server.Start(); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		got := make(chan int, 1)
		topic := fmt.Sprintf("topic-%d", round)
		if err := client.Subscribe(topic, func(a int) { got <- a }, server.address, server.path); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		server.EventBus().Publish(topic, round)
		if a := receiveWithin(t, got, "the event after a restart"); a != round {
			t.Errorf("round %d: got %d", round, a)
		}
		server.Stop()
		// a new port is picked on the next Start
		server.address = anyPort
	}
}
