package EventBus

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"slices"
	"sync"
)

// SubscribeType - how the client intends to subscribe
type SubscribeType int

const (
	// Subscribe - subscribe to all events
	Subscribe SubscribeType = iota
	// SubscribeOnce - subscribe to only one event
	SubscribeOnce
)

const (
	// RegisterService - Server subscribe service method
	RegisterService = "ServerService.Register"
)

// SubscribeArg - object to hold subscribe arguments from remote event handlers
type SubscribeArg struct {
	ClientAddr    string
	ClientPath    string
	ServiceMethod string
	SubscribeType SubscribeType
	Topic         string
}

// Server - object capable of being subscribed to by remote handlers
type Server struct {
	eventBus    Bus
	address     string
	path        string
	lock        sync.Mutex // a lock for subscribers
	subscribers map[string][]*SubscribeArg
	service     *ServerService
	rpc         rpcHTTP
}

// NewServer - create a new Server at the address and path
func NewServer(address, path string, eventBus Bus) *Server {
	server := new(Server)
	server.eventBus = eventBus
	server.address = address
	server.path = path
	server.subscribers = make(map[string][]*SubscribeArg)
	server.service = &ServerService{server}
	return server
}

// EventBus - returns wrapped event bus
func (server *Server) EventBus() Bus {
	return server.eventBus
}

// rpcCallback returns the local handler that forwards events to a remote client.
// Delivery errors are logged, because a handler has no way to return them.
func (server *Server) rpcCallback(subscribeArg *SubscribeArg) func(args ...any) {
	return func(args ...any) {
		if subscribeArg.SubscribeType == SubscribeOnce {
			// the local handler is gone, so let the client subscribe again
			server.removeSubscriber(subscribeArg)
		}
		if err := pushEvent(subscribeArg, args); err != nil {
			log.Printf("EventBus: push %q to %s%s: %v", subscribeArg.Topic, subscribeArg.ClientAddr, subscribeArg.ClientPath, err)
		}
	}
}

func pushEvent(subscribeArg *SubscribeArg, args []any) error {
	client, err := rpc.DialHTTPPath("tcp", subscribeArg.ClientAddr, subscribeArg.ClientPath)
	if err != nil {
		return err
	}
	defer client.Close()
	clientArg := &ClientArg{Args: args, Topic: subscribeArg.Topic}
	var reply bool
	return client.Call(subscribeArg.ServiceMethod, clientArg, &reply)
}

// HasClientSubscribed - True if a client subscribed to this server with the same topic
func (server *Server) HasClientSubscribed(arg *SubscribeArg) bool {
	server.lock.Lock()
	defer server.lock.Unlock()
	return server.hasClientSubscribed(arg)
}

func (server *Server) hasClientSubscribed(arg *SubscribeArg) bool {
	return slices.ContainsFunc(server.subscribers[arg.Topic], func(subscriber *SubscribeArg) bool {
		return *subscriber == *arg
	})
}

func (server *Server) removeSubscriber(arg *SubscribeArg) {
	server.lock.Lock()
	defer server.lock.Unlock()
	server.subscribers[arg.Topic] = slices.DeleteFunc(server.subscribers[arg.Topic], func(subscriber *SubscribeArg) bool {
		return subscriber == arg
	})
}

// Start - starts a service for remote clients to subscribe to events
func (server *Server) Start() error {
	if server.rpc.started() {
		return errors.New("Server bus already started")
	}
	address, err := server.rpc.start(server.address, server.path, map[string]any{"ServerService": server.service})
	if err != nil {
		return fmt.Errorf("listen error: %w", err)
	}
	server.address = address
	return nil
}

// Stop - stops serving and closes the listener
func (server *Server) Stop() {
	server.rpc.stop()
}

// ServerService - service object to listen to remote subscriptions
type ServerService struct {
	server *Server
}

// Register - Registers a remote handler to this event bus
// for a remote subscribe - a given client address only needs to subscribe once
// event will be republished in local event bus
func (service *ServerService) Register(arg *SubscribeArg, success *bool) error {
	server := service.server
	server.lock.Lock()
	defer server.lock.Unlock()
	if !server.hasClientSubscribed(arg) {
		rpcCallback := server.rpcCallback(arg)
		var err error
		switch arg.SubscribeType {
		case Subscribe:
			err = server.eventBus.Subscribe(arg.Topic, rpcCallback)
		case SubscribeOnce:
			err = server.eventBus.SubscribeOnce(arg.Topic, rpcCallback)
		default:
			err = fmt.Errorf("unknown subscribe type %d", arg.SubscribeType)
		}
		if err != nil {
			return err
		}
		server.subscribers[arg.Topic] = append(server.subscribers[arg.Topic], arg)
	}
	*success = true
	return nil
}
