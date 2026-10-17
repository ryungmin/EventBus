package EventBus

import (
	"errors"
	"fmt"
	"net/rpc"
)

const (
	// PublishService - Client service method
	PublishService = "ClientService.PushEvent"
)

// ClientArg - object containing event for client to publish locally
type ClientArg struct {
	Args  []any
	Topic string
}

// Client - object capable of subscribing to a remote event bus
type Client struct {
	eventBus Bus
	address  string
	path     string
	service  *ClientService
	rpc      rpcHTTP
}

// NewClient - create a client object with the address and server path
func NewClient(address, path string, eventBus Bus) *Client {
	client := new(Client)
	client.eventBus = eventBus
	client.address = address
	client.path = path
	client.service = &ClientService{client}
	return client
}

// EventBus - returns the underlying event bus
func (client *Client) EventBus() Bus {
	return client.eventBus
}

// doSubscribe subscribes fn locally first, so no event pushed right after the remote
// registration is missed, and removes it again if the registration fails.
func (client *Client) doSubscribe(topic string, fn any, serverAddr, serverPath string, subscribeType SubscribeType) error {
	var err error
	if subscribeType == SubscribeOnce {
		err = client.eventBus.SubscribeOnce(topic, fn)
	} else {
		err = client.eventBus.Subscribe(topic, fn)
	}
	if err != nil {
		return err
	}
	if err := client.register(topic, serverAddr, serverPath, subscribeType); err != nil {
		client.eventBus.Unsubscribe(topic, fn)
		return err
	}
	return nil
}

func (client *Client) register(topic, serverAddr, serverPath string, subscribeType SubscribeType) error {
	rpcClient, err := rpc.DialHTTPPath("tcp", serverAddr, serverPath)
	if err != nil {
		return fmt.Errorf("dialing: %w", err)
	}
	defer rpcClient.Close()
	args := &SubscribeArg{client.address, client.path, PublishService, subscribeType, topic}
	var reply bool
	if err := rpcClient.Call(RegisterService, args, &reply); err != nil {
		return fmt.Errorf("Register error: %w", err)
	}
	if !reply {
		return errors.New("Register error: server refused the subscription")
	}
	return nil
}

// Subscribe subscribes to a topic in a remote event bus.
// Returns error if fn is not a function or the server cannot be reached.
func (client *Client) Subscribe(topic string, fn any, serverAddr, serverPath string) error {
	return client.doSubscribe(topic, fn, serverAddr, serverPath, Subscribe)
}

// SubscribeOnce subscribes once to a topic in a remote event bus.
// Returns error if fn is not a function or the server cannot be reached.
func (client *Client) SubscribeOnce(topic string, fn any, serverAddr, serverPath string) error {
	return client.doSubscribe(topic, fn, serverAddr, serverPath, SubscribeOnce)
}

// Start - starts the client service to listen to remote events
func (client *Client) Start() error {
	if client.rpc.started() {
		return errors.New("Client service already started")
	}
	address, err := client.rpc.start(client.address, client.path, map[string]any{"ClientService": client.service})
	if err != nil {
		return fmt.Errorf("listen error: %w", err)
	}
	client.address = address
	return nil
}

// Stop - stops serving and closes the listener
func (client *Client) Stop() {
	client.rpc.stop()
}

// ClientService - service object listening to events published in a remote event bus
type ClientService struct {
	client *Client
}

// PushEvent - exported service to listening to remote events.
// A panic while publishing, such as args that do not match the handler, is returned
// as an error so a remote caller cannot crash this process.
func (service *ClientService) PushEvent(arg *ClientArg, reply *bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("publish %q: %v", arg.Topic, r)
		}
	}()
	service.client.eventBus.Publish(arg.Topic, arg.Args...)
	*reply = true
	return nil
}
