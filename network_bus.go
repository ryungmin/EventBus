package EventBus

import (
	"errors"
	"fmt"
)

// NetworkBus - object capable of subscribing to remote event buses in addition to remote event
// busses subscribing to it's local event bus. Compoed of a server and client
type NetworkBus struct {
	*Client
	*Server
	service   *NetworkBusService
	sharedBus Bus
	address   string
	path      string
}

// NewNetworkBus - returns a new network bus object at the server address and path
func NewNetworkBus(address, path string) *NetworkBus {
	bus := new(NetworkBus)
	bus.sharedBus = New()
	bus.Server = NewServer(address, path, bus.sharedBus)
	bus.Client = NewClient(address, path, bus.sharedBus)
	bus.service = &NetworkBusService{}
	bus.address = address
	bus.path = path
	return bus
}

// EventBus - returns wrapped event bus
func (networkBus *NetworkBus) EventBus() Bus {
	return networkBus.sharedBus
}

// NetworkBusService - object capable of serving the network bus
type NetworkBusService struct {
	rpc rpcHTTP
}

// Start - helper method to serve a network bus service
func (networkBus *NetworkBus) Start() error {
	service := networkBus.service
	if service.rpc.started() {
		return errors.New("Server bus already started")
	}
	address, err := service.rpc.start(networkBus.address, networkBus.path, map[string]any{
		"ServerService": networkBus.Server.service,
		"ClientService": networkBus.Client.service,
	})
	if err != nil {
		return fmt.Errorf("listen error: %w", err)
	}
	networkBus.address = address
	networkBus.Server.address = address
	networkBus.Client.address = address
	return nil
}

// Stop - stops serving and closes the listener
func (networkBus *NetworkBus) Stop() {
	networkBus.service.rpc.stop()
}
