package EventBus

import (
	"errors"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

// rpcHTTP serves net/rpc services over HTTP on its own listener and mux.
// rpc.Server.HandleHTTP registers on http.DefaultServeMux, which panics when the
// same path is registered twice, so a stopped service could never be started again.
type rpcHTTP struct {
	lock     sync.Mutex
	server   *http.Server
	listener net.Listener
}

// start listens on address and serves services at path.
// It returns the address to advertise: the listener's address when address asks
// for any port (port 0), otherwise address itself.
func (r *rpcHTTP) start(address, path string, services map[string]any) (string, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.server != nil {
		return "", errors.New("already started")
	}
	rpcServer := rpc.NewServer()
	for name, service := range services {
		if err := rpcServer.RegisterName(name, service); err != nil {
			return "", err
		}
	}
	mux := http.NewServeMux()
	mux.Handle(path, rpcServer)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return "", err
	}
	r.server = &http.Server{Handler: mux}
	r.listener = listener
	go r.server.Serve(listener)
	if _, port, _ := net.SplitHostPort(address); port == "0" {
		address = listener.Addr().String()
	}
	return address, nil
}

// stop closes the listener and all connections. It does nothing if not started.
func (r *rpcHTTP) stop() {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.server != nil {
		// Serve may not have taken the listener yet, and then Close would miss it
		r.listener.Close()
		r.server.Close()
		r.server, r.listener = nil, nil
	}
}

func (r *rpcHTTP) started() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.server != nil
}
