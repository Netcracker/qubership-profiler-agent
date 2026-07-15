package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

type (
	Service struct {
		Opts     ConnectionOpts
		listener Listener

		mu      sync.Mutex
		tcp     net.Listener
		stopped bool
		conns   sync.WaitGroup
	}
)

func PrepareServer(ctx context.Context, opts ConnectionOpts, listener Listener) (sc *Service) {
	return &Service{
		Opts:     opts,
		listener: listener,
	}
}

// Start listen to incoming connection (call this method in separated goroutine!)
func (ss *Service) Start(ctx context.Context) (err error) {
	var l net.Listener
	l, err = net.Listen("tcp4", fmt.Sprintf(":%d", ss.Opts.ProtocolPort))
	if err != nil {
		fmt.Println(err)
		return err
	}
	ss.mu.Lock()
	if ss.stopped {
		ss.mu.Unlock()
		return l.Close()
	}
	ss.tcp = l
	ss.mu.Unlock()
	defer l.Close()

	for {
		c, e := l.Accept()
		if e != nil {
			ss.mu.Lock()
			stopped := ss.stopped
			ss.mu.Unlock()
			if stopped {
				return nil // Stop closed the listener; not a failure
			}
			fmt.Println(e)
			return e
		}
		sc := ss.prepareConnectionHandler(ctx, c)
		ss.conns.Add(1)
		go func() {
			defer ss.conns.Done()
			sc.Handle()
		}()
	}
}

// Stop closes the TCP listener so Start returns, then waits for the accepted
// connections to finish their teardown (including PodDisconnected), so the
// caller can safely release the resources the listener writes to.
func (ss *Service) Stop() {
	ss.mu.Lock()
	ss.stopped = true
	if ss.tcp != nil {
		_ = ss.tcp.Close()
	}
	ss.mu.Unlock()
	ss.conns.Wait()
}

func (ss *Service) prepareConnectionHandler(ctx context.Context, c net.Conn) (sc *ConnectionHandler) {
	return &ConnectionHandler{
		ctx:        ctx,
		listener:   ss.listener,
		conn:       c,
		opts:       ss.Opts,
		acceptedAt: time.Now(),
	}
}
