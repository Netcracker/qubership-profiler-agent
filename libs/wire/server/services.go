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
		active  map[*ConnectionHandler]struct{}
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
		ss.mu.Lock()
		if ss.active == nil {
			ss.active = map[*ConnectionHandler]struct{}{}
		}
		ss.active[sc] = struct{}{}
		ss.mu.Unlock()
		ss.conns.Add(1)
		go func() {
			defer ss.conns.Done()
			defer func() {
				ss.mu.Lock()
				delete(ss.active, sc)
				ss.mu.Unlock()
			}()
			sc.Handle()
		}()
	}
}

// Stop closes the TCP listener so Start returns, closes every active agent
// connection, and waits for their teardown (including PodDisconnected), so
// the caller can safely release the resources the listener writes to. The
// connections are closed, not drained: an idle agent would otherwise hold
// Stop until its read deadline (tens of seconds), blowing the 03 §5.4
// shutdown budget — the agent treats the abrupt close like a crash and
// reconnects to another replica as a fresh pod-restart (03 §5.2).
func (ss *Service) Stop() {
	ss.mu.Lock()
	ss.stopped = true
	if ss.tcp != nil {
		_ = ss.tcp.Close()
	}
	active := make([]*ConnectionHandler, 0, len(ss.active))
	for sc := range ss.active {
		active = append(active, sc)
	}
	ss.mu.Unlock()
	for _, sc := range active {
		_ = sc.Close()
	}
	ss.conns.Wait()
}

// Addr reports the bound listener address, nil before Start binds it; tests
// dial it when the configured port is 0.
func (ss *Service) Addr() net.Addr {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.tcp == nil {
		return nil
	}
	return ss.tcp.Addr()
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
