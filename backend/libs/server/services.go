package server

import (
	"context"
	"fmt"
	"net"
)

type (
	Service struct {
		Opts     ConnectionOpts
		listener Listener
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
	defer l.Close()
	//rand.Seed(time.Now().Unix())

	for {
		c, e := l.Accept()
		if e != nil {
			fmt.Println(e)
			return e
		}
		sc := ss.prepareConnectionHandler(ctx, c)
		go sc.Handle()
	}
}

func (ss *Service) prepareConnectionHandler(ctx context.Context, c net.Conn) (sc *ConnectionHandler) {
	return &ConnectionHandler{
		ctx:      ctx,
		listener: ss.listener,
		conn:     c,
		opts:     ss.Opts,
	}
}
