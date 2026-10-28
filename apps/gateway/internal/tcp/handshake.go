package tcp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"time"

	profio "github.com/Netcracker/qubership-profiler-agent/libs/wire/io"
	model "github.com/Netcracker/qubership-profiler-agent/libs/wire/protocol"
)

// handshake is the agent's COMMAND_GET_PROTOCOL_VERSION_V2 and every byte
// read from the socket while parsing it.
type handshake struct {
	clientVersion           uint64
	pod, service, namespace string
	raw                     []byte
}

// readHandshake parses the agent's opening command with the same reader the
// collector uses, so the two cannot disagree on the framing. raw holds
// everything read from c, read-ahead included, so a replay of raw followed
// by the rest of c reproduces the agent's stream byte for byte.
func readHandshake(c net.Conn, timeout time.Duration) (*handshake, error) {
	if err := c.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	var rec bytes.Buffer
	r := profio.PrepareTcpReader(io.TeeReader(c, &rec))
	ctx := context.Background()

	cmd, err := r.ReadFixedByte(ctx)
	if err != nil {
		return nil, fmt.Errorf("read the handshake command: %w", err)
	}
	if model.Command(cmd) != model.COMMAND_GET_PROTOCOL_VERSION_V2 {
		return nil, fmt.Errorf("the connection opened with command 0x%02x, not GET_PROTOCOL_VERSION_V2 (0x%02x)",
			cmd, byte(model.COMMAND_GET_PROTOCOL_VERSION_V2))
	}
	h := &handshake{}
	if h.clientVersion, err = r.ReadFixedLong(ctx); err != nil {
		return nil, fmt.Errorf("read the client version: %w", err)
	}
	// The field order is pod, service, namespace, as the agent writes it.
	for _, field := range []*string{&h.pod, &h.service, &h.namespace} {
		if *field, err = r.ReadFixedString(ctx); err != nil {
			return nil, fmt.Errorf("read the handshake identity: %w", err)
		}
	}
	if err := c.SetReadDeadline(time.Time{}); err != nil {
		return nil, err
	}
	h.raw = rec.Bytes()
	return h, nil
}

// replayConn is a connection whose first reads return bytes already taken
// from it. Deadlines and writes go to the socket.
type replayConn struct {
	net.Conn
	r io.Reader
}

func newReplayConn(c net.Conn, prefix []byte) *replayConn {
	return &replayConn{Conn: c, r: io.MultiReader(bytes.NewReader(prefix), c)}
}

func (c *replayConn) Read(p []byte) (int, error) { return c.r.Read(p) }
