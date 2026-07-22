// Package emutest provides a scripted in-process collector for transport and
// virtual-dumper tests. It speaks the server side of the agent wire protocol
// (06-wire-protocol-server.md) faithfully enough to exercise the client state
// machine, and lets a test shape every acknowledgement: delay it, drop it,
// refuse with ACK_ERROR_MAGIC, or piggyback diagnostic commands.
//
// By default acks are flushed as soon as they are written, matching the 06 §5
// requirement that the collector flush on its own cadence well under the
// agent's read timeout. Set BufferAcks to withhold them until the next
// REQUEST_ACK_FLUSH, which mirrors a collector that only force-flushes.
package emutest

import (
	"net"
	"sync"
	"testing"
	"time"

	"context"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	profio "github.com/Netcracker/qubership-profiler-backend/libs/io"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
)

type (
	// Collector is the scripted collector double. Configure the exported
	// fields before the first agent connects; they must not change afterwards.
	Collector struct {
		t  testing.TB
		ln net.Listener

		// BufferAcks withholds RCV_DATA acks until the next REQUEST_ACK_FLUSH
		// (or a refusal), instead of flushing each ack immediately.
		BufferAcks bool
		// HandshakeVersion overrides the protocol version the collector
		// answers; nil answers PROTOCOL_VERSION_V2.
		HandshakeVersion func(clientVersion uint64) uint64
		// AckOf decides the acknowledgement for the ackSeq-th acked command of
		// a connection (RCV_DATA and REQUEST_ACK_FLUSH counted together,
		// starting at 0); nil acks everything with ACK_OK.
		AckOf func(conn, ackSeq int) Ack
		// InitReplyOf overrides the INIT_STREAM_V2 reply per stream; nil
		// echoes the requested sequence id with no rotation limits.
		InitReplyOf func(conn int, stream string) InitReply

		mu     sync.Mutex
		events []Event
		conns  int
	}

	// Ack scripts one acknowledgement.
	Ack struct {
		Refuse   bool          // answer ACK_ERROR_MAGIC (flushed) and close the connection
		Drop     bool          // answer nothing, to exercise the agent's timeout path
		Delay    time.Duration // wait before answering
		Commands []Piggyback   // diagnostic commands; the ack byte carries len(Commands)
	}

	// Piggyback is one diagnostic command sent after a positive ack byte.
	Piggyback struct {
		Id   common.Uuid
		Text string
	}

	// InitReply scripts one INIT_STREAM_V2 response.
	InitReply struct {
		NullHandle           bool // refuse the stream: null handle, then close
		RotationPeriodMs     uint64
		RequiredRotationSize uint64
		// SeqId answers this rolling sequence id when non-nil; nil echoes the
		// requested one, like the real collector.
		SeqId *int
	}

	// Event is one recorded protocol command, in arrival order across all
	// connections.
	Event struct {
		Conn    int
		Command model.Command
		// GET_PROTOCOL_VERSION_V2
		Pod, Service, Namespace string
		// INIT_STREAM_V2
		Stream string // also set on RCV_DATA, resolved through the handle
		SeqId  int
		Reset  bool
		// RCV_DATA
		Payload []byte
		// REPORT_COMMAND_RESULT
		CommandId common.Uuid
		Success   bool
		At        time.Time
	}
)

// Start launches the collector double on a loopback port and registers its
// shutdown with t.Cleanup.
func Start(t testing.TB) *Collector {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("emutest: listen: %v", err)
	}
	c := &Collector{t: t, ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go c.acceptLoop()
	return c
}

// Addr returns the host:port agents should dial.
func (c *Collector) Addr() string {
	return c.ln.Addr().String()
}

// Events returns a snapshot of every recorded command, in arrival order.
func (c *Collector) Events() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Event, len(c.events))
	copy(out, c.events)
	return out
}

// EventsOf returns the snapshot filtered to one command type.
func (c *Collector) EventsOf(cmd model.Command) []Event {
	var out []Event
	for _, e := range c.Events() {
		if e.Command == cmd {
			out = append(out, e)
		}
	}
	return out
}

// Connections reports how many agent connections have been accepted.
func (c *Collector) Connections() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conns
}

// StreamData concatenates the RCV_DATA payloads of one stream on one
// connection, in arrival order.
func (c *Collector) StreamData(conn int, stream string) []byte {
	var out []byte
	for _, e := range c.Events() {
		if e.Conn == conn && e.Command == model.COMMAND_RCV_DATA && e.Stream == stream {
			out = append(out, e.Payload...)
		}
	}
	return out
}

func (c *Collector) record(e Event) {
	e.At = time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
}

func (c *Collector) acceptLoop() {
	for {
		conn, err := c.ln.Accept()
		if err != nil {
			return // listener closed by t.Cleanup
		}
		c.mu.Lock()
		idx := c.conns
		c.conns++
		c.mu.Unlock()
		go c.handle(conn, idx)
	}
}

// handle mirrors the real server's per-connection command loop
// (libs/server.ConnectionHandler.HandleCommand), recording instead of storing.
func (c *Collector) handle(conn net.Conn, idx int) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(time.Minute))
	ctx := context.Background()
	r := profio.PrepareTcpReader(conn)
	w := profio.PrepareTcpWriter(conn)
	handles := map[common.UUID]string{}
	ackSeq := 0

	for {
		b, err := r.ReadFixedByte(ctx)
		if err != nil {
			return // connection closed by the agent or the deadline
		}
		switch model.Command(b) {
		case model.COMMAND_GET_PROTOCOL_VERSION_V2:
			clientVersion, _ := r.ReadFixedLong(ctx)
			pod, _ := r.ReadFixedString(ctx)
			service, _ := r.ReadFixedString(ctx)
			namespace, _ := r.ReadFixedString(ctx)
			if r.EOF() {
				return
			}
			c.record(Event{Conn: idx, Command: model.COMMAND_GET_PROTOCOL_VERSION_V2,
				Pod: pod, Service: service, Namespace: namespace})
			reply := model.PROTOCOL_VERSION_V2
			if c.HandshakeVersion != nil {
				reply = c.HandshakeVersion(clientVersion)
			}
			if c.fail(w.WriteFixedLong(ctx, reply)) || c.fail(w.Flush()) {
				return
			}

		case model.COMMAND_INIT_STREAM_V2:
			stream, _ := r.ReadFixedString(ctx)
			seqId, _ := r.ReadFixedInt(ctx)
			reset, _ := r.ReadFixedInt(ctx)
			if r.EOF() {
				return
			}
			c.record(Event{Conn: idx, Command: model.COMMAND_INIT_STREAM_V2,
				Stream: stream, SeqId: seqId, Reset: reset == 1})
			reply := InitReply{}
			if c.InitReplyOf != nil {
				reply = c.InitReplyOf(idx, stream)
			}
			if reply.NullHandle {
				_ = w.WriteUuid(ctx, common.Uuid{})
				_ = w.Flush()
				return
			}
			handle := common.RandomUuid()
			handles[handle.ToBin()] = stream
			replySeq := seqId
			if reply.SeqId != nil {
				replySeq = *reply.SeqId
			}
			if c.fail(w.WriteUuid(ctx, handle)) ||
				c.fail(w.WriteFixedLong(ctx, reply.RotationPeriodMs)) ||
				c.fail(w.WriteFixedLong(ctx, reply.RequiredRotationSize)) ||
				c.fail(w.WriteFixedInt(ctx, replySeq)) ||
				c.fail(w.Flush()) {
				return
			}

		case model.COMMAND_RCV_DATA:
			handle, _ := r.ReadUuid(ctx)
			payload, _ := r.ReadFixedString(ctx)
			if r.EOF() {
				return
			}
			c.record(Event{Conn: idx, Command: model.COMMAND_RCV_DATA,
				Stream: handles[handle.ToBin()], Payload: []byte(payload)})
			if !c.writeAck(ctx, w, idx, &ackSeq, false) {
				return
			}

		case model.COMMAND_REQUEST_ACK_FLUSH:
			c.record(Event{Conn: idx, Command: model.COMMAND_REQUEST_ACK_FLUSH})
			if !c.writeAck(ctx, w, idx, &ackSeq, true) {
				return
			}

		case model.COMMAND_REPORT_COMMAND_RESULT:
			id, _ := r.ReadUuid(ctx)
			success, _ := r.ReadFixedByte(ctx)
			if r.EOF() {
				return
			}
			c.record(Event{Conn: idx, Command: model.COMMAND_REPORT_COMMAND_RESULT,
				CommandId: id, Success: success == 'K'})

		case model.COMMAND_CLOSE:
			c.record(Event{Conn: idx, Command: model.COMMAND_CLOSE})
			return

		default:
			c.t.Errorf("emutest: unknown command byte 0x%02X on conn %d", b, idx)
			_ = w.WriteFixedByte(ctx, model.ACK_ERROR_MAGIC)
			_ = w.Flush()
			return
		}
	}
}

// writeAck answers one ackable command (RCV_DATA or REQUEST_ACK_FLUSH) per the
// AckOf script. It reports false when the connection must close.
func (c *Collector) writeAck(ctx context.Context, w *profio.TcpWriter, conn int, ackSeq *int, force bool) bool {
	ack := Ack{}
	if c.AckOf != nil {
		ack = c.AckOf(conn, *ackSeq)
	}
	*ackSeq++
	if ack.Delay > 0 {
		time.Sleep(ack.Delay)
	}
	switch {
	case ack.Drop:
		return true
	case ack.Refuse:
		// The real server flushes an error ack and tears the connection down
		// (06 §6).
		_ = w.WriteFixedByte(ctx, model.ACK_ERROR_MAGIC)
		_ = w.Flush()
		return false
	}
	if c.fail(w.WriteFixedByte(ctx, byte(len(ack.Commands)))) {
		return false
	}
	for _, cmd := range ack.Commands {
		if c.fail(w.WriteUuid(ctx, cmd.Id)) || c.fail(w.WriteFixedString(ctx, cmd.Text)) {
			return false
		}
	}
	if force || !c.BufferAcks || len(ack.Commands) > 0 {
		if c.fail(w.Flush()) {
			return false
		}
	}
	return true
}

func (c *Collector) fail(err error) bool {
	if err != nil {
		c.t.Logf("emutest: write: %v", err)
		return true
	}
	return false
}
