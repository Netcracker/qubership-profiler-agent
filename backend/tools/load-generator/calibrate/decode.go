package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"net"
	"os"
	"sync"
	"sync/atomic"

	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
)

// Profile is the JSON traffic profile of one tap run.
type Profile struct {
	Target      string         `json:"target"`
	Connections []*ConnProfile `json:"connections"`
}

// ConnProfile is one agent connection as seen on the wire.
type ConnProfile struct {
	StartMs   int64  `json:"start_ms"`
	EndMs     int64  `json:"end_ms"`
	Pod       string `json:"pod"`
	Service   string `json:"service"`
	Namespace string `json:"namespace"`

	// Inits records every INIT_STREAM_V2 in arrival order.
	Inits []InitEvent `json:"inits"`
	// Streams aggregates RCV_DATA per stream, with 1 s buckets since the
	// connection start.
	Streams map[string]*StreamStats `json:"streams"`
	// RcvSizeLog2 is a histogram of RCV_DATA payload sizes: index i counts
	// payloads in [2^i, 2^(i+1)).
	RcvSizeLog2 [12]int64 `json:"rcv_size_log2"`
	// AckFlushSec counts REQUEST_ACK_FLUSH commands per second-since-start —
	// the agent's flush cadence.
	AckFlushSec map[int]int `json:"ack_flush_sec"`
	Acks        int64       `json:"acks"`
	AckErrors   int64       `json:"ack_errors"`
	Injected    bool        `json:"injected_ack_error,omitempty"`
	CloseSent   bool        `json:"close_sent"`
	ParseError  string      `json:"parse_error,omitempty"`
}

// InitEvent is one stream open.
type InitEvent struct {
	AtMs   int64  `json:"at_ms"` // since connection start
	Stream string `json:"stream"`
	SeqId  int    `json:"seq_id"`
	Reset  bool   `json:"reset"`
}

// StreamStats aggregates one stream's RCV_DATA traffic.
type StreamStats struct {
	Bytes    int64         `json:"bytes"`
	Rcv      int64         `json:"rcv"`
	BytesSec map[int]int64 `json:"bytes_sec"`
}

type tap struct {
	target    string
	injectAck int

	mu      sync.Mutex
	conns   []*ConnProfile
	ackSeen atomic.Int64
}

func newTap(target string, injectAck int) *tap {
	return &tap{target: target, injectAck: injectAck}
}

func (t *tap) profile() *Profile {
	t.mu.Lock()
	defer t.mu.Unlock()
	return &Profile{Target: t.target, Connections: t.conns}
}

// expected response tokens, pushed by the agent-side parser and consumed in
// order by the collector-side parser — the protocol's replies are strictly
// ordered by the commands that caused them.
const (
	tokHandshake = iota
	tokInitReply
	tokAck
)

func (t *tap) handle(ctx context.Context, agent net.Conn) {
	defer func() { _ = agent.Close() }()
	collector, err := net.Dial("tcp", t.target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "calibrate: dial %s: %v\n", t.target, err)
		return
	}
	defer func() { _ = collector.Close() }()

	cp := &ConnProfile{
		StartMs:     nowMs(),
		Streams:     map[string]*StreamStats{},
		AckFlushSec: map[int]int{},
	}
	t.mu.Lock()
	t.conns = append(t.conns, cp)
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		cp.EndMs = nowMs()
		t.mu.Unlock()
	}()

	go func() {
		<-ctx.Done()
		_ = agent.Close()
		_ = collector.Close()
	}()

	expect := make(chan int, 1<<16)
	handles := &sync.Map{} // handle uuid (string) -> stream name
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer func() { _ = collector.(*net.TCPConn).CloseWrite() }()
		defer close(expect)
		if err := t.parseAgentSide(cp, agent, collector, expect, handles); err != nil && err != io.EOF {
			t.recordErr(cp, "agent side: "+err.Error())
		}
	}()
	go func() {
		defer wg.Done()
		defer func() { _ = agent.(*net.TCPConn).CloseWrite() }()
		if err := t.parseCollectorSide(cp, collector, agent, expect, handles); err != nil && err != io.EOF {
			t.recordErr(cp, "collector side: "+err.Error())
		}
	}()
	wg.Wait()
}

func (t *tap) recordErr(cp *ConnProfile, msg string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if cp.ParseError == "" {
		cp.ParseError = msg
	}
}

// parseAgentSide decodes the agent's command stream while forwarding it
// byte-exact: the parser reads through a TeeReader into the collector.
func (t *tap) parseAgentSide(cp *ConnProfile, agent, collector net.Conn,
	expect chan<- int, handles *sync.Map) error {

	r := bufio.NewReaderSize(io.TeeReader(agent, collector), 64*1024)
	for {
		cmd, err := r.ReadByte()
		if err != nil {
			return err
		}
		switch model.Command(cmd) {
		case model.COMMAND_GET_PROTOCOL_VERSION_V2:
			if _, err := readN(r, 8); err != nil { // client version
				return err
			}
			pod, err := readString(r)
			if err != nil {
				return err
			}
			service, err := readString(r)
			if err != nil {
				return err
			}
			namespace, err := readString(r)
			if err != nil {
				return err
			}
			t.mu.Lock()
			cp.Pod, cp.Service, cp.Namespace = pod, service, namespace
			t.mu.Unlock()
			expect <- tokHandshake

		case model.COMMAND_INIT_STREAM_V2:
			stream, err := readString(r)
			if err != nil {
				return err
			}
			seqBuf, err := readN(r, 4)
			if err != nil {
				return err
			}
			resetBuf, err := readN(r, 4)
			if err != nil {
				return err
			}
			t.mu.Lock()
			cp.Inits = append(cp.Inits, InitEvent{
				AtMs:   nowMs() - cp.StartMs,
				Stream: stream,
				SeqId:  int(binary.BigEndian.Uint32(seqBuf)),
				Reset:  binary.BigEndian.Uint32(resetBuf) == 1,
			})
			t.mu.Unlock()
			handles.Store("pending-init", stream)
			expect <- tokInitReply

		case model.COMMAND_RCV_DATA:
			handle, err := readN(r, 16)
			if err != nil {
				return err
			}
			payload, err := readString(r) // fixed buf: length + bytes
			if err != nil {
				return err
			}
			stream := "?"
			if v, ok := handles.Load(string(handle)); ok {
				stream = v.(string)
			}
			t.mu.Lock()
			st := cp.Streams[stream]
			if st == nil {
				st = &StreamStats{BytesSec: map[int]int64{}}
				cp.Streams[stream] = st
			}
			st.Bytes += int64(len(payload))
			st.Rcv++
			st.BytesSec[int((nowMs()-cp.StartMs)/1000)] += int64(len(payload))
			if len(payload) > 0 {
				cp.RcvSizeLog2[min(bits.Len(uint(len(payload)))-1, 11)]++
			}
			t.mu.Unlock()
			expect <- tokAck

		case model.COMMAND_REQUEST_ACK_FLUSH:
			t.mu.Lock()
			cp.AckFlushSec[int((nowMs()-cp.StartMs)/1000)]++
			t.mu.Unlock()
			expect <- tokAck

		case model.COMMAND_REPORT_COMMAND_RESULT:
			if _, err := readN(r, 17); err != nil { // uuid + result byte
				return err
			}

		case model.COMMAND_CLOSE:
			t.mu.Lock()
			cp.CloseSent = true
			t.mu.Unlock()

		default:
			return fmt.Errorf("unknown agent command 0x%02X", cmd)
		}
	}
}

// parseCollectorSide decodes the collector's replies, consuming the expected
// tokens in order, and forwards them — except an optionally injected
// ACK_ERROR_MAGIC replacing the real ack byte.
func (t *tap) parseCollectorSide(cp *ConnProfile, collector, agent net.Conn,
	expect <-chan int, handles *sync.Map) error {

	r := bufio.NewReaderSize(collector, 64*1024)
	w := agent
	for tok := range expect {
		switch tok {
		case tokHandshake:
			b, err := readN(r, 8)
			if err != nil {
				return err
			}
			if _, err := w.Write(b); err != nil {
				return err
			}

		case tokInitReply:
			b, err := readN(r, 16+8+8+4)
			if err != nil {
				return err
			}
			if v, ok := handles.LoadAndDelete("pending-init"); ok {
				handles.Store(string(b[:16]), v.(string))
			}
			if _, err := w.Write(b); err != nil {
				return err
			}

		case tokAck:
			ack, err := r.ReadByte()
			if err != nil {
				return err
			}
			n := t.ackSeen.Add(1)
			if t.injectAck > 0 && n == int64(t.injectAck) {
				t.mu.Lock()
				cp.Injected = true
				t.mu.Unlock()
				if _, err := w.Write([]byte{model.ACK_ERROR_MAGIC}); err != nil {
					return err
				}
				return io.EOF // the agent reconnects; this connection is done
			}
			t.mu.Lock()
			cp.Acks++
			if ack == model.ACK_ERROR_MAGIC {
				cp.AckErrors++
			}
			t.mu.Unlock()
			if _, err := w.Write([]byte{ack}); err != nil {
				return err
			}
			if ack != model.ACK_ERROR_MAGIC && ack > 0 && ack < 0x80 {
				// Piggybacked diagnostic commands: uuid + string, forwarded.
				for i := byte(0); i < ack; i++ {
					b, err := readN(r, 16)
					if err != nil {
						return err
					}
					if _, err := w.Write(b); err != nil {
						return err
					}
					s, err := readStringRaw(r, w)
					if err != nil {
						return err
					}
					_ = s
				}
			}
		}
	}
	return nil
}

func readN(r *bufio.Reader, n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}

// readString reads a fixed-int length plus that many raw bytes (the protocol's
// fixed string / fixed buf framing).
func readString(r *bufio.Reader) (string, error) {
	lb, err := readN(r, 4)
	if err != nil {
		return "", err
	}
	n := binary.BigEndian.Uint32(lb)
	if n > 1<<20 {
		return "", fmt.Errorf("implausible field length %d", n)
	}
	b, err := readN(r, int(n))
	return string(b), err
}

// readStringRaw reads a fixed string while forwarding its bytes.
func readStringRaw(r *bufio.Reader, w io.Writer) (string, error) {
	lb, err := readN(r, 4)
	if err != nil {
		return "", err
	}
	if _, err := w.Write(lb); err != nil {
		return "", err
	}
	n := binary.BigEndian.Uint32(lb)
	if n > 1<<20 {
		return "", fmt.Errorf("implausible field length %d", n)
	}
	b, err := readN(r, int(n))
	if err != nil {
		return "", err
	}
	if _, err := w.Write(b); err != nil {
		return "", err
	}
	return string(b), nil
}
