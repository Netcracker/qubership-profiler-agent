// Package vdumper is the virtual dumper: a Go behavioral layer that
// reproduces the Java agent's remote-dump pipeline — the DumperThread +
// Dumper + DefaultCollectorClient state machine — for load generation. The
// contract, traced rule by rule to the Java sources, lives in
// backend/docs/design/virtual-dumper.md.
package vdumper

import (
	"bytes"
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/emulator"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator/wire"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/pkg/errors"
)

// ErrBlacklisted reports the collector's BLACK_LISTED_RESP handshake answer;
// like the agent, the virtual dumper stops permanently instead of
// reconnecting.
var ErrBlacklisted = errors.New("collector blacklisted the namespace")

// ErrServerVersion reports a handshake answer the virtual dumper cannot
// drive. In particular PROTOCOL_VERSION_V3 selects the agent's posDictionary
// branch, which never activates against this collector and is deliberately
// not implemented (virtual-dumper.md §2.2).
var ErrServerVersion = errors.New("collector answered an unsupported protocol version")

// errChurn ends a healthy incarnation on purpose (churn mode,
// virtual-dumper.md §1.1): the socket closes abruptly with no COMMAND_CLOSE
// and the pod takes the ordinary reconnect path. Never surfaced outside Run.
var errChurn = errors.New("deliberate churn disconnect")

// VirtualDumper emulates one profiled pod. Run drives the DumperThread
// lifecycle: connect → open streams → pump → on any failure close, wait
// RestartInterval, reconnect with a full dictionary resend.
type VirtualDumper struct {
	cfg     Config
	clock   Clock
	stats   StatsListener
	dict    *dictionary
	streams []*streamState
	// chunks is the bounded dirty-buffer queue between the producer
	// goroutines and the dumper loop; producers drop on overflow (§2.5).
	chunks chan chunk

	traceS, callsS, sqlS, xmlS, dictS, suspendS *streamState
	// callsState is the per-file calls encoder state, reset with every calls
	// file header.
	callsState *wire.CallsFileState
	// sqlCache is the agent's dedup cache (TLimitedLongLongHashMap, 10 000
	// entries): value → (file index, offset) of its first write. Cleared on
	// every initialize and on sql rotation.
	sqlCache map[string]bigRef

	// rnd drives the dumper-side shape (suspend pauses); producers carry
	// their own generators.
	rnd *rand.Rand
	// suspendAt / suspendBacklog account the pause rate over elapsed time.
	suspendAt      time.Time
	suspendBacklog float64

	// startMs mirrors TimerCache.startTime: the process-start epoch written
	// as the trace file header, stable across reconnects.
	startMs int64
	// lastSuspendMs is the suspend stream's header timestamp and the running
	// delta base of its (end, duration) pairs (Dumper.lastSuspendLogEntry).
	lastSuspendMs int64
}

// bigRef is one dedup-cache entry: where a value already lives in the sql
// stream.
type bigRef struct {
	seq    int
	offset int
}

// sqlCacheCap mirrors the agent's SQL_CACHE_SIZE default.
const sqlCacheCap = 10000

// New builds a virtual dumper for one pod; see Config for the knobs.
func New(cfg Config) *VirtualDumper {
	cfg = cfg.withDefaults()
	d := &VirtualDumper{
		cfg:   cfg,
		clock: cfg.Clock,
		stats: cfg.Stats,
		dict:  newDictionary(cfg.DictionaryInitial, cfg.Workload.DictionaryGrowthPerMin),
		rnd:   rand.New(rand.NewSource(cfg.Seed * 31)), //nolint:gosec // load shape, not crypto
	}
	// Stream order and rotation thresholds mirror Dumper.initStreams; the
	// collector refuses anything outside this seven-stream set
	// (virtual-dumper.md §2.2).
	d.streams = []*streamState{
		newStreamState(model.StreamTrace, false, traceRotateSize, false, d.stats),
		newStreamState(model.StreamCalls, false, callsRotateSize, false, d.stats),
		newStreamState(model.StreamXml, false, valueRotateSize, false, d.stats),
		newStreamState(model.StreamSql, false, valueRotateSize, false, d.stats),
		newStreamState(model.StreamDictionary, true, 0, false, d.stats),
		newStreamState(model.StreamSuspend, true, 0, false, d.stats),
		newStreamState(model.StreamParams, true, 0, true, d.stats),
	}
	d.traceS = d.streams[0]
	d.callsS = d.streams[1]
	d.xmlS = d.streams[2]
	d.sqlS = d.streams[3]
	d.dictS = d.streams[4]
	d.suspendS = d.streams[5]
	d.chunks = make(chan chunk, cfg.ChunkQueueSize)
	return d
}

// Run drives the pod until ctx is cancelled (graceful close, nil) or a
// permanent condition stops it (ErrBlacklisted / ErrServerVersion). Every
// other failure enters the agent's reconnect loop: close, sleep
// RestartInterval, re-open all streams, re-send the dictionary from word 0
// with resetRequired=1.
func (d *VirtualDumper) Run(ctx context.Context) error {
	d.startMs = d.clock.Now().UnixMilli()
	d.lastSuspendMs = d.startMs

	// Producers model the application threads: they live for the pod's whole
	// life and keep generating across dumper reconnects (the drop window).
	// The derived cancel stops them when Run exits on a permanent error.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var producers sync.WaitGroup
	for i := 0; i < d.cfg.ThreadsPerPod; i++ {
		p := newProducer(i, d.cfg, d.dict, d.chunks)
		producers.Add(1)
		go func() {
			defer producers.Done()
			p.run(ctx)
		}()
	}
	defer producers.Wait()

	for incarnation := 0; ; incarnation++ {
		if incarnation > 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-d.clock.After(d.cfg.RestartInterval):
			}
		}
		err := d.runIncarnation(ctx, incarnation)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, ErrBlacklisted), errors.Is(err, ErrServerVersion):
			return err
		case errors.Is(err, errChurn):
			// A deliberate cycle, not a failure: it must not pollute the
			// reconnect and ack-error counters a storm run reads real
			// failures from.
			d.stats.Churned(incarnation)
			continue
		}
		if errors.Is(err, emulator.ErrAckRefused) {
			d.stats.AckError()
		}
		d.stats.Disconnected(incarnation, err)
	}
}

func (d *VirtualDumper) runIncarnation(ctx context.Context, incarnation int) error {
	// The connection must outlive ctx cancellation just long enough for the
	// graceful close (flush + COMMAND_CLOSE); WithoutCancel keeps the context
	// values (log level) while detaching the connection from the shutdown.
	ac := emulator.PrepareAgent(context.WithoutCancel(ctx), nil, nil, d.cfg.PodName)
	var t Transport = ac.Prepare(d.cfg.Connection)
	defer func() { _ = t.Close() }()

	dialStart := d.clock.Now()
	if err := t.Connect(); err != nil {
		return err
	}
	d.stats.TcpConnected(d.clock.Now().Sub(dialStart))
	if err := t.InitializeConnection(model.PROTOCOL_VERSION_V3,
		d.cfg.Namespace, d.cfg.Service, d.cfg.PodName); err != nil {
		return err
	}
	switch v := t.ServerVersion(); v {
	case model.PROTOCOL_VERSION_V2:
	case model.BLACK_LISTED_RESP:
		return ErrBlacklisted
	default:
		return errors.Wrapf(ErrServerVersion, "got %d", v)
	}

	// Dumper.initialize: the dictionary counter resets, so the dictionary
	// stream opens with resetRequired=1 and the full word list is re-sent;
	// the sql dedup cache starts empty (dedupParamCache.clear()).
	d.dict.resetSent()
	d.sqlCache = make(map[string]bigRef)
	for _, s := range d.streams {
		s.resetForConnection()
	}
	for _, s := range d.streams {
		if err := d.openStream(t, s); err != nil {
			return err
		}
	}
	d.stats.SessionReady(d.clock.Now().Sub(dialStart))
	d.stats.Connected(incarnation)

	return d.pump(ctx, t)
}

// openStream opens (or rotates) one stream and re-emits its file header
// (virtual-dumper.md §2.2 table).
func (d *VirtualDumper) openStream(t Transport, s *streamState) error {
	reset := s.name == model.StreamDictionary && d.dict.sentNothing()
	if _, err := s.open(t, d.clock.Now(), reset); err != nil {
		return err
	}
	if s == d.sqlS {
		// A rotated sql file invalidates nothing downstream, but the agent
		// clears its dedup cache with the file (bigParamsDedupOs.fileRotated).
		d.sqlCache = make(map[string]bigRef)
	}
	return d.writeStreamHeader(t, s)
}

func (d *VirtualDumper) writeStreamHeader(t Transport, s *streamState) error {
	b := &bytes.Buffer{}
	switch s.name {
	case model.StreamTrace:
		wire.PutFixedLong(b, uint64(d.startMs))
		return s.write(b.Bytes())
	case model.StreamCalls:
		baseMs := d.clock.Now().UnixMilli()
		wire.PutFixedLong(b, uint64(wire.CallsHeaderMagic)<<32|wire.CallsFormatVersion)
		wire.PutFixedLong(b, uint64(baseMs))
		// A fresh file resets the thread table and the time-delta base
		// (CallsCompressedLocalAndRemoteOutputStream.fileRotated).
		d.callsState = wire.NewCallsFileState(baseMs)
		return s.write(b.Bytes())
	case model.StreamSuspend:
		wire.PutFixedLong(b, uint64(d.lastSuspendMs))
		return s.writePhrase(b.Bytes())
	case model.StreamParams:
		// One-shot: the payload goes out at open with its own flush + ack
		// cycle, then the stream idles (Dumper's paramInfoOs.fileRotated).
		b.WriteByte(1) // format version
		for _, p := range d.cfg.Params {
			wire.PutVarString(b, p.Name)
			putBool(b, p.Index)
			putBool(b, p.List)
			wire.PutVarInt(b, uint64(p.Order))
			wire.PutVarString(b, p.Signature)
		}
		if err := s.writePhrase(b.Bytes()); err != nil {
			return err
		}
		if err := s.flushTail(); err != nil {
			return err
		}
		if err := t.Flush(); err != nil {
			return err
		}
		s.closed = true
		return nil
	default: // dictionary, xml, sql carry no file header
		return nil
	}
}

// pump is the dumpLoop mirror: it wakes on incoming producer chunks or the
// flush timer, and on every wake-up serializes what arrived, rotates what
// needs rotation, appends the pending dictionary growth, and — when the 5 s
// interval elapsed — runs the flush cycle.
func (d *VirtualDumper) pump(ctx context.Context, t Transport) error {
	nextFlush := d.clock.Now().Add(d.cfg.FlushInterval)
	// Churn mode: a healthy incarnation lives ChurnInterval (± jitter) past
	// session-ready, then disconnects abruptly — returning errChurn skips
	// gracefulClose, so the deferred socket close is all the collector sees.
	var churnCh <-chan time.Time
	if d.cfg.ChurnInterval > 0 {
		churnCh = d.clock.After(jitterDuration(d.rnd, d.cfg.ChurnInterval, d.cfg.ChurnJitter))
	}
	// flushCh stays armed across chunk wake-ups: re-arming per iteration would
	// allocate one timer per chunk and leak the abandoned ones.
	var flushCh <-chan time.Time
	for {
		if flushCh == nil {
			flushCh = d.clock.After(nextFlush.Sub(d.clock.Now()))
		}
		select {
		case <-ctx.Done():
			return d.gracefulClose(t)
		case <-churnCh:
			return errChurn
		case c := <-d.chunks:
			if err := d.writeChunk(t, c); err != nil {
				return err
			}
		case <-flushCh:
			flushCh = nil
		}
		if err := d.rotateStreams(t); err != nil {
			return err
		}
		d.dict.grow(d.clock.Now())
		if err := d.writeDictionary(); err != nil {
			return err
		}
		if err := d.writeSuspend(); err != nil {
			return err
		}
		if now := d.clock.Now(); !now.Before(nextFlush) {
			if err := d.flushCycle(t); err != nil {
				return err
			}
			for !nextFlush.After(now) {
				nextFlush = nextFlush.Add(d.cfg.FlushInterval)
			}
			// A still-armed timer for the old deadline fires one spurious
			// wake-up at most; the re-arm above picks the new deadline.
		}
	}
}

// writeSuspend appends the stop-the-world pauses the SuspendPerSec rate owes
// for the elapsed wall time: (delta-of-end, duration) varint pairs, one
// phrase each (Dumper.dumpSuspendLog). Pause ends spread evenly over the
// elapsed window, durations are log-uniform 1–200 ms.
func (d *VirtualDumper) writeSuspend() error {
	rate := d.cfg.Workload.SuspendPerSec
	if rate <= 0 {
		return nil
	}
	now := d.clock.Now()
	if d.suspendAt.IsZero() {
		d.suspendAt = now
		return nil
	}
	elapsed := now.Sub(d.suspendAt)
	d.suspendBacklog += elapsed.Seconds() * rate
	d.suspendAt = now
	n := int(d.suspendBacklog)
	if n == 0 {
		return nil
	}
	d.suspendBacklog -= float64(n)
	step := elapsed.Milliseconds() / int64(n)
	for i := 0; i < n; i++ {
		endMs := d.lastSuspendMs + max(step, 1)
		durMs := logUniform(d.rnd, 1, 200)
		b := &bytes.Buffer{}
		wire.PutVarInt(b, uint64(endMs-d.lastSuspendMs))
		wire.PutVarInt(b, uint64(durMs))
		if err := d.suspendS.writePhrase(b.Bytes()); err != nil {
			return err
		}
		d.lastSuspendMs = endMs
	}
	return nil
}

// writeChunk serializes one producer buffer as a logical trace chunk —
// [threadId, startTime] header, events, EVENT_FINISH_RECORD — and emits one
// calls record per root call completed inside it, carrying the
// (file index, buffer offset, record index) linkage into the trace bytes
// (Dumper.writeBufferToFS + writeCall).
func (d *VirtualDumper) writeChunk(_ Transport, c chunk) error {
	bufferOffset := d.traceS.fileOffset
	b := &bytes.Buffer{}
	wire.PutFixedLong(b, c.threadId)
	wire.PutFixedLong(b, uint64(c.startMs))
	for _, op := range c.ops {
		if op.big == nil {
			wire.PutTraceEvent(b, op.ev)
			continue
		}
		ref, err := d.writeBigValue(op.big)
		if err != nil {
			return err
		}
		wire.PutTraceEvent(b, wire.BigTag(op.big.deltaMs, op.big.tagId, op.big.dedup, ref.seq, ref.offset))
	}
	b.WriteByte(wire.EventFinishRecord)
	if err := d.traceS.write(b.Bytes()); err != nil {
		return err
	}
	for _, done := range c.calls {
		rb := &bytes.Buffer{}
		d.callsState.PutRecord(rb, wire.CallRecord{
			StartMs:        done.startMs,
			Method:         done.method,
			DurationMs:     done.durationMs,
			ChildCalls:     done.callCount,
			ThreadName:     c.threadName,
			TraceFileIndex: d.traceS.fileIndex,
			BufferOffset:   bufferOffset,
			RecordIndex:    done.recordIndex,
			Params:         done.params,
			CpuTimeMs:      done.cpuMs,
			WaitTimeMs:     done.waitMs,
			MemoryUsed:     done.memory,
		})
		if err := d.callsS.write(rb.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

// writeBigValue places one big-param value the way Dumper.writeParam does:
// an sql value already in the dedup cache yields its existing (seq, offset)
// reference without a write; everything else is appended to its value stream
// at the current offset.
func (d *VirtualDumper) writeBigValue(op *bigOp) (bigRef, error) {
	if op.dedup {
		if ref, ok := d.sqlCache[op.value]; ok {
			return ref, nil
		}
	}
	s := d.xmlS
	if op.dedup {
		s = d.sqlS
	}
	ref := bigRef{seq: s.fileIndex, offset: s.fileOffset}
	b := &bytes.Buffer{}
	wire.PutVarString(b, op.value)
	if err := s.write(b.Bytes()); err != nil {
		return bigRef{}, err
	}
	if op.dedup {
		if len(d.sqlCache) >= sqlCacheCap {
			// The agent's limited map evicts; dropping the whole cache is the
			// simplest bounded stand-in and only costs re-written values.
			d.sqlCache = make(map[string]bigRef)
		}
		d.sqlCache[op.value] = ref
	}
	return ref, nil
}

// rotateStreams mirrors the rotateIfRequired pass: a rotation flushes the old
// file's tail with a full ack cycle (closing the old rolling file does that in
// the agent), then re-opens the stream and re-emits its header.
func (d *VirtualDumper) rotateStreams(t Transport) error {
	for _, s := range d.streams {
		if !s.needsRotation(d.clock.Now()) {
			continue
		}
		if err := s.flushTail(); err != nil {
			return err
		}
		if err := t.Flush(); err != nil {
			return err
		}
		if err := d.openStream(t, s); err != nil {
			return err
		}
	}
	return nil
}

// writeDictionary appends every not-yet-sent word (Dumper.dumpDictionary):
// one phrase per word, ids implied by order.
func (d *VirtualDumper) writeDictionary() error {
	for _, w := range d.dict.takePending() {
		b := &bytes.Buffer{}
		wire.PutVarString(b, w)
		if err := d.dictS.writePhrase(b.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

// flushCycle mirrors Dumper.flushDumpFile: every open stream flushes its tail
// and runs the client flush (REQUEST_ACK_FLUSH + synchronous ack drain) — one
// ack cycle per stream, so an idle pod still produces the agent's keep-alive
// shape.
func (d *VirtualDumper) flushCycle(t Transport) error {
	for _, s := range d.streams {
		if s.closed {
			continue // params after its one-shot payload
		}
		if err := s.flushTail(); err != nil {
			return err
		}
		drainStart := d.clock.Now()
		if err := t.Flush(); err != nil {
			return err
		}
		d.stats.AckFlushed(s.name, d.clock.Now().Sub(drainStart))
	}
	return nil
}

// gracefulClose mirrors the agent's shutdown hook: steal what the producers
// already handed off, push out everything pending, then announce the close.
// Errors are ignored — the run is over.
func (d *VirtualDumper) gracefulClose(t Transport) error {
	for {
		select {
		case c := <-d.chunks:
			_ = d.writeChunk(t, c)
			continue
		default:
		}
		break
	}
	_ = d.writeDictionary()
	_ = d.flushCycle(t)
	_ = t.CommandClose()
	return nil
}

func (d *VirtualDumper) streamByName(name string) *streamState {
	for _, s := range d.streams {
		if s.name == name {
			return s
		}
	}
	return nil
}

func putBool(b *bytes.Buffer, v bool) {
	if v {
		b.WriteByte(1)
		return
	}
	b.WriteByte(0)
}
