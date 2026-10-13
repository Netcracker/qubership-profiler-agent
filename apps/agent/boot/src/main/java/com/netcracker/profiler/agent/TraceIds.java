package com.netcracker.profiler.agent;

/**
 * Records the distributed-tracing identifiers of the call the current thread is profiling.
 *
 * <p>A tracer reports the same span more than once: when it creates the span, when it makes the span
 * current, and again on every thread the context is handed to. Each identifier is therefore kept in
 * {@link CallInfo}, and an event is written only when the value differs from the last one recorded
 * for the call.</p>
 *
 * <p>Nothing is recorded outside a profiled call, because there is no call to attach the events
 * to. The span that is current on the thread is the exception: a tracer makes it current before
 * the call starts whenever it hands a context to another thread, so it is remembered in
 * {@link LocalState}, and each call on the thread records the span it started under.</p>
 */
public final class TraceIds {
    public static final String TRACE_ID = "trace.id";
    public static final String SPAN_ID = "span.id";
    public static final String PARENT_SPAN_ID = "parent.span.id";

    private static final int TRACEPARENT_LENGTH = 55;
    private static final int TRACE_ID_START = 3;
    private static final int TRACE_ID_END = 35;
    private static final int PARENT_ID_START = 36;
    private static final int PARENT_ID_END = 52;
    private static final int B3_SPAN_ID_LENGTH = 16;

    private TraceIds() {
    }

    /**
     * Records a span that runs in this process.
     *
     * @param traceId the trace the span belongs to, or null to leave the trace as it is
     * @param spanId the span, or null to leave the span as it is
     */
    public static void recordSpan(String traceId, String spanId) {
        LocalState state = Profiler.getState();
        if (state.sp == 0) {
            return;
        }
        recordSpanAtCallStart(state);
        recordSpan(state, traceId, spanId);
    }

    /**
     * Records a span whose ID the tracer holds as a number, in the 16-character lowercase hex form
     * that B3 and W3C Trace Context use on the wire.
     *
     * @param traceId the trace the span belongs to, or null to leave the trace as it is
     * @param spanId the span
     */
    public static void recordSpan(String traceId, long spanId) {
        if (Profiler.getState().sp == 0) {
            return;
        }
        recordSpan(traceId, toSpanIdString(spanId));
    }

    /** Returns the ID as 16 lowercase hex characters, zero-padded on the left. */
    static String toSpanIdString(long spanId) {
        char[] chars = new char[16];
        for (int i = 15; i >= 0; i--) {
            chars[i] = Character.forDigit((int) (spanId & 0xf), 16);
            spanId >>>= 4;
        }
        return new String(chars);
    }

    /**
     * Records the span of the caller that propagated its context to this process.
     *
     * @param traceId the trace the caller's span belongs to, or null to leave the trace as it is
     * @param parentSpanId the caller's span, or null to leave the parent as it is
     */
    public static void recordRemoteParent(String traceId, String parentSpanId) {
        LocalState state = Profiler.getState();
        if (state.sp == 0) {
            return;
        }
        recordSpanAtCallStart(state);
        recordRemoteParent(state, traceId, parentSpanId);
    }

    /**
     * Records the span that has just become current on this thread, and remembers it for the calls
     * that start while it stays current.
     *
     * <p>A tracer that reports through this method must report every scope it closes through
     * {@link #restoreCurrentSpan}. Otherwise the span outlives its scope here, and a later call on
     * the thread records a trace it does not belong to.</p>
     *
     * @param traceId the trace the span belongs to, or null when no span is current anymore
     * @param spanId the span, or null when no span is current anymore
     * @param remote true when the span arrived from another process, so it is the parent of the
     *     work on this thread, not a span that runs here
     */
    public static void recordCurrentSpan(String traceId, String spanId, boolean remote) {
        LocalState state = Profiler.getState();
        replaceCurrentSpan(state, traceId, spanId, remote);
        if (state.sp != 0) {
            recordCurrentSpan(state);
        }
    }

    /**
     * Remembers the span that is current on this thread again after a scope was closed. The span
     * is not recorded for the call in progress: the call keeps the last span that became current
     * in it.
     *
     * @param traceId the trace the span belongs to, or null when no span is current anymore
     * @param spanId the span, or null when no span is current anymore
     * @param remote true when the span arrived from another process
     */
    public static void restoreCurrentSpan(String traceId, String spanId, boolean remote) {
        replaceCurrentSpan(Profiler.getState(), traceId, spanId, remote);
    }

    private static void replaceCurrentSpan(LocalState state, String traceId, String spanId, boolean remote) {
        if (state.sp != 0) {
            // The call started under the span that is about to be forgotten
            recordSpanAtCallStart(state);
        }
        state.currentTraceId = traceId;
        state.currentSpanId = spanId;
        state.currentSpanRemote = remote;
    }

    /**
     * Records the span that was current when the call started, unless the call has recorded it
     * already.
     *
     * <p>Nothing announces the start of a call, so the span is recorded on the first of: another
     * identifier is recorded, the current span changes, the call finishes. The remembered span
     * changes only through {@link #replaceCurrentSpan}, which calls this method first, so the span
     * is still the one the call started under.</p>
     *
     * <p>Waiting also keeps a call that is too short to be logged from being logged only because
     * a span was current.</p>
     */
    static void recordSpanAtCallStart(LocalState state) {
        CallInfo callInfo = state.callInfo;
        if (callInfo.spanAtCallStartRecorded) {
            return;
        }
        callInfo.spanAtCallStartRecorded = true;
        recordCurrentSpan(state);
    }

    private static void recordCurrentSpan(LocalState state) {
        if (state.currentTraceId == null) {
            return;
        }
        if (state.currentSpanRemote) {
            recordRemoteParent(state, state.currentTraceId, state.currentSpanId);
        } else {
            recordSpan(state, state.currentTraceId, state.currentSpanId);
        }
    }

    private static void recordSpan(LocalState state, String traceId, String spanId) {
        CallInfo callInfo = state.callInfo;
        recordTraceId(state, traceId);
        if (spanId != null && !spanId.equals(callInfo.getSpanId())) {
            callInfo.setSpanId(spanId);
            event(state, spanId, SPAN_ID);
        }
    }

    private static void recordRemoteParent(LocalState state, String traceId, String parentSpanId) {
        CallInfo callInfo = state.callInfo;
        recordTraceId(state, traceId);
        if (parentSpanId != null && !parentSpanId.equals(callInfo.getParentSpanId())) {
            callInfo.setParentSpanId(parentSpanId);
            event(state, parentSpanId, PARENT_SPAN_ID);
        }
    }

    /**
     * Records the trace and the caller's span from a W3C Trace Context {@code traceparent} header.
     * A missing or malformed header records nothing.
     *
     * @param traceparent the header value, or null when the request carries none
     */
    public static void recordTraceparent(String traceparent) {
        String traceId = traceparentTraceId(traceparent);
        if (traceId != null) {
            recordRemoteParent(traceId, traceparent.trim().substring(PARENT_ID_START, PARENT_ID_END));
        }
    }

    /**
     * Records the trace and the caller's span from the B3 multi-header format that Zipkin defines:
     * {@code X-B3-TraceId} and {@code X-B3-SpanId}. Nothing is recorded unless both are valid.
     *
     * <p>The trace ID is recorded as received, so a 64-bit ID stays 16 characters. A tracer that
     * pads it to 128 bits later records the padded form as a new value.</p>
     *
     * @param traceId the {@code X-B3-TraceId} value, or null when the request carries none
     * @param spanId the {@code X-B3-SpanId} value, or null when the request carries none
     */
    public static void recordB3(String traceId, String spanId) {
        if (traceId == null || spanId == null) {
            return;
        }
        traceId = traceId.trim();
        spanId = spanId.trim();
        if (isB3TraceId(traceId, 0, traceId.length()) && isB3SpanId(spanId, 0, spanId.length())) {
            recordRemoteParent(traceId, spanId);
        }
    }

    /**
     * Records the trace and the caller's span from a B3 single header, {@code b3}. A missing or
     * malformed header records nothing, and so does one that carries only a sampling decision.
     *
     * @param b3 the header value, or null when the request carries none
     */
    public static void recordB3(String b3) {
        String traceId = b3TraceId(b3);
        if (traceId != null) {
            recordRemoteParent(traceId, b3SpanId(b3));
        }
    }

    /**
     * Returns the trace ID of a B3 single header, or null when the header is missing, is malformed,
     * or carries only a sampling decision.
     */
    public static String b3TraceId(String b3) {
        String value = validB3(b3);
        return value == null ? null : value.substring(0, value.indexOf('-'));
    }

    /**
     * Returns the span ID of a B3 single header, or null when the header is missing, is malformed,
     * or carries only a sampling decision.
     */
    static String b3SpanId(String b3) {
        String value = validB3(b3);
        if (value == null) {
            return null;
        }
        int start = value.indexOf('-') + 1;
        return value.substring(start, start + B3_SPAN_ID_LENGTH);
    }

    /**
     * Returns the trimmed header when it has the shape
     * {@code traceid-spanid[-sampling[-parentspanid]]}, and null otherwise.
     *
     * <p>The trace ID is 16 or 32 lowercase hex characters and each span ID is 16. The sampling
     * state is {@code 0}, {@code 1}, or {@code d}. A header that is only a sampling state is valid
     * B3, but it names no trace, so it is rejected here.</p>
     */
    private static String validB3(String b3) {
        if (b3 == null) {
            return null;
        }
        String value = b3.trim();
        int length = value.length();
        int spanStart = value.indexOf('-') + 1;
        int spanEnd = spanStart + B3_SPAN_ID_LENGTH;
        if (spanStart == 0 || !isB3TraceId(value, 0, spanStart - 1)
                || spanEnd > length || !isB3SpanId(value, spanStart, spanEnd)) {
            return null;
        }
        if (spanEnd == length) {
            return value;
        }
        // "-" and the sampling state
        int samplingEnd = spanEnd + 2;
        if (samplingEnd > length || value.charAt(spanEnd) != '-' || "01d".indexOf(value.charAt(spanEnd + 1)) < 0) {
            return null;
        }
        if (samplingEnd == length) {
            return value;
        }
        int parentStart = samplingEnd + 1;
        if (value.charAt(samplingEnd) != '-' || length != parentStart + B3_SPAN_ID_LENGTH
                || !isB3SpanId(value, parentStart, length)) {
            return null;
        }
        return value;
    }

    private static boolean isB3TraceId(String value, int start, int end) {
        int length = end - start;
        return (length == B3_SPAN_ID_LENGTH || length == 2 * B3_SPAN_ID_LENGTH) && isNonZeroHex(value, start, end);
    }

    private static boolean isB3SpanId(String value, int start, int end) {
        return end - start == B3_SPAN_ID_LENGTH && isNonZeroHex(value, start, end);
    }

    /**
     * Returns the {@code trace-id} field of a {@code traceparent} header, or null when the header is
     * missing or is not valid under W3C Trace Context.
     */
    static String traceparentTraceId(String traceparent) {
        String value = validTraceparent(traceparent);
        return value == null ? null : value.substring(TRACE_ID_START, TRACE_ID_END);
    }

    /**
     * Returns the {@code parent-id} field of a {@code traceparent} header, or null when the header is
     * missing or is not valid under W3C Trace Context.
     */
    static String traceparentParentId(String traceparent) {
        String value = validTraceparent(traceparent);
        return value == null ? null : value.substring(PARENT_ID_START, PARENT_ID_END);
    }

    /**
     * Returns the trimmed header when it has the shape {@code version-traceid-parentid-flags}, and
     * null otherwise.
     *
     * <p>Version {@code 00} is exactly 55 characters. A later version may append fields after
     * another dash, and the specification tells a reader to parse the first four as version
     * {@code 00}. Version {@code ff} is forbidden, and so are an all-zero trace ID and an all-zero
     * parent ID.</p>
     */
    private static String validTraceparent(String traceparent) {
        if (traceparent == null) {
            return null;
        }
        String value = traceparent.trim();
        int length = value.length();
        if (length < TRACEPARENT_LENGTH
                || value.charAt(2) != '-'
                || value.charAt(TRACE_ID_END) != '-'
                || value.charAt(PARENT_ID_END) != '-') {
            return null;
        }
        boolean firstVersion = value.charAt(0) == '0' && value.charAt(1) == '0';
        if (firstVersion ? length != TRACEPARENT_LENGTH
                : length > TRACEPARENT_LENGTH && value.charAt(TRACEPARENT_LENGTH) != '-') {
            return null;
        }
        if (value.charAt(0) == 'f' && value.charAt(1) == 'f') {
            return null;
        }
        if (!isHex(value, 0, 2) || !isHex(value, PARENT_ID_END + 1, TRACEPARENT_LENGTH)
                || !isNonZeroHex(value, TRACE_ID_START, TRACE_ID_END)
                || !isNonZeroHex(value, PARENT_ID_START, PARENT_ID_END)) {
            return null;
        }
        return value;
    }

    private static boolean isHex(String value, int start, int end) {
        for (int i = start; i < end; i++) {
            char c = value.charAt(i);
            if ((c < '0' || c > '9') && (c < 'a' || c > 'f')) {
                return false;
            }
        }
        return true;
    }

    private static boolean isNonZeroHex(String value, int start, int end) {
        if (!isHex(value, start, end)) {
            return false;
        }
        for (int i = start; i < end; i++) {
            if (value.charAt(i) != '0') {
                return true;
            }
        }
        return false;
    }

    private static void recordTraceId(LocalState state, String traceId) {
        if (traceId == null) {
            return;
        }
        CallInfo callInfo = state.callInfo;
        if (callInfo.getEndToEndId() == null) {
            callInfo.setEndToEndId(traceId);
        }
        callInfo.setTraceId(traceId);
        if (callInfo.traceIdChanged()) {
            event(state, traceId, TRACE_ID);
        }
    }

    private static void event(LocalState state, String value, String name) {
        int tagId = DumperConstants.DATA_TAG_RECORD | ProfilerData.resolveTag(name);
        if (state.sp == 0) {
            // The call is finishing. LocalState ignores events at this point, so the event goes
            // to the buffer directly, as the CallInfo of the call does.
            state.buffer.event(value, tagId);
        } else {
            state.event(value, tagId);
        }
    }
}
