package io.opencensus.trace;

public class SpanContext {

    public native TraceId getTraceId();
    public native SpanId getSpanId();

}
