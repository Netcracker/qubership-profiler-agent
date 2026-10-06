package io.opentelemetry.api.trace;

public interface SpanContext {
    public String getTraceId();
    public String getSpanId();
}
