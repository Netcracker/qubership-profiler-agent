package io.opentelemetry.javaagent.shaded.io.opentelemetry.api.trace;

/** The part of the relocated {@code SpanContext} the injected code reads. */
public interface SpanContext {
    String getTraceId();

    String getSpanId();

    boolean isValid();

    boolean isRemote();
}
