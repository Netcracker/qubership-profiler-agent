package io.opentelemetry.javaagent.shaded.io.opentelemetry.api.trace;

import io.opentelemetry.javaagent.shaded.io.opentelemetry.context.Context;

/** The part of the relocated {@code Span} the injected code reads. */
public interface Span {
    static Span fromContextOrNull(Context context) {
        throw new UnsupportedOperationException("stub");
    }

    SpanContext getSpanContext();
}
