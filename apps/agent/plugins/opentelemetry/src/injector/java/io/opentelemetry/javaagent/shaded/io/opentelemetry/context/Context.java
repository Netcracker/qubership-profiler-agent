package io.opentelemetry.javaagent.shaded.io.opentelemetry.context;

/** The part of the relocated {@code Context} the injected code reads. */
public interface Context {
    static Context current() {
        throw new UnsupportedOperationException("stub");
    }
}
