package com.netcracker.persistence.op;

import io.smallrye.mutiny.Uni;

import java.util.concurrent.Future;

public record UniOperation(Uni<Boolean> op) implements Operation {
    private static final UniOperation EMPTY = new UniOperation(null);

    public static UniOperation fromFuture(Future<Boolean> c) {
        return new UniOperation(Uni.createFrom().future(c));
    }

    public static UniOperation of(Uni<Boolean> c) {
        return new UniOperation(c);
    }

    public static UniOperation empty() {
        return EMPTY;
    }

    @Override
    public boolean isEmpty() {
        return op == null;
    }
}
