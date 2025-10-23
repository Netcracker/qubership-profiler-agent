package com.netcracker.persistence.op;

public sealed interface Operation permits UniOperation, EmptyOperation {

    boolean isEmpty();

    static Operation empty() {
        return EMPTY;
    }

    Operation EMPTY = new EmptyOperation();

}
