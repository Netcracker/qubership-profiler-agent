package com.netcracker.persistence.op;

final class EmptyOperation implements Operation {

    @Override
    public boolean isEmpty() {
        return true;
    }
}
