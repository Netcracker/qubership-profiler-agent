package com.netcracker.profiler.test.pigs;

public class TransactionPig extends Exception {
    int status;
    Throwable rollbackReason;

    public TransactionPig(int status, Throwable rollbackReason) {
        super("Obscure exception status " + status);
        this.status = status;
        this.rollbackReason = rollbackReason;
    }

    public final Throwable getRollbackReason() {
        return rollbackReason;
    }

    public int getStatus() {
        return status;
    }

    public String getStatusAsString() {
        return "" + status;
    }
}
