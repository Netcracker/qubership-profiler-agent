package com.netcracker.profiler.dump.download;

public class ExecCommandException extends RuntimeException {
    public ExecCommandException() {
    }

    public ExecCommandException(String message) {
        super(message);
    }

    public ExecCommandException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExecCommandException(Throwable cause) {
        super(cause);
    }

    public ExecCommandException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
