package com.netcracker.profiler.parsers.exception;

public class ParserException extends Exception{
    public ParserException() {
        super();
    }

    public ParserException(String message) {
        super(message);
    }

    public ParserException(String message, Throwable cause) {
        super(message, cause);
    }
}
