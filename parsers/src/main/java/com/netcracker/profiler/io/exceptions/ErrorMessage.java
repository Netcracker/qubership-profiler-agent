package com.netcracker.profiler.io.exceptions;


import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

public class ErrorMessage {
    public final Level level;
    public final String message;
    public final Throwable exception;

    public ErrorMessage(Level level, String message, Throwable exception) {
        this.level = level;
        this.message = message;
        this.exception = exception;
    }

    public void toJson(JsonGenerator jgen) throws IOException {
        jgen.writeRaw("alert(");
        jgen.writeString(level.toString());
        jgen.writeRaw("+");
        jgen.writeString(": ");
        jgen.writeRaw("+");
        jgen.writeString(message);
        if(exception != null) {
            jgen.writeRaw("+");
            jgen.writeString(",\n");
            jgen.writeRaw("+");
            jgen.writeString(exception.getMessage());
        }
        jgen.writeRaw(");\n");
    }
}
