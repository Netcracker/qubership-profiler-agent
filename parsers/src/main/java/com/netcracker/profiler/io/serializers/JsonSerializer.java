package com.netcracker.profiler.io.serializers;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

public interface JsonSerializer<T> {
    public void serialize(T value, JsonGenerator gen) throws IOException;
}
