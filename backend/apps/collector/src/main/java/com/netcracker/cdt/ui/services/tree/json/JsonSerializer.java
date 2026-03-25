package com.netcracker.cdt.ui.services.tree.json;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

public interface JsonSerializer<T> {
    void serialize(T value, JsonGenerator gen) throws IOException;
}
