package com.netcracker.cdt.collector.common;

import java.util.UUID;

public class StreamNotInitializedException extends RuntimeException{
    UUID uuid;
    public StreamNotInitializedException(UUID uuid) {
        super("Stream with UUID " + uuid + " is not registered.");
        this.uuid = uuid;
    }

    public UUID getUuid() {
        return uuid;
    }
}
