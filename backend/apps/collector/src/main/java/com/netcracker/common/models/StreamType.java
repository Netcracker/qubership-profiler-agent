package com.netcracker.common.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@JsonFormat(with = JsonFormat.Feature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
public enum StreamType implements IStreamType {
    PARAMS("params", false, ""),
    DICTIONARY("dictionary", false, ""),
    CALLS("calls", true, ""),
    TRACE("trace", true, ""),
    SUSPEND("suspend", true, ""),
    SQL("sql", true, "sql"),
    XML("xml", true, "xml"),
    TOP("top", true, "top.txt"),
    TD("td", true, "td.txt"),
    HEAP("heap", true, "hprof.zip"),
    GC("gc", true, "gc.log");

    private final String name;
    private final boolean rotationRequired;
    private final String fileExtension;

    StreamType(String name, boolean rotationRequired, String fileExtension) {
        this.name = name;
        this.rotationRequired = rotationRequired;
        this.fileExtension = fileExtension;
    }

    @JsonValue
    public String getName() {
        return name;
    }

    public String getFileExtension() {
        return fileExtension;
    }

    public boolean isRotationRequired() {
        return rotationRequired;
    }

    public boolean isMetaStream() {
        switch (this) {
            case PARAMS, DICTIONARY, SUSPEND -> {
                return true;
            }
        }
        return false;
    }

    public boolean isAppendableStat() {
        return PARAMS.equals(this) || DICTIONARY.equals(this);
    }

    @Override
    public String toString() {
        return getName();
    }

    public static Set<String> allStreams() {
        return Arrays.stream(StreamType.values()).map(s -> s.name).collect(Collectors.toSet());
    }

    public static List<StreamType> withRotation() {
        return Arrays.stream(values()).filter(StreamType::isRotationRequired).toList();
    }

    public static boolean isValid(String name) {
        return byName(name) != null;
    }

    @JsonCreator
    public static StreamType byName(String name) {
        try {
            return StreamType.valueOf(name.toUpperCase());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
