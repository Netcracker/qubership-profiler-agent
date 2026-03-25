package com.netcracker.common.models;

public interface Sizeable {
    int BOOLEAN_MAX_LENGTH = 5;
    int INTEGER_MAX_LENGTH = 10; // yyyy-mm-ddT|HH:MM:SS.fff+NNNN - 29 chars for timestamp
    int TIMESTAMP_MAX_LENGTH = 29;
    int TWO_BRACKETS_COMMA_AND_NEWLINE = 4;

    int getSize();
}
