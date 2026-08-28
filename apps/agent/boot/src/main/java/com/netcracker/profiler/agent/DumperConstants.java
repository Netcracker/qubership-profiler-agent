package com.netcracker.profiler.agent;

public interface DumperConstants {
    public static final int DATA_TAG_FIELD = 3;
    public static final int DATA_TAG_RECORD = DATA_TAG_FIELD << 24;
    public static final int DATA_EXIT_FIELD = 0;
    public static final int DATA_EXIT_RECORD = DATA_EXIT_FIELD << 24;
    public static final int DATA_ENTER_FIELD = 1;
    public static final int DATA_ENTER_RECORD = DATA_ENTER_FIELD << 24;
    public static final int DATA_TYPE_MASK = 0xff000000;
    public static final int DATA_ID_MASK = 0x00ffffff;

    public static final byte EVENT_EMPTY = -1;
    public static final byte EVENT_ENTER_RECORD = 0;
    public static final byte EVENT_EXIT_RECORD = 1;
    public static final byte EVENT_TAG_RECORD = 2;
    public static final byte EVENT_FINISH_RECORD = 3;
    public static final byte COMMAND_ROTATE_LOG = 1;
    public static final byte COMMAND_FLUSH_LOG = 2;
    public static final byte COMMAND_EXIT = 3;
    public static final byte COMMAND_GET_INFLIGHT_CALLS = 4;
    public static final byte COMMAND_GRACEFUL_SHUTDOWN = 5;

    public static final int TAGS_ROOT = -1;
    public static final int TAGS_HOTSPOTS = -2;
    public static final int TAGS_PARAMETERS = -3;
    public static final int TAGS_CALL_ACTIVE = -4;

    // "java.thread" is not repeated for every call in the calls stream, thus need to allocate special id
    // to pass java.thread from CallToJS to javascript
    public static final int TAGS_JAVA_THREAD = -5;
}
