package com.netcracker.profiler.cloud.transport;

public interface ProtocolConst {
    int DATA_BUFFER_SIZE = 1024;

    int PLAIN_SOCKET_PORT = 1715;

    // TODO: Change this value
    int PLAIN_SOCKET_READ_TIMEOUT = 30000; //10 seconds
    //for some reason if we use 1kb buffers in docker, it is extremely slow. up to 100 ms per read
    int PLAIN_SOCKET_RCV_BUFFER_SIZE = 8*DATA_BUFFER_SIZE;
    int PLAIN_SOCKET_SND_BUFFER_SIZE = 8*DATA_BUFFER_SIZE;
    int PLAIN_SOCKET_BACKLOG = 50;  //how many idle connections are allowed
    long MAX_FLUSH_INTERVAL_MILLIS = 5000;
    long FLUSH_CHECK_INTERVAL_MILLIS = 500;

    //for client to receive data under timeout and respond under timeout
    int MAX_IDLE_BEFORE_DEATH = 2* PLAIN_SOCKET_READ_TIMEOUT + 1000;
    int IO_BUFFER_SIZE = 1024;
    int MAX_PHRASE_SIZE = 10240;

    int COMMAND_INIT_STREAM = 0x01;
    int COMMAND_INIT_STREAM_V2 = 0x15;
    int COMMAND_RCV_DATA = 0x02;
    int COMMAND_CLOSE = 0x04;
    int COMMAND_GET_PROTOCOL_VERSION = 0x08;
    int COMMAND_RESET_STREAM = 0x10;
    int COMMAND_REQUEST_ACK_FLUSH = 0x11;
    int COMMAND_KEEP_ALIVE = 0x12;
    int COMMAND_REPORT_COMMAND_RESULT = 0x13;

    int COMMAND_GET_PROTOCOL_VERSION_V2 = 0x14;

    long PROTOCOL_VERSION = 100505L;
    long PROTOCOL_VERSION_V2 = 100605L;
    long PROTOCOL_VERSION_V3 = 100705L;

    byte ACK_RESPONSE_MAGIC = 'K';
    byte ACK_ERROR_MAGIC = -1;

    byte COMMAND_SUCCESS = 'K';
    byte COMMAND_FAILURE = -1;

    int MAX_STRINGS_IN_LIST = 100;
    boolean ZIPPING_ENABLED = false;

    long BLACK_LISTED_RESP = 88888888;
}
