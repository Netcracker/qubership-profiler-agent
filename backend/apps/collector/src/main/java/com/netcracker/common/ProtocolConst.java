package com.netcracker.common;

public interface ProtocolConst {
    int PLAIN_SOCKET_PORT = 1715;

    int DATA_BUFFER_SIZE = 1024;
    //for some reason if we use 1kb buffers in docker, it is extremely slow. up to 100 ms per read
    int PLAIN_SOCKET_RCV_BUFFER_SIZE = 8*DATA_BUFFER_SIZE;
    int PLAIN_SOCKET_SND_BUFFER_SIZE = 8*DATA_BUFFER_SIZE;

    int PLAIN_SOCKET_READ_TIMEOUT = 30000; // 30 seconds
    int PLAIN_SOCKET_BACKLOG = 50;  //how many idle connections are allowed
    long MAX_FLUSH_INTERVAL_MILLIS = 15000;
    long FLUSH_CHECK_INTERVAL_MILLIS = 500;

    //for client to receive data under timeout and respond under timeout
    int MAX_IDLE_BEFORE_DEATH = 2* PLAIN_SOCKET_READ_TIMEOUT + 1000;
    int IO_BUFFER_SIZE = 1024;
    int MAX_PHRASE_SIZE = 10240;

    byte COMMAND_INIT_STREAM = 0x01;
    byte COMMAND_INIT_STREAM_V2 = 0x15;
    byte COMMAND_RCV_DATA = 0x02;
    byte COMMAND_CLOSE = 0x04;
    byte COMMAND_GET_PROTOCOL_VERSION = 0x08;
    byte COMMAND_GET_PROTOCOL_VERSION_V2 = 0x14;
    byte COMMAND_RESET_STREAM = 0x10;
    byte COMMAND_REQUEST_ACK_FLUSH = 0x11;
    byte COMMAND_KEEP_ALIVE = 0x12;
    byte COMMAND_REPORT_COMMAND_RESULT = 0x13;

    long PROTOCOL_VERSION = 100505L;
    long PROTOCOL_VERSION_V2 = 100605L;

    byte ACK_RESPONSE_MAGIC = 'K';
    byte ACK_ERROR_MAGIC = -1;

    byte COMMAND_SUCCESS = 'K';
    byte COMMAND_FAILURE = -1;

}
