package com.netcracker.profiler.sax.io;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.netcracker.utils.Utils.byteStream;
import static org.junit.jupiter.api.Assertions.*;

class DataInputStreamExTest {

    @Test
    void testReadData() throws IOException {

        try (var is = byteStream(0, 0x63, 0, 0x61)) {
            assertEquals(0, is.position());
            assertEquals(4, is.available());

            assertEquals('c', is.readChar());
            assertEquals(2, is.position());
            assertEquals(2, is.available());

            assertEquals('a', is.readChar());
            assertEquals(4, is.position());
            assertEquals(0, is.available());
        }

        try (var is = byteStream(0, 11, 0, 0, 1, 114)) {
            assertEquals(0, is.position());
            assertEquals(6, is.available());
            assertEquals(11, is.readShort());
            assertEquals(2, is.position());
            assertEquals(4, is.available());
        }
        try (var is = byteStream(0, 0, 0, 11, 0, 0, 1, 114)) {
            assertEquals(0, is.position());
            assertEquals(8, is.available());
            assertEquals(11, is.readInt());
            assertEquals(4, is.position());
            assertEquals(4, is.available());
        }
        try (var is = byteStream(0, 0, 0, 11, 0, 0, 1, 114)) {
            assertEquals(0, is.position());
            assertEquals(8, is.available());
            assertEquals(47244640626L, is.readLong());
            assertEquals(8, is.position());
            assertEquals(0, is.available());
        }

        try (var is = byteStream(0x03, 0x00, 0x03, 0x00)) {
            assertEquals(0, is.position());
            assertEquals(4, is.available());
            assertEquals(3, is.readVarInt()); // varint(3) takes one byte
            assertEquals(1, is.position());
            assertEquals(3, is.available());
        }
        try (var is = byteStream(0x03, 0x00, 0x03, 0x00)) {
            assertEquals(0, is.position());
            assertEquals(4, is.available());
            assertEquals(3, is.readVarLong()); // varlong(3) takes one byte too
            assertEquals(1, is.position());
            assertEquals(3, is.available());
        }

        try (var is = byteStream(0x05, 0, 0x63, 0, 0x61, 0, 0x6c, 0, 0x2e, 0, 0x6c, 1)) {
            assertEquals(0, is.position());
            assertEquals(12, is.available());
            assertEquals("cal.l", is.readString());
            assertEquals(11, is.position());
            assertEquals(1, is.available());
        }
        try (var is = byteStream(0x05, 0, 0x63, 0, 0x61, 0, 0x6c, 0, 0x2e, 0, 0x6c, 1)) {
            assertEquals(0, is.position());
            assertEquals(12, is.available());
            is.skipString();
            assertEquals(11, is.position());
            assertEquals(1, is.available());
        }

        var buf = new byte[5];
        try (var is = byteStream(0x05, 0, 0x63, 0, 0x61, 0, 0x6c, 0, 0x2e, 0, 0x6c, 1)) {
            assertEquals(0, is.position());
            assertEquals(12, is.available());
            assertEquals(5, is.read(buf));
            assertEquals(0x05, buf[0]);
            assertEquals(0x0, buf[1]);
            assertEquals(0x63, buf[2]);
            assertEquals(0x0, buf[3]);
            assertEquals(0x61, buf[4]);
//            assertEquals(5, is.position()); // conflicts with `read(byte[] b, int off, int len)`
            assertEquals(7, is.available());
        }
//        int readVarIntZigZag()
//        int readVarLongZigZag()

    }

    @Test
    void testReadVarInt() throws IOException {
        assertEquals(3, parseVarInt(0x03, 0x00));
        assertEquals(9, parseVarInt(0x09, 0x00));
        assertEquals(10, parseVarInt(0x0A, 0x00));
        assertEquals(17, parseVarInt(0x11, 0x00));
        assertEquals(25, parseVarInt(0x19, 0x00));
        assertEquals(167, parseVarInt(0xA7, 0x01));
        assertEquals(244, parseVarInt(0xF4, 0x01));
        assertEquals(254, parseVarInt(0xFE, 0x01));
        assertEquals(256, parseVarInt(0x80, 0x02));

        assertEquals(177, parseVarInt(0xB1, 0x01));

        assertEquals(0, parseVarInt(0, 0, 39, 18));
    }

    int parseVarInt(int... arr) throws IOException {
        try (var is = byteStream(arr)) {
            return is.readVarInt();
        }
    }

}