package com.netcracker.fixtures.data;

import com.netcracker.utils.Utils;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public record BinaryFile(String filename, long size) {

    public byte[] getBytes() throws IOException {
        var res = Utils.readBytes(filename);
        assertNotNull(res);
        assertEquals(size, res.length);
        return res;
    }

    public static BinaryFile of(String filename, long size) {
        return new BinaryFile(filename, size);
    }
}
