package com.netcracker.profiler.test.dump;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.netcracker.profiler.dump.DataInputStreamEx;
import com.netcracker.profiler.dump.DataOutputStreamEx;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.Enumeration;

public class DataOutputStreamTest {
    @Test
    public void varIntOutputTest() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        @SuppressWarnings("resource")
        DataOutputStreamEx dos = new DataOutputStreamEx(baos);
        dos.writeVarInt(0);
        assertArrayEquals(new byte[]{0}, baos.toByteArray());
        dos.writeVarInt(1);
        assertArrayEquals(new byte[]{0, 1}, baos.toByteArray());
        dos.writeVarInt(2);
        assertArrayEquals(new byte[]{0, 1, 2}, baos.toByteArray());
        dos.writeVarInt(0x7f);
        assertArrayEquals(new byte[]{0, 1, 2, 0x7f}, baos.toByteArray());
        dos.writeVarInt(0x80);
        assertArrayEquals(new byte[]{0, 1, 2, 0x7f, (byte) 0x80, 1}, baos.toByteArray());
        dos.writeVarInt(0x81);
        assertArrayEquals(new byte[]{0, 1, 2, 0x7f, (byte) 0x80, 1, (byte) 0x81, 1}, baos.toByteArray());
        dos.writeVarInt(999465);
        assertArrayEquals(new byte[]{0, 1, 2, 0x7f, (byte) 0x80, 1, (byte) 0x81, 1, (byte) 169, (byte) 0x80, 61}, baos.toByteArray());
        dos.writeVarInt(999466);
        assertArrayEquals(new byte[]{0, 1, 2, 0x7f, (byte) 0x80, 1, (byte) 0x81, 1, (byte) 169, (byte) 0x80, 61, (byte) 170, (byte) 0x80, 61}, baos.toByteArray());
        dos.writeVarInt(999467);
        assertArrayEquals(new byte[]{0, 1, 2, 0x7f, (byte) 0x80, 1, (byte) 0x81, 1, (byte) 169, (byte) 0x80, 61, (byte) 170, (byte) 0x80, 61, (byte) 171, (byte) 0x80, 61}, baos.toByteArray());
    }

    @Test
    public void varIntInputTest() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        @SuppressWarnings("resource")
        DataOutputStreamEx dos = new DataOutputStreamEx(baos);
        dos.writeVarInt(0);
        dos.writeVarInt(1);
        dos.writeVarInt(2);
        dos.writeVarInt(0x7f);
        dos.writeVarInt(0x80);
        dos.writeVarInt(0x81);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        @SuppressWarnings("resource")
        DataInputStreamEx dis = new DataInputStreamEx(bais);
        assertEquals(0, dis.readVarInt());
        assertEquals(1, dis.readVarInt());
        assertEquals(2, dis.readVarInt());
        assertEquals(0x7f, dis.readVarInt());
        assertEquals(0x80, dis.readVarInt());
        assertEquals(0x81, dis.readVarInt());
    }

    @Test
    @Tag("performance")
    public void negativeIntegers() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        @SuppressWarnings("resource")
        DataOutputStreamEx dos = new DataOutputStreamEx(baos);
        for (int i = -100000; i <= 0; i++)
            dos.writeVarInt(i);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        @SuppressWarnings("resource")
        DataInputStreamEx dis = new DataInputStreamEx(bais);
        for (int i = -100000; i <= 0; i++)
            assertEquals(dis.readVarInt(), i);
    }

    @Test
    public void varIntOutputZigzagTest() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        @SuppressWarnings("resource")
        DataOutputStreamEx dos = new DataOutputStreamEx(baos);
        dos.writeVarIntZigZag(0);
        assertArrayEquals(new byte[]{0}, baos.toByteArray());
        dos.writeVarIntZigZag(1);
        assertArrayEquals(new byte[]{0, 2}, baos.toByteArray());
        dos.writeVarIntZigZag(-1);
        assertArrayEquals(new byte[]{0, 2, 1}, baos.toByteArray());
        dos.writeVarIntZigZag(2);
        assertArrayEquals(new byte[]{0, 2, 1, 4}, baos.toByteArray());
        dos.writeVarIntZigZag(-2);
        assertArrayEquals(new byte[]{0, 2, 1, 4, 3}, baos.toByteArray());
    }

    @Test
    public void zigzagTestInt() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        @SuppressWarnings("resource")
        DataOutputStreamEx dos = new DataOutputStreamEx(baos);
        int i;
        for (i = -1000; i <= 1000; i++)
            dos.writeVarIntZigZag(i);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        @SuppressWarnings("resource")
        DataInputStreamEx dis = new DataInputStreamEx(bais);
        for (i = -1000; i <= 1000; i++)
            assertEquals(i, dis.readVarIntZigZag(), "Wrong integer decoded from the stream");
        assertEquals(0, dis.available(), "Buffer contains unread bytes");
    }

    @Test
    public void readFully() throws IOException {
        final int N = 30;
        InputStream in = new SequenceInputStream(new Enumeration<InputStream>() {
            int count = 0;
            public boolean hasMoreElements() {
                return count<N;
            }

            public InputStream nextElement() {
                count++;
                return new ByteArrayInputStream(new byte[]{(byte) count});
            }
        }
        );

        @SuppressWarnings("resource")
        DataInputStreamEx inEx = new DataInputStreamEx(in);
        byte []res = new byte[N];
        inEx.readFully(res);
        for(int i = 0; i<N; i++)
            assertEquals(res[i], i+1);
    }

    // dependsOnMethods = {"varIntOutputTest"}
    @RepeatedTest(10)
    @Tag("performance")
    public void varIntOutputPerformance() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1100000 * 4);
        @SuppressWarnings("resource")
        DataOutputStreamEx dos = new DataOutputStreamEx(new BufferedOutputStream(baos));
        for (int i = 0; i < 1000000; i++) {
            dos.writeVarInt(i);
        }
    }

    // dependsOnMethods = {"varIntOutputTest", "varIntInputTest"}
    @RepeatedTest(10)
    @Tag("performance")
    public void varIntInputPerformance() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1100000 * 4);
        @SuppressWarnings("resource")
        DataOutputStreamEx dos = new DataOutputStreamEx(new BufferedOutputStream(baos));
        for (int i = 0; i < 1000000; i++) {
            dos.writeVarInt(i);
        }
        dos.flush();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        @SuppressWarnings("resource")
        DataInputStreamEx dis = new DataInputStreamEx(new BufferedInputStream(bais));
        for (int i = 0; i < 1000000; i++) {
            dis.readVarInt();
        }
    }
}
