package org.qubership.profiler.test.dump;

import org.qubership.profiler.dump.DataInputStreamEx;
import org.qubership.profiler.dump.DataOutputStreamEx;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Enumeration;

public class DataOutputStream {
    @Test
    public void varIntOutputTest() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        @SuppressWarnings("resource")
        DataOutputStreamEx dos = new DataOutputStreamEx(baos);
        dos.writeVarInt(0);
        Assert.assertEquals(new byte[]{0}, baos.toByteArray());
        dos.writeVarInt(1);
        Assert.assertEquals(new byte[]{0, 1}, baos.toByteArray());
        dos.writeVarInt(2);
        Assert.assertEquals(new byte[]{0, 1, 2}, baos.toByteArray());
        dos.writeVarInt(0x7f);
        Assert.assertEquals(new byte[]{0, 1, 2, 0x7f}, baos.toByteArray());
        dos.writeVarInt(0x80);
        Assert.assertEquals(new byte[]{0, 1, 2, 0x7f, (byte) 0x80, 1}, baos.toByteArray());
        dos.writeVarInt(0x81);
        Assert.assertEquals(new byte[]{0, 1, 2, 0x7f, (byte) 0x80, 1, (byte) 0x81, 1}, baos.toByteArray());
        dos.writeVarInt(999465);
        Assert.assertEquals(new byte[]{0, 1, 2, 0x7f, (byte) 0x80, 1, (byte) 0x81, 1, (byte) 169, (byte) 0x80, 61}, baos.toByteArray());
        dos.writeVarInt(999466);
        Assert.assertEquals(new byte[]{0, 1, 2, 0x7f, (byte) 0x80, 1, (byte) 0x81, 1, (byte) 169, (byte) 0x80, 61, (byte) 170, (byte) 0x80, 61}, baos.toByteArray());
        dos.writeVarInt(999467);
        Assert.assertEquals(new byte[]{0, 1, 2, 0x7f, (byte) 0x80, 1, (byte) 0x81, 1, (byte) 169, (byte) 0x80, 61, (byte) 170, (byte) 0x80, 61, (byte) 171, (byte) 0x80, 61}, baos.toByteArray());
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
        Assert.assertEquals(dis.readVarInt(), 0);
        Assert.assertEquals(dis.readVarInt(), 1);
        Assert.assertEquals(dis.readVarInt(), 2);
        Assert.assertEquals(dis.readVarInt(), 0x7f);
        Assert.assertEquals(dis.readVarInt(), 0x80);
        Assert.assertEquals(dis.readVarInt(), 0x81);
    }

    @Test(groups = "performance")
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
            Assert.assertEquals(dis.readVarInt(), i);
    }

    @Test
    public void varIntOutputZigzagTest() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        @SuppressWarnings("resource")
        DataOutputStreamEx dos = new DataOutputStreamEx(baos);
        dos.writeVarIntZigZag(0);
        Assert.assertEquals(new byte[]{0}, baos.toByteArray());
        dos.writeVarIntZigZag(1);
        Assert.assertEquals(new byte[]{0, 2}, baos.toByteArray());
        dos.writeVarIntZigZag(-1);
        Assert.assertEquals(new byte[]{0, 2, 1}, baos.toByteArray());
        dos.writeVarIntZigZag(2);
        Assert.assertEquals(new byte[]{0, 2, 1, 4}, baos.toByteArray());
        dos.writeVarIntZigZag(-2);
        Assert.assertEquals(new byte[]{0, 2, 1, 4, 3}, baos.toByteArray());
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
            Assert.assertEquals(dis.readVarIntZigZag(), i, "Wrong integer decoded from the stream");
        Assert.assertEquals(dis.available(), 0, "Buffer contains unread bytes");
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
            Assert.assertEquals(res[i], i+1);
    }

    @Test(groups = {"performance"}, invocationCount = 10, dependsOnMethods = {"varIntOutputTest"})
    public void varIntOutputPerformance() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1100000 * 4);
        @SuppressWarnings("resource")
        DataOutputStreamEx dos = new DataOutputStreamEx(new BufferedOutputStream(baos));
        for (int i = 0; i < 1000000; i++) {
            dos.writeVarInt(i);
        }
    }

    @Test(groups = {"performance"}, invocationCount = 10, dependsOnMethods = {"varIntOutputTest", "varIntInputTest"})
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
