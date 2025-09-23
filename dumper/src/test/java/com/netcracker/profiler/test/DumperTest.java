package com.netcracker.profiler.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.*;

import java.io.*;

public class DumperTest {
    @Test
    @Disabled
    public void testByteConversion() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        byte x = (byte) (3 << 6);
        System.out.println("x = " + x);
        dos.write(x);
        final byte[] buf = baos.toByteArray();

        ByteArrayInputStream is = new ByteArrayInputStream(buf);
        DataInputStream dis = new DataInputStream(is);
        assertEquals(3, dis.read() >> 6);
    }

    @Test
    @Disabled
    public void testIntConversion() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        for (int millis = -10000; millis <= 10000; millis++) {
            baos.reset();

            int b1 = (millis >>> 24);
            int b2 = ((millis >>> 16) & 0xff);
            int b3 = ((millis >>> 8) & 0xff);
            int b4 = (millis & 0xff);
            int cnt = (byte) (b1 != 0 ? 3 : (b2 != 0 ? 2 : (b3 != 0 ? 1 : 0)));
            System.out.println("millis = " + millis);
            System.out.println("cnt = " + cnt);
            cnt |= 1 << 2;
            System.out.println("cnt = " + cnt);
            final int cnn = ((byte) 1) | (cnt << 4);
            System.out.println("cnn = " + cnn);
            System.out.println("cnn>>4 = " + ((cnn >> 4) & 0x3));
            System.out.println("b1 = " + b1);
            System.out.println("b2 = " + b2);
            System.out.println("b3 = " + b3);
            System.out.println("b4 = " + b4);
            dos.writeByte(cnn);
            switch (cnt & 0x3) {
                case 3:
                    dos.write(b1);
                case 2:
                    dos.write(b2);
                case 1:
                    dos.write(b3);
                case 0:
                    dos.write(b4);
            }

            final byte[] buf = baos.toByteArray();
            ByteArrayInputStream is = new ByteArrayInputStream(buf);
            DataInputStream dis = new DataInputStream(is);

            int x = dis.readByte();
            System.out.println("x = " + x);
            int t = 0;
            switch ((x >>> 4) & 0x3) {
                case 3:
                    t |= dis.read() << 24;
                case 2:
                    t |= dis.read() << 16;
                case 1:
                    t |= dis.read() << 8;
                case 0:
                    t |= dis.read();
            }


            assertEquals(millis, t);
        }
    }
}
