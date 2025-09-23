package com.netcracker.profiler.io;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class SizeParserTest {
    @Test
    public void sizeBytesTest() {
        assertEquals(42, SizeParser.parseSize("42b", 0));
        assertEquals(42, SizeParser.parseSize(" 42 bytes ", 0));
    }

    @Test
    public void sizeKBytesTest() {
        assertEquals(42 * 1024, SizeParser.parseSize("42k", 0));
        assertEquals(42 * 1024, SizeParser.parseSize("42Kb", 0));
    }

    @Test
    public void sizeMBytesTest() {
        assertEquals(42 * 1024 * 1024, SizeParser.parseSize("42", 0));
        assertEquals(42 * 1024 * 1024, SizeParser.parseSize("42 M", 0));
        assertEquals(42 * 1024 * 1024, SizeParser.parseSize("42 Mo", 0));
    }

    @Test
    public void sizeGBytesTest() {
        assertEquals(42L * 1024 * 1024 * 1024, SizeParser.parseSize("42G", 0));
        assertEquals(42L * 1024 * 1024 * 1024, SizeParser.parseSize("42 gb", 0));
    }

    @Test
    public void sizeTBytesTest() {
        assertEquals(42L * 1024 * 1024 * 1024 * 1024, SizeParser.parseSize("42T", 0));
        assertEquals(42L * 1024 * 1024 * 1024 * 1024, SizeParser.parseSize("42 Tb", 0));
    }

    @Test
    public void sizeCombined() {
        assertEquals(42 * 1024 * 1024 + 84 * 1024, SizeParser.parseSize("42M84K", 0));
        assertEquals(42 * 1024 * 1024 - 84 * 1024, SizeParser.parseSize("42Mb -84Kb", 0));
    }

    @Test
    public void sizeUnparsable() {
        assertEquals(12345, SizeParser.parseSize("abcd", 12345));
    }
}
