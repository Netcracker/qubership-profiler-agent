package com.netcracker.profiler.sax.raw;

import java.io.IOException;

/**
 * Similar to {@link java.lang.CharSequence}, but with {@code throws IOException}.
 * {@link java.io.Reader} might also work, but it has too many unrelated methods.
 */
public interface StrReader {
    int length() throws IOException;
    CharSequence subSequence(int start, int end) throws IOException;
}
