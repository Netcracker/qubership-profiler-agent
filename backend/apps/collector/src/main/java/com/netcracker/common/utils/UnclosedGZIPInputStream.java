package com.netcracker.common.utils;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

public class UnclosedGZIPInputStream extends GZIPInputStream {
    int lastLenTried = -1;

    public UnclosedGZIPInputStream(InputStream in, int size) throws IOException {
        super(in, size);
    }

    public UnclosedGZIPInputStream(InputStream in) throws IOException {
        super(in);
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        while (true) {
            try {
                return super.read(buf, off, len);
            } catch (EOFException err) {
                if("Unexpected end of ZLIB input stream".equals(err.getMessage())){
                    lastLenTried = len;
                    if (len == 1) {
                        return -1;
                    }
                    len = len / 2;
                } else {
                    throw err;
                }
            } catch (ZipException e) {
                throw e;
            }
        }
    }
}
