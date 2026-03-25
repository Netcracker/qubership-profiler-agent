package com.netcracker.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.netcracker.common.utils.UnclosedGZIPInputStream;
import com.netcracker.profiler.sax.io.DataInputStreamEx;
import io.quarkus.logging.Log;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;

public class Utils {

    public static BitSet setOf(int... args) {
        var r = new BitSet();
        for (var i: args)
            r.set(i);
        return r;
    }

    public static String toJson(Object data) {
        try {
            return (new ObjectMapper()).writeValueAsString(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertJsonEquals(String expected, String actual) {
        try {
            JSONAssert.assertEquals(expected, actual, JSONCompareMode.STRICT);
        } catch (AssertionError e) {
            Log.errorf("Json non equal from: %s", e.getStackTrace()[3]);
            Log.errorf(e, "asd");
            Log.errorf("Expected: %s", expected);
            Log.errorf("Actual: %s", actual);
            throw e;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static DataInputStreamEx byteStream(int... bytes) {
        var b = new byte[bytes.length];
        for (int i =0; i<bytes.length; i++) {
            b[i] = (byte) bytes[i];
        }
        return new DataInputStreamEx(new ByteArrayInputStream(b));
    }

    public static DataInputStreamEx testRawDataStream(String fileName) {
        return new DataInputStreamEx(fileStream(fileName));
    }

    public static DataInputStreamEx testZipDataStream(String fileName) {
        return new DataInputStreamEx(unzipInputStream(fileStream(fileName)));
    }

    public static InputStream unzipInputStream(InputStream dataStream) {
        try {
            return new UnclosedGZIPInputStream(dataStream, Short.MAX_VALUE); // add one more stream that turns ZipExceptions into EOF
        } catch (IOException e) {
            return dataStream;
        }
    }

    public static byte[] readBytes(String fileName) throws IOException {
        return ByteStreams.toByteArray(fileStream(fileName));
    }

    public static String readString(String fileName) throws IOException {
        return new String(readBytes(fileName));
    }

    public static InputStream fileStream(String fileName) {
        return Utils.class.getClassLoader().getResourceAsStream(fileName);
    }
}
