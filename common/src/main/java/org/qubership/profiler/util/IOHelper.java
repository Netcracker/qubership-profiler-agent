package org.qubership.profiler.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class IOHelper {
    public static byte[] readFully(InputStream is) throws IOException {
        ArrayList<byte[]> os = null;
        byte[] b, out = null;
        int totalLength = 0;
        while (true) {
            b = new byte[Math.max(is.available(), 1000)];
            final int bytesRead = is.read(b);
            if (bytesRead == -1) break;
            totalLength += bytesRead;
            if (bytesRead < b.length) {
                byte[] c = new byte[bytesRead];
                System.arraycopy(b, 0, c, 0, bytesRead);
                b = c;
            }
            if (out == null) {
                out = b;
                continue;
            }

            if (os == null) {
                os = new ArrayList<byte[]>();
                os.add(out);
            }
            os.add(b);
        }
        if (os == null)
            return out;

        b = new byte[totalLength];
        int pos = 0;
        for (byte[] o : os) {
            System.arraycopy(o, 0, b, pos, o.length);
            pos += o.length;
        }

        return b;
    }

    public static List<String> readAllLinesFromFile(File file) throws IOException {
        try(BufferedReader reader = new BufferedReader(new FileReader(file))) {
            List<String> lines = new ArrayList<>();
            String line;
            while((line = reader.readLine())!=null) {
                lines.add(line);
            }
            return lines;
        }
    }

    public static Properties loadPropertiesFromFile(File file) {
        Properties properties = new Properties();
        try(InputStream in = new FileInputStream(file)) {
            properties.load(in);
        } catch (Exception e) {
            //Do nothing
        }
        return properties;
    }

    public static BufferedReader ensureBuffered(Reader reader) {
        if (reader instanceof BufferedReader) {
            return (BufferedReader)reader;
        }
        return new BufferedReader(reader);
    }

    public static void close(InputStream is) {
        if (is == null) return;
        try {
            is.close();
        } catch(Throwable t){
            /* ignore */
        }
    }
    public static void close(OutputStream os) {
        if (os == null) return;
        try {
            os.close();
        } catch(Throwable t){
            /* ignore */
        }
    }

    public static void close(Writer w) {
        if (w == null) return;
        try {
            w.close();
        } catch(Throwable t){
            /* ignore */
        }
    }
}
