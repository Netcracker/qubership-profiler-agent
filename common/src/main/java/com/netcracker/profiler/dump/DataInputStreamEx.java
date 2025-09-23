package com.netcracker.profiler.dump;

import com.netcracker.profiler.timeout.ReadInterruptedException;
import com.netcracker.profiler.util.IOHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class DataInputStreamEx extends FilterInputStream implements IDataInputStreamEx {
    final static NumberFormat fileIndexFormat = NumberFormat.getIntegerInstance();

    static {
        fileIndexFormat.setGroupingUsed(false);
        fileIndexFormat.setMinimumIntegerDigits(6);
    }

    private static final Logger log = LoggerFactory.getLogger(DataInputStreamEx.class);

    private int position = 0;
    private final Long contentLength;

    /**
     * Creates a DataInputStream that uses the specified
     * underlying InputStream.
     *
     * @param in the specified input stream
     */
    public DataInputStreamEx(InputStream in) {
        this(in, null);
    }

    public DataInputStreamEx(InputStream in, Long contentLength) {
        super(in);
        this.contentLength = contentLength;
    }

    public String readString() throws IOException {
        return readString(100 * 1024 * 1024);
    }

    public String readString(int maxLength) throws IOException {
        int length = readVarInt();
        if (length > maxLength) {
            throw new IOException("Expecting string of max length " + maxLength + ", got " + length
                    + " chars; position = " + position);
        }
        char[] x = new char[length];
        for (int i = 0; i < length; i++)
            x[i] = readChar();
        return new String(x);
    }

    public int readString(Writer out, int maxLength) throws IOException {
        int length = readVarInt();
        if (length < maxLength)
            maxLength = length;

        if (length == 0)
            return 0;

        for (int i = 0; i < maxLength; i++)
            out.write(readChar());

        if (length > maxLength)
            skipBytes((maxLength - length) * 2);

        return length;
    }


    public char readChar() throws IOException {
        int c1 = read();
        int c2 = read();
        if ((c1 | c2) < 0) throw new EOFException();
        return (char) ((c1 << 8) | c2);
    }

    public short readShort() throws IOException {
        int c1 = read();
        int c2 = read();
        if ((c1 | c2) < 0) throw new EOFException();
        return (short) ((c1 << 8) | c2);
    }

    public final int readInt() throws IOException {
        int ch1 = in.read();
        int ch2 = in.read();
        int ch3 = in.read();
        int ch4 = in.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    private char[] charBuffer = new char[8];
    private byte[] byteBuffer = new byte[8];

    public long readLong() throws IOException {
        final byte[] buffer = byteBuffer;
        readFully(buffer, 0, 8);
        return (((long) buffer[0] << 56) +
                ((long) (buffer[1] & 255) << 48) +
                ((long) (buffer[2] & 255) << 40) +
                ((long) (buffer[3] & 255) << 32) +
                ((long) (buffer[4] & 255) << 24) +
                ((buffer[5] & 255) << 16) +
                ((buffer[6] & 255) << 8) +
                ((buffer[7] & 255)));
    }

    public void readFully(byte[] buffer) throws IOException {
        readFully(buffer, 0, buffer.length);
    }

    public void readFully(byte[] buffer, int pos, int len) throws IOException {
        while (len > 0) {
            final int bytesRead = read(buffer, pos, len);
            if (bytesRead < 0)
                throw new EOFException();
            pos += bytesRead;
            len -= bytesRead;
        }
    }

    public int readVarInt() throws IOException {
        int res = read(), x;
        if (res == -1) throw new EOFException();
        if ((res & 0x80) == 0) return res;
        res &= ~0x80;
        if ((x = read()) == -1) throw new EOFException();
        res |= x << 7;
        if ((res & (0x80 << 7)) == 0) return res;
        res &= ~(0x80 << 7);
        if ((x = read()) == -1) throw new EOFException();
        res |= x << 14;
        if ((res & (0x80 << 14)) == 0) return res;
        res &= ~(0x80 << 14);
        if ((x = read()) == -1) throw new EOFException();
        res |= x << 21;
        if ((res & (0x80 << 21)) == 0) return res;
        res &= ~(0x80 << 21);
        if ((x = read()) == -1) throw new EOFException();
        res |= x << 28;
        return res;
    }

    public long readVarLong() throws IOException {
        int res = read(), x;
        if (res == -1) throw new EOFException();
        if ((res & 0x80) == 0) return res;
        res &= ~0x80; // 0..6
        if ((x = read()) == -1) throw new EOFException();
        res |= x << 7;
        if ((res & (0x80 << 7)) == 0) return res;
        res &= ~(0x80 << 7); // 7..13
        if ((x = read()) == -1) throw new EOFException();
        res |= x << 14;
        if ((res & (0x80 << 14)) == 0) return res;
        res &= ~(0x80 << 14); // 14..20
        if ((x = read()) == -1) throw new EOFException();
        res |= x << 21;
        if ((res & (0x80 << 21)) == 0) return res;
        res &= ~(0x80 << 21); // 21..28
        if ((x = read()) == -1) throw new EOFException();
        if ((x & 0x80) == 0) return (((long) x) << 28) | res;
        long resLong = (((long) (x & 0x7f)) << 28) | res;

        return (((long) readVarInt()) << 35) | resLong;
    }

    public final int readVarIntZigZag() throws IOException {
        int res = readVarInt();
        return (res >>> 1) ^ (-(res & 1));
    }

    public final long readVarLongZigZag() throws IOException {
        long res = readVarLong();
        return (res >>> 1) ^ (-(res & 1));
    }

    public int position() {
        return position;
    }

    private void checkInterrupted(){
        if(Thread.interrupted()) {
            throw new ReadInterruptedException();
        }
    }

    @Override
    public long skip(long n) throws IOException {
        checkInterrupted();
        final long bytesRead = super.skip(n);
        // We do not expect skipping more than 2G bytes as we divite files into several megabyte chunks
        position = Math.addExact(position, Math.toIntExact(bytesRead));
        return bytesRead;
    }

    @Override
    public int read() throws IOException {
        checkInterrupted();
        final int i = super.read();
        position++;
        return i;
    }

    @Override
    public int read(byte[] b) throws IOException {
        checkInterrupted();
        final int bytesRead = super.read(b);
        position += bytesRead;
        return bytesRead;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkInterrupted();
        final int bytesRead = super.read(b, off, len);
        position += bytesRead;
        return bytesRead;
    }

    public void skipBytes(int bytes) throws IOException {
        while (bytes > 0) {
            long skipped = skip(bytes);
            if(skipped == 0){
                throw new EOFException();
            }
            bytes -= skipped;
        }
    }

    public void skipString() throws IOException {
        int length = readVarInt();
        skipBytes(length * 2);
    }

    public static InputStream openInputStream(File file) throws IOException {
        boolean isGzip = file.getName().endsWith(".gz");
        try {
            return new BufferedInputStream(new GZIPInputStream(new FileInputStream(file.getAbsolutePath() + (isGzip ? "" : ".gz")), 131072), 131072);
        } catch (FileNotFoundException e) {
            /* fall through -- will try find .gz file later */
        }
        String fileName = file.getAbsolutePath();
        if (isGzip)
            fileName = fileName.substring(0, fileName.length() - 2);
        return new BufferedInputStream(new FileInputStream(fileName), 131072);
    }

    public Long contentLength(){
        return contentLength;
    }

    private static File attemptGZExt(File file){
        if(file.exists()) {
            return file;
        }

        String name = file.getName();
        if (name.endsWith(".gz")) {
            name = name.substring(0, name.length() - 3);
        } else {
            name += ".gz";
        }
        File otherFile =new File(file.getParent(), name);
        return otherFile.exists()? otherFile : file ;
    }

    public static FilesEnumeration openDataInputStreams(final List<File> files) throws IOException {
        List<File> correctedGzExt = new ArrayList<>(files.size());
        for(File file: files) {
            file = attemptGZExt(file);
            correctedGzExt.add(file);
        }

        return new FilesEnumeration(correctedGzExt.iterator());
    }

    public static DataInputStreamEx openDataInputStream(File file) throws IOException {
        file = attemptGZExt(file);
        if(!file.exists()) {
            log.warn("File " + file.getAbsolutePath() + " does not exist. Returning null output stream");
            return null;
        }
        FilesEnumeration fen = openDataInputStreams(Collections.singletonList(file));
        InputStream fin = fen.nextElement();
        return new DataInputStreamEx(fin, fen.currentFileLenght());
    }

    public static synchronized DataInputStreamEx openDataInputStreamAllSequences (File root, String name) throws IOException {
        return new DataInputStreamEx(new SequenceInputStream(enumerateInputStreams(root, name)));
    }

    public static synchronized FilesEnumeration enumerateInputStreams(File root, String name) throws IOException {
        File logsRoot = new File(root, name);
        if(!logsRoot.exists() || !logsRoot.isDirectory()) {
            throw new RuntimeException("Failed to read logs from directory " + logsRoot.getCanonicalPath());
        }
        File[] files = logsRoot.listFiles();
        if(files == null || files.length == 0){
            throw new RuntimeException("Failed to find any files in " + root.getCanonicalPath());
        }
        List<File> toLoad = new ArrayList<>(files.length);
        for(File f: files) {
            if(f.isDirectory()){
                continue;
            }
            String fileName = f.getName();
            if(".".equals(fileName) || "..".equals(fileName)) {
                continue;
            }
            toLoad.add(f);
        }
        Collections.sort(toLoad, new Comparator<File>() {
            @Override
            public int compare(File file, File t1) {
                return file.getName().compareTo(t1.getName());
            }
        });

        return openDataInputStreams(toLoad);
    }

    public static synchronized DataInputStreamEx openDataInputStream(File root, String name, int index) throws IOException {
        return openDataInputStream(new File(root, name + File.separatorChar + fileIndexFormat.format(index)));
    }

    public static DataInputStreamEx reopenDataInputStream(DataInputStreamEx prev, File root, String name, int index) throws IOException {
        if (prev != null)
            IOHelper.close(prev);
        return openDataInputStream(root, name, index);
    }

    public int available() throws IOException {
        return in.available();
    }

    public void reset() throws IOException {
        in.reset();
    }

    @Override
    public void close() throws IOException {
        //protect against NPE when closing a stream that failed to open
        if(in != null) {
            super.close();
        }
    }
}
