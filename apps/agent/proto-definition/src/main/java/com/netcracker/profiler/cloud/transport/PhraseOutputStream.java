package com.netcracker.profiler.cloud.transport;

import java.io.*;


public class PhraseOutputStream extends ByteArrayOutputStream {

    private BufferedOutputStream bufferedOutputStream;
    private int length;


    public PhraseOutputStream(OutputStream out, int size, int outputBufferSize) {
        super(size);

        this.bufferedOutputStream = new BufferedOutputStream(out, outputBufferSize);
    }

    @Override
    public synchronized void write(int b) {
        ensureCapacity(1);
        super.write(b);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) {
        ensureCapacity(len);
        super.write(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
        ensureCapacity(b.length);
        super.write(b);
    }

    private void ensureCapacity(int numToWrite) {
        if(numToWrite > buf.length) {
            throw new ProfilerProtocolException("Can not write a phrase: buffer of " + buf.length + " bytes exceeded. requested size is " + numToWrite );
        }
        //attempt to write out whatever phrases have been committed
        if(size() + numToWrite >= buf.length) {
            try {
                writeDataIntoOutputStream();
            } catch (IOException e) {
                throw new ProfilerProtocolException(e);
            }
        }
        if(size() + numToWrite >= buf.length) {
            // it fails in cases when the single word has size more `outputBufferSize` (usually MAX_PHRASE_SIZE)
            throw new ProfilerProtocolException("Can not write a phrase: buffer of " + buf.length + " bytes exceeded. requested size is " + numToWrite + ". uncommited size is " + size());
        }
    }

    public void writePhrase() throws IOException {
        length = super.size();

        if (super.size() < buf.length - 200) {
            return;
        }

        writeDataIntoOutputStream();
    }

    @Override
    public synchronized void flush() throws IOException {
        writeDataIntoOutputStream();

        bufferedOutputStream.flush();
    }

    private void writeDataIntoOutputStream() throws IOException {
        //do not write anything if no phrases have been closed
        if(length == 0) {
            return; // beware of cases when the word has size more `outputBufferSize` (usually MAX_PHRASE_SIZE)
        }

        new DataOutputStream(bufferedOutputStream).writeInt(length);

        if (length == super.size()) {
            bufferedOutputStream.write(buf, 0, count);

            super.reset();
        } else {
            int count = size();

            bufferedOutputStream.write(buf, 0, length);

            super.reset();

            //will cause ensureCapacity to be fired second time. but there should always be enough capacity
            write(buf, length, count-length);
        }

        length = 0;
    }

    @Override
    public void close() throws IOException {
        try (OutputStream ostream = bufferedOutputStream) {
            flush();
        }
    }

    public int getPhraseLength() {
        return length;
    }
}
