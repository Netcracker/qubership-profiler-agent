package com.netcracker.cdt.collector.parsers;

import java.io.*;

import static com.netcracker.common.ProtocolConst.MAX_PHRASE_SIZE;

public class ParsedInputStream extends InputStream {

    private byte[] backingArray = new byte[MAX_PHRASE_SIZE+1];
    private ByteArrayInputStream stream;
    private DataInputStream dataInputStream;
    private int lengthOfPhrase = 0;
    private int actualLengthOfData;

    public ParsedInputStream() {
        this.stream = new ByteArrayInputStream(backingArray);
        this.dataInputStream = new DataInputStream(this);
    }

    /**
     *
     * @param b
     * @param off
     * @param len
     * @return number of bytes that was added
     */
    public int addNewData(byte[] b, int off, int len) {
        if (actualLengthOfData + len >= MAX_PHRASE_SIZE) {
            deleteParsedData(stream.available());
        }

        int numBytesToRead = Math.min(
                len, Math.min(
                        b.length - off,
                        backingArray.length-actualLengthOfData))
                ;

        System.arraycopy(b, off, backingArray, actualLengthOfData, numBytesToRead);
        actualLengthOfData += numBytesToRead;
        return numBytesToRead;
    }

    private void deleteParsedData(int remainingData) {
        actualLengthOfData -= backingArray.length - remainingData;

        System.arraycopy(backingArray, backingArray.length - remainingData, backingArray, 0, actualLengthOfData);

        stream.reset();
    }

    @Override
    public int read() throws IOException {
        return stream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return stream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return stream.read(b, off, len);
    }

    @Override
    public long skip(long n) {
        return stream.skip(n);
    }

    @Override
    public int available() {
        return stream.available();
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        stream.mark(readlimit);
    }

    @Override
    public synchronized void reset() {
        stream.reset();
    }

    @Override
    public boolean markSupported() {
        return stream.markSupported();
    }

    public int getLengthOfPhrase() {
        return lengthOfPhrase;
    }

    public boolean isAbleToReadFullPhrase() {
        return remainingDataLength() >= lengthOfPhrase && lengthOfPhrase != 0;
    }

    public void readLenOfPhrases() throws IOException {
        if (remainingDataLength() >= 4) {
            this.lengthOfPhrase = dataInputStream.readInt();
        } else {
            lengthOfPhrase = 0;
        }
    }

    private int remainingDataLength() {
        return actualLengthOfData - (MAX_PHRASE_SIZE - stream.available());
    }

    private int getPhraseStartPosition(){
        return MAX_PHRASE_SIZE - stream.available();
    }

    public void compressData(OutputStream compressor) throws IOException {
        if(compressor != null) {
            compressor.write(backingArray, getPhraseStartPosition(), lengthOfPhrase);
        }
    }
}

