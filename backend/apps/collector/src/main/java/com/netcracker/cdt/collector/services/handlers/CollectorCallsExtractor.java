package com.netcracker.cdt.collector.services.handlers;

import com.netcracker.common.models.CallsModel;
import com.netcracker.common.models.pod.streams.StreamRegistry;
import com.netcracker.persistence.PersistenceService;
import com.netcracker.profiler.model.Call;
import com.netcracker.profiler.sax.call.CallDataReaderFactory;
import com.netcracker.profiler.sax.io.DataInputStreamEx;
import com.netcracker.profiler.timeout.ReadInterruptedException;
import io.quarkus.logging.Log;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import static com.netcracker.common.Consts.CALL_HEADER_MAGIC;

// This is the same as com.netcracker.cdt.ui.services.calls.search.CallsExtractor, but it is used in the collector
public class CollectorCallsExtractor {

    private final PersistenceService persistence;
    private final StreamRegistry streamRegistry;

    private final PipedInputStream pipedInputStream;
    private PipedOutputStream pipedOutputStream;

    private volatile boolean isRunning = true;
    private volatile boolean isFinished = false;

    public CollectorCallsExtractor(PersistenceService persistence, StreamRegistry streamRegistry) {
        this.persistence = persistence;
        this.streamRegistry = streamRegistry;
        this.pipedInputStream = new PipedInputStream();
        try {
            this.pipedOutputStream =  new PipedOutputStream(pipedInputStream);
        } catch (IOException e) {
            Log.errorf("exception while creating CallsExtractor: %s", e.getMessage());
        }
        Thread.startVirtualThread(this::findCallsInStream);
    }

    /**
     * Adds bytes to the stream to write them to the database
     *
     * @param bytes - Non-zipped (gzipped) bytes of data that came from the agent
     * @param len - The actual amount of data in the byte array
     */
    public void add(byte[] bytes, int len) {
        try {
            pipedOutputStream.write(bytes, 0, len);
        } catch (IOException e) {
            Log.errorf("exception while add bytes: %s", e.getMessage());
        }
    }

    // TODO: This may lead to the fact that the data will not have time to be written to the database.
    public void close() throws IOException {
        isRunning = false;
        pipedOutputStream.close();
    }

    public boolean isFinished() {
        return isFinished;
    }

    private void findCallsInStream() {

        DataInputStreamEx calls  = new DataInputStreamEx(pipedInputStream);
        int readCalls = 0;

        try {
            CallsFileHeader cfh = readStartTime(calls);
            int fileFormat = cfh.fileFormat();
            long time = cfh.startTime();

            var callDataReader = CallDataReaderFactory.createReader(fileFormat);
            if (callDataReader == null) {
                Log.errorf("invalid calls' format %d in stream", fileFormat);
                return;
            }

            while (isRunning) {

                if (Thread.interrupted()) {
                    throw new ReadInterruptedException();
                }

                Call call = new Call();
                callDataReader.read(call, calls);
                time += call.time;
                call.time = time;

                callDataReader.readParams(call, calls);
                readCalls++;

                // TODO: replace with batching of calls
                Thread.startVirtualThread(() -> persistence.calls.insert(CallsModel.of(call, streamRegistry.podRestart())));
            }
        } catch (IOException e) {
            // it's ok to get EOF when reading current stream
            // TODO: change log message (add details: pod sequenceId how much calls and bytes)
            Log.warnf("exception while reading current stream: %s (read %d calls)", streamRegistry.podRestart().oldPodName(), readCalls);
        }

        isFinished = true;
    }

    private CallsFileHeader readStartTime(DataInputStreamEx calls) throws IOException {
        long time = calls.readLong();
        int fileFormat = 0;
        if ((int) (time >>> 32) == CALL_HEADER_MAGIC) {
            fileFormat = (int) (time & 0xffffffffL);
            time = calls.readLong();
        }
        return new CallsFileHeader(fileFormat, time);
    }

    public record CallsFileHeader(int fileFormat, long startTime) {
    }

}
