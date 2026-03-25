package com.netcracker.cdt.collector.parsers;

import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.pod.PodIdRestart;
import com.netcracker.common.utils.DB;
import com.netcracker.persistence.op.Operation;
import com.netcracker.persistence.PersistenceService;
import com.netcracker.common.models.Sizeable;
import com.netcracker.profiler.sax.IPhraseInputStreamParser;
import io.quarkus.logging.Log;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public sealed abstract class StreamParser<T extends Sizeable>
        permits ParamsStreamParser, DictionaryStreamParser, SuspendStreamParser {

    protected final PodIdRestart pod;
    protected final StreamType streamType;
    protected final IPhraseInputStreamParser parser;
    protected final ParsedInputStream parsedInputStream;

    protected int length = 0;

    public StreamParser(PodIdRestart pod, StreamType streamType,
                        IPhraseInputStreamParser parser,
                        ParsedInputStream parsedInputStream) {
        this.pod = pod;
        this.streamType = streamType;
        this.parser = parser;
        this.parsedInputStream = parsedInputStream;
    }

    public void resetExistingContents() { // override for Dictionary
    }

    public void receiveData(byte[] b, int off, int len, OutputStream compressor) {
        Log.tracef("[%s] Start parsing %s with bufferLength=%d", streamType, pod.podId(), len);

        if (len == 0) {
            return;
        }

        int numBytesAdded = 0;
        while (numBytesAdded < len) {
            try {
                numBytesAdded += parsedInputStream.addNewData(b, off + numBytesAdded, len);

                if (parsedInputStream.getLengthOfPhrase() == 0) {
                    parsedInputStream.readLenOfPhrases();
                }

                while (parsedInputStream.isAbleToReadFullPhrase()) {
                    parsedInputStream.compressData(compressor);

                    parser.parsingPhrases(parsedInputStream.getLengthOfPhrase(), false);

                    parsedInputStream.readLenOfPhrases();
                }
            } catch (EOFException ignored) {
            } catch (IOException e) {
                Log.errorf(e, "Unable to read %s", streamType);
            }
        }
        Log.tracef("[%s] Finish parsing %s with bufferLength = %d", streamType, pod.podId(), len);
    }

    @DB
    public final void saveData(PersistenceService service) {
        var list = retrieveData();
        if (list.isEmpty()) {
            return;
        }
        Log.tracef("[%s] Found %d entities for pod=%s", streamType, list.size(), pod.podId());
        service.batch.saveInBatches(list, toSave -> save(service, toSave));
        Log.tracef("[%s] Finish saving %d entities for pod=%s", streamType, list.size(), pod.podId());
    }

    public abstract List<T> retrieveData();

    protected abstract Operation save(PersistenceService service, T toSave);

}
