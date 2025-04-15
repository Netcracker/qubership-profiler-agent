package org.qubership.profiler.sax.readers;

import org.qubership.profiler.dump.IDataInputStreamEx;
import org.qubership.profiler.io.IPhraseInputStreamParser;
import org.qubership.profiler.sax.raw.ISuspendLogVisitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SuspendPhraseReader implements IPhraseInputStreamParser {
    private static final Logger logger = LoggerFactory.getLogger(SuspendPhraseReader.class);

    private ISuspendLogVisitor visitor;
    private IDataInputStreamEx is;
    private long time;
    private int dt;
    private int delay;

    public SuspendPhraseReader(IDataInputStreamEx dataInputStreamEx, ISuspendLogVisitor visitor) {
        this.visitor = visitor;
        this.is = dataInputStreamEx;
    }

    private void initTime() throws IOException {
        if (time == 0L) {
            time = is.readLong();
        }
    }

    public void parsingPhrases(int lenOfPhraseToRead, boolean parseUntilEOF, long begin, long end) throws IOException {
        boolean isTraceEnabled = logger.isTraceEnabled();

        int numberOfBytesToRemain = is.available() - lenOfPhraseToRead;
        initTime();

        while (is.available() > numberOfBytesToRemain || parseUntilEOF) {
            dt = is.readVarInt();
            delay = is.readVarInt();

            time += dt;

            if(time < begin) continue;
            if((time - delay) > end) break;
            if (isTraceEnabled) {
                logger.trace("time: {}, delay: {}, delta_time: {}", time, delay, dt);
            }

            visitor.visitHiccup(time, delay);
        }
    }

    public void parsingPhrases(int lenOfPhraseToRead, boolean parseUntilEOF) throws IOException {
        parsingPhrases(lenOfPhraseToRead, parseUntilEOF, Long.MIN_VALUE, Long.MAX_VALUE);
    }
}
