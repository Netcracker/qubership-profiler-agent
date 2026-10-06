package com.netcracker.profiler.io;

import com.netcracker.profiler.io.exceptions.ErrorSupervisor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class LimitedInputStreamProcessor implements InputStreamProcessor {
    public static final Logger log = LoggerFactory.getLogger(LimitedInputStreamProcessor.class);

    private final InputStreamProcessor out;
    private long offset;
    private long length;

    public LimitedInputStreamProcessor(InputStreamProcessor out, long offset, long length) {
        this.out = out;
        this.offset = offset;
        this.length = length;
    }

    public boolean process(InputStream is, String name) {
        boolean needContinue = true;
        try {
            if (offset != 0) {
                long skip = is.skip(offset);
                offset -= skip;
                if (offset > 0) return true; // should try next stream
            }
            LimitedInputStream limitIS = null;
            if (length != Long.MAX_VALUE) {
                if (length < 0) return false; // No need to walk further
                limitIS = new LimitedInputStream(is, length);
                is = limitIS;
            }

            needContinue = out.process(is, null);

            if (limitIS != null)
                length -= limitIS.position();
        } catch (IOException e) {
            ErrorSupervisor.getInstance().warn("Error while processing " + name, e);
        }
        return needContinue;
    }
}
