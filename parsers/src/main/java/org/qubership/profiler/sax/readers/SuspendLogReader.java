package org.qubership.profiler.sax.readers;

import org.qubership.profiler.dump.DataInputStreamEx;
import org.qubership.profiler.io.exceptions.ErrorSupervisor;
import org.qubership.profiler.sax.raw.SuspendLogCollapsingVisitor;
import org.qubership.profiler.sax.raw.SuspendLogVisitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;

@Component
@Scope("prototype")
@Profile("filestorage")
public class SuspendLogReader {
    public static final Logger log = LoggerFactory.getLogger(SuspendLogReader.class);
    protected final SuspendLogVisitor sv;
    private File dataFolder;
    private long begin;
    private long end;

    public SuspendLogReader(SuspendLogVisitor sv, String dataFolderPath, long begin, long end) {
        this.sv = sv;
        this.dataFolder = new File(dataFolderPath);
        this.begin = begin;
        this.end = end;
    }

    public SuspendLogReader(SuspendLogVisitor sv, String dataFolderPath) {
        this(sv, dataFolderPath, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    public SuspendLogReader(SuspendLogVisitor sv) {
        this.sv = sv;
    }

    public void read() {
        DataInputStreamEx is = null;
        Enumeration<InputStream> inputStreams = openInputStream();
        while (inputStreams.hasMoreElements()) {
            try {
                is = new DataInputStreamEx(inputStreams.nextElement());
                long length = 1024; //magic
                if (is.contentLength() != null) {
                    length = is.contentLength();
                }
                read(is, length);
            } catch (Error e) {
                throw e;
            } catch (Throwable t) {
                ErrorSupervisor.getInstance().warn("Unable to open/read suspend log", t);
            } finally {
                sv.visitEnd();
                if (is != null) try {
                    is.close();
                } catch (IOException e) {
                    /**/
                }
            }
        }
    }

    protected void read(DataInputStreamEx is, long fileSize) {
        SuspendLogVisitor sv = this.sv;
        if (fileSize > 1024 * 1024) {
            // When input file is big, it is likely to contain lots of side-by-side hiccups
            sv = new SuspendLogCollapsingVisitor(sv);
        }
        try {
            new SuspendPhraseReader(is, sv).parsingPhrases(Integer.MAX_VALUE, true, begin, end);
        } catch (IOException e) {
            if (!(e instanceof EOFException)) {
                ErrorSupervisor.getInstance().warn("Unable to read suspend log ", e);
            }
        }

    }

    protected Enumeration<InputStream> openInputStream() {
        try {
            return DataInputStreamEx.enumerateInputStreams(dataFolder, "suspend");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
