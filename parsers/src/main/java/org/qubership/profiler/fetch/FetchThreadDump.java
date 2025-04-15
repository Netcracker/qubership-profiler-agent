package org.qubership.profiler.fetch;

import org.qubership.profiler.analyzer.AggregateThreadStacks;
import org.qubership.profiler.analyzer.FilterThreadStacks;
import org.qubership.profiler.analyzer.MergeTrees;
import org.qubership.profiler.analyzer.MoveLockLineUp;
import org.qubership.profiler.dom.ProfiledTreeStreamVisitor;
import org.qubership.profiler.io.FileWalker;
import org.qubership.profiler.io.InputStreamProcessor;
import org.qubership.profiler.io.LimitedInputStreamProcessor;
import org.qubership.profiler.io.exceptions.ErrorSupervisor;
import org.qubership.profiler.sax.readers.ThreadDumpReader;
import org.qubership.profiler.sax.stack.DumpVisitor;
import org.qubership.profiler.sax.stack.DumpsVisitor;
import org.qubership.profiler.util.ProfilerConstants;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

public class FetchThreadDump implements Runnable {
    private final ProfiledTreeStreamVisitor sv;
    private final String dumpsFile;
    private final long firstByte;
    private final long lastByte;

    private class FindThreadDumpsProcessor implements InputStreamProcessor {
        private final ThreadDumpReader reader;

        public FindThreadDumpsProcessor(ThreadDumpReader reader) {
            this.reader = reader;
        }

        public boolean process(InputStream is, String name) {
            try {
                reader.read(new InputStreamReader(is, "UTF-8"), name);
            } catch (UnsupportedEncodingException e) {
                ErrorSupervisor.getInstance().error("Java somehow does not support UTF-8", e);
            }
            return true;
        }
    }

    public FetchThreadDump(ProfiledTreeStreamVisitor sv, String dumpsFile, long firstByte, long lastByte) {
        this.sv = sv;
        this.dumpsFile = dumpsFile;
        this.firstByte = firstByte;
        this.lastByte = lastByte;
    }

    public void run() {
        DumpsVisitor processChain = getDumpsProcessChain(sv);
        final ThreadDumpReader reader = new ThreadDumpReader(processChain.asSkipVisitEnd());
        InputStreamProcessor process = new FindThreadDumpsProcessor(reader);
        LimitedInputStreamProcessor out = new LimitedInputStreamProcessor(process, firstByte, lastByte);
        FileWalker walker = new FileWalker(out);
        try {
            walker.walk(dumpsFile);
        } catch (IOException e) {
            ErrorSupervisor.getInstance().warn("Error processing " + dumpsFile, e);
        } finally {
            processChain.visitEnd();
        }

    }

    private DumpsVisitor getDumpsProcessChain(ProfiledTreeStreamVisitor sv) {
        ProfiledTreeStreamVisitor merge = new MergeTrees(sv);
        DumpsVisitor agg = new AggregateThreadStacks(merge);
        return new DumpsVisitor(ProfilerConstants.PROFILER_V1, agg) {
            @Override
            public DumpVisitor visitDump() {
                DumpVisitor out = super.visitDump();
                out = new FilterThreadStacks(out);
                out = new MoveLockLineUp(out);
                return out;
            }
        };
    }
}
