package com.netcracker.profiler.fetch;

import com.netcracker.profiler.analyzer.AggregateJFRStacks;
import com.netcracker.profiler.analyzer.MergeTrees;
import com.netcracker.profiler.dom.ProfiledTreeStreamVisitor;
import com.netcracker.profiler.io.FileWalker;
import com.netcracker.profiler.io.InputStreamProcessor;
import com.netcracker.profiler.io.LimitedInputStreamProcessor;
import com.netcracker.profiler.io.exceptions.ErrorSupervisor;
import com.netcracker.profiler.output.CallTreeMediator;
import com.netcracker.profiler.sax.stack.DumpsVisitor;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

public class FetchStackcollapse implements Runnable {
    private final ProfiledTreeStreamVisitor sv;
    private final String dumpsFile;
    private final long firstByte;
    private final long lastByte;

    static class AsyncProfilerRawProcessor implements InputStreamProcessor {
        private final StackcollapseParser parser;

        public AsyncProfilerRawProcessor(StackcollapseParser parser) {
            this.parser = parser;
        }

        public boolean process(InputStream is, String name) {
            try {
                parser.parse(new InputStreamReader(is, "UTF-8"), name);
            } catch (UnsupportedEncodingException e) {
                ErrorSupervisor.getInstance().error("Java somehow does not support UTF-8", e);
            }
            return true;
        }
    }

    public FetchStackcollapse(CallTreeMediator mediator, String dumpsFile, long firstByte, long lastByte) {
        this.sv = mediator;
        this.dumpsFile = dumpsFile;
        this.firstByte = firstByte;
        this.lastByte = lastByte;
    }

    public void run() {
        DumpsVisitor processChain = getDumpsProcessChain(sv);
        StackcollapseParser parser = new StackcollapseParser(processChain);
        InputStreamProcessor process = new AsyncProfilerRawProcessor(parser);
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
        DumpsVisitor agg = new AggregateJFRStacks(merge);
        return agg;
    }
}
