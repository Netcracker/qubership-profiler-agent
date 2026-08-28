package com.netcracker.profiler.fetch;

import com.netcracker.profiler.dom.ProfiledTreeStreamVisitor;
import com.netcracker.profiler.io.FileWalker;
import com.netcracker.profiler.io.InputStreamProcessor;
import com.netcracker.profiler.io.exceptions.ErrorSupervisor;
import com.netcracker.profiler.sax.builders.ProfiledTreeBuilderMR;
import com.netcracker.profiler.sax.builders.SuspendLogBuilderFactory;
import com.netcracker.profiler.sax.raw.RepositoryVisitor;
import com.netcracker.profiler.sax.readers.DbmsHprofReader;

import java.io.*;

public class FetchDbmsHprof implements Runnable {
    private final ProfiledTreeStreamVisitor sv;
    private final String dumpsFile;
    private final SuspendLogBuilderFactory suspendLogBuilderFactory;

    public FetchDbmsHprof(ProfiledTreeStreamVisitor sv, String dumpsFile, SuspendLogBuilderFactory suspendLogBuilderFactory) {
        this.sv = sv;
        this.dumpsFile = dumpsFile;
        this.suspendLogBuilderFactory = suspendLogBuilderFactory;
    }

    public void run() {
        final ProfiledTreeBuilderMR treeBuilderMR = new ProfiledTreeBuilderMR(sv, suspendLogBuilderFactory);
        InputStreamProcessor parseHprof = new InputStreamProcessor() {
            public boolean process(InputStream is, String name) {
                Reader reader;
                try {
                    reader = new InputStreamReader(is, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    return true;
                }
                RepositoryVisitor rv = treeBuilderMR.visitRepository(null);
                DbmsHprofReader hprofDecoder = new DbmsHprofReader(rv);
                hprofDecoder.read(reader, name);
                return true;
            }
        };
        FileWalker walker = new FileWalker(parseHprof);
        try {
            walker.walk(dumpsFile);
        } catch (IOException e) {
            ErrorSupervisor.getInstance().warn("Error processing " + dumpsFile, e);
        } finally {
            treeBuilderMR.visitEnd();
        }
    }
}
