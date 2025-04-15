package org.qubership.profiler.fetch;

import org.qubership.profiler.dom.ProfiledTreeStreamVisitor;
import org.qubership.profiler.io.FileWalker;
import org.qubership.profiler.io.InputStreamProcessor;
import org.qubership.profiler.io.exceptions.ErrorSupervisor;
import org.qubership.profiler.sax.builders.ProfiledTreeBuilderMR;
import org.qubership.profiler.sax.raw.RepositoryVisitor;
import org.qubership.profiler.sax.readers.DbmsHprofReader;

import org.springframework.context.ApplicationContext;

import java.io.*;

public class FetchDbmsHprof implements Runnable {
    private final ProfiledTreeStreamVisitor sv;
    private final String dumpsFile;
    private ApplicationContext context;

    public FetchDbmsHprof(ProfiledTreeStreamVisitor sv, String dumpsFile, ApplicationContext applicationContext) {
        this.sv = sv;
        this.dumpsFile = dumpsFile;
        this.context = applicationContext;
    }

    public void run() {
        final ProfiledTreeBuilderMR treeBuilderMR = new ProfiledTreeBuilderMR(sv, context);
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
