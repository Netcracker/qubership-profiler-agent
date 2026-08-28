package com.netcracker.profiler.sax.readers;

import com.netcracker.profiler.io.exceptions.ErrorSupervisor;
import com.netcracker.profiler.parsers.exception.ParserException;
import com.netcracker.profiler.sax.MethodIdBulder;
import com.netcracker.profiler.sax.raw.RepositoryVisitor;
import com.netcracker.profiler.sax.raw.TreeRowid;
import com.netcracker.profiler.sax.string.StringRepositoryAdapter;
import com.netcracker.profiler.sax.string.StringTraceAdapter;
import com.netcracker.profiler.sax.string.StringTreeTraceAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Reader;

/**
 * Reads Oracle dbms_hprof profiler traces
 */
public class DbmsHprofReader {
    private final static Logger log = LoggerFactory.getLogger(DbmsHprofReader.class);

    private final RepositoryVisitor rv;

    public DbmsHprofReader(RepositoryVisitor rv) {
        this.rv = rv;
    }

    public void read(Reader reader, String name) {
        BufferedReader br;
        if (reader instanceof BufferedReader)
            br = (BufferedReader) reader;
        else
            br = new BufferedReader(reader);
        String s;
        try {
            StringRepositoryAdapter sra = new StringRepositoryAdapter(rv);
            StringTraceAdapter sta = sra.visitTrace();
            StringTreeTraceAdapter ttv = sta.visitTree(TreeRowid.UNDEFINED);
            MethodIdBulder idBuilder = new MethodIdBulder();
            HPROFLine line = new HPROFLine();
            long time = 0;
            long prevMillis = 0;
            while((s = br.readLine()) != null) {
                if (s.startsWith("P#X ")) {
                    try {
                        time += Long.parseLong(s.substring(4));
                        long millis = time / 1000;
                        if (millis != prevMillis) {
                            ttv.visitTimeAdvance(millis - prevMillis);
                            prevMillis = millis;
                        }
                    } catch (NumberFormatException e) {
                        log.warn("Unable to parse 'timer advance' line " + s, e);
                    }
                    continue;
                }
                if ("P#R".equals(s)) {
                    ttv.visitExit();
                    continue;
                }
                if (s.startsWith("P#C")) {
                    if(!line.init(s)) {
                        throw new ParserException("Cannot parse the following line in HPROF Dump: "+s);
                    }
                    ttv.visitEnter(line.buildId(idBuilder));
                    continue;
                }
            }

            ttv.visitEnd();
            sta.visitEnd();
            sra.visitEnd();
        } catch (Error e) {
            throw e;
        } catch (Throwable t) {
            ErrorSupervisor.getInstance().error("Unable to parse HPROF from " + name, t);
        }
    }
}
