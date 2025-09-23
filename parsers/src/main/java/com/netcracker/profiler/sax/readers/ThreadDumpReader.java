package com.netcracker.profiler.sax.readers;

import com.netcracker.profiler.io.exceptions.ErrorSupervisor;
import com.netcracker.profiler.sax.stack.DumpVisitor;
import com.netcracker.profiler.sax.stack.DumpsVisitor;
import com.netcracker.profiler.threaddump.parser.*;
import com.netcracker.profiler.util.IOHelper;

import java.io.BufferedReader;
import java.io.Reader;

public class ThreadDumpReader {
    private final DumpsVisitor dumps;

    private enum ParserState {
        NOT_STARTED,
        IN_DUMP,
        THREAD_STACK,
        OWNABLE_SYNCHRONIZERS,
        SMR_INFO
    }


    public ThreadDumpReader(DumpsVisitor dumps) {
        this.dumps = dumps;

    }

    public void read(Reader reader, String name) {
        BufferedReader br = IOHelper.ensureBuffered(reader);
        ThreadFormatParser parser = null;
        String s = null;
        ParserState state = ParserState.NOT_STARTED;
        ThreadInfo thread = null;

        DumpVisitor dump = null;

        int maxGarbageLines = 0;
        try {
            while ((s = br.readLine()) != null) {
                if (s.length() == 0) {
                    if (state == ParserState.THREAD_STACK) {
                        dump.visitThread(thread);
                        state = ParserState.IN_DUMP;
                    }
                    if (state == ParserState.OWNABLE_SYNCHRONIZERS) {
                        state = ParserState.IN_DUMP;
                    }
                    continue;
                }
                char c = s.charAt(0);
                if (state == ParserState.THREAD_STACK) {
                    if (c == '\t' || (c == ' ' && s.length() > 10 && (s.charAt(5) == 't' || s.charAt(5) == '-'))) {
                        thread.addThreadLine(parser.parseThreadLine(s));
                        continue;
                    } else if (c == ' ' && s.startsWith("   java.lang.Thread"))
                        continue;
                    dump.visitThread(thread);
                    state = ParserState.IN_DUMP;
                    continue;
                }

                if (state == ParserState.OWNABLE_SYNCHRONIZERS) {
                    if (!s.startsWith("\t-")) {
                        state = ParserState.IN_DUMP;
                    }
                    continue;
                }

                if (state == ParserState.SMR_INFO) {
                    if (c == '}') {
                        state = ParserState.IN_DUMP;
                    }
                    continue;
                }

                if (state == ParserState.IN_DUMP) {
                    if (s.startsWith("   Locked ownable synchronizers:")) {
                        state = ParserState.OWNABLE_SYNCHRONIZERS;
                        continue;
                    }
                    if(s.startsWith("Threads class SMR info")) {
                        state = ParserState.SMR_INFO;
                        continue;
                    }
                    if (c == '"') {
                        state = ParserState.THREAD_STACK;
                        thread = parser.parseThread(s);
                        maxGarbageLines = 0;
                        continue;
                    }
                    if (maxGarbageLines-- > 0)
                        continue;
                    dump.visitEnd();
                }
                if (c == 'F' && s.startsWith("Full thread dump") ||
                        c == 'T' && s.startsWith("Thread dump") ||
                        c == '=' && s.startsWith("===== FULL THREAD DUMP ===============")) {
                    if (parser == null)
                        parser = c == 'F' ? new SunThreadFormatParser() :
                                c == 'T' ? new SunJMXThreadFormatParser() :
                                        new JRockitThreadParser();
                    state = ParserState.IN_DUMP;
                    dump = this.dumps.visitDump();
                    maxGarbageLines = 10;
                    continue;
                }
                state = ParserState.NOT_STARTED;
            }
        } catch (Error e) {
            throw e;
        } catch (Throwable t) {
            ErrorSupervisor.getInstance().warn("Error while parsing thread dump " + name + " at line " + s, t);
        }
        if (state == ParserState.IN_DUMP)
            dump.visitEnd();

        this.dumps.visitEnd();
    }
}
