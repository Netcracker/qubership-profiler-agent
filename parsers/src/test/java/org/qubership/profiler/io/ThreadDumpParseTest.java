package org.qubership.profiler.io;

import org.qubership.profiler.sax.readers.ThreadDumpReader;
import org.qubership.profiler.sax.stack.DumpVisitor;
import org.qubership.profiler.sax.stack.DumpsVisitor;
import org.qubership.profiler.threaddump.parser.SunThreadFormatParser;
import org.qubership.profiler.threaddump.parser.ThreadInfo;
import org.qubership.profiler.util.IOHelper;
import org.qubership.profiler.util.ProfilerConstants;

import org.junit.Assert;
import org.testng.annotations.Test;

import java.io.*;

public class ThreadDumpParseTest {

    @Test
    public void testSunDump() throws Exception {
        SunThreadFormatParser p = new SunThreadFormatParser();
        ThreadInfo threadInfo = p.parseThread("\"ServiceExecutor[8]\" #695 daemon prio=5 os_prio=0 tid=0x00007f7284004800 nid=0x695a waiting on condition [0x00007f70d34f5000]");
        Assert.assertNotNull(threadInfo);
        Assert.assertEquals("tid", "0x00007f7284004800", threadInfo.threadID);
        Assert.assertEquals("name", "ServiceExecutor[8]", threadInfo.name);
    }

    @Test
    public void testHighOsPrio() throws Exception {
        SunThreadFormatParser p = new SunThreadFormatParser();
        ThreadInfo threadInfo = p.parseThread("\"play-thread-3\" #48 prio=5 os_prio=15 tid=0x00000008dcc54000 nid=0x18e14 waiting on condition [0x00007fffdcfcd000]");
        Assert.assertNotNull(threadInfo);
        Assert.assertEquals("name", "play-thread-3", threadInfo.name);
        Assert.assertEquals("tid", "0x00000008dcc54000", threadInfo.threadID);
    }

    @Test
    public void testOwnableSynchronizers() throws Exception {
        final StringBuffer out = new StringBuffer();
        DumpsVisitor dv = new DumpsVisitor(ProfilerConstants.PROFILER_V1) {
            @Override
            public DumpVisitor visitDump() {
                return new DumpVisitor(ProfilerConstants.PROFILER_V1){
                    @Override
                    public void visitThread(ThreadInfo thread) {
                        thread.toJS(out);
                        out.append('\n');
                    }

                    @Override
                    public void visitEnd() {
                        out.append("dump end\n");
                    }
                };
            }

            @Override
            public void visitEnd() {
                out.append("dumps end\n");
            }
        };
        ThreadDumpReader p = new ThreadDumpReader(dv);
        Reader r = null;
        String fileName = "/harvester_dumps.txt";
        try {
            r = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));
            p.read(r, fileName);
        } finally {
            if (r != null) {
                try {
                    r.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        InputStream is = getClass().getResourceAsStream(fileName + ".out.txt");
        String expected = new String(IOHelper.readFully(is), "UTF-8");
        Assert.assertEquals(normalizeNl(expected), normalizeNl(out.toString()));
    }

    private String normalizeNl(String in) {
        return in.replaceAll("[\r\n]+", "\n");
    }
}
