package org.qubership.profiler.io;

import org.qubership.profiler.fetch.StackcollapseParser;
import org.qubership.profiler.threaddump.parser.ThreadInfo;
import org.qubership.profiler.threaddump.parser.ThreaddumpParser;

import org.junit.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StackcollapseParseTest {
    private void assertEquals(String expected, ThreaddumpParser.JSerializable res) {
        StringBuffer sb = new StringBuffer();
        res.toJS(sb);
        Assert.assertEquals(expected, sb.toString());
    }

    @DataProvider
    public static Object[][] frames() {
        return new String[][]{
                {"java/util/concurrent/ThreadPoolExecutor.runWorker", "M.g('java.util.concurrent.ThreadPoolExecutor\\000runWorker\\000Unknown\\000')\n"},
                {"__do_softirq_[k]", "M.g('kernel\\000__do_softirq\\000\\000')\n"},
                {"java_lang_Thread::get_thread_status(oopDesc*)", "M.g('java_lang_Thread\\000get_thread_status\\000\\000')\n"},
                {"ThreadLocalAllocBuffer::fill(HeapWord*, HeapWord*, unsigned long)", "M.g('ThreadLocalAllocBuffer\\000fill\\000\\000')\n"},
                {"pthread_cond_wait@@GLIBC_2.3.2", "M.g('pthread_cond_wait@@GLIBC_2\\0003.2\\000Unknown\\000')\n"},
        };
    }

    @Test(dataProvider = "frames")
    public void frame(String line, String expected) {
        StackcollapseParser p = new StackcollapseParser(null);
        ThreaddumpParser.ThreadLineInfo res = p.parseThreadLine(line);
        assertEquals(expected, res);
    }

    @DataProvider
    public static Object[][] threads() {
        return new String[][]{
                {"fr/marben/diameterimpl/MARBENa/MARBENa/MARBENk.MARBENa" +
                        ";sun/nio/ch/SocketChannelImpl.read" +
                        ";sun/nio/ch/IOUtil.read" +
                        ";sun/nio/ch/IOUtil.readIntoNativeBuffer" +
                        ";sun/nio/ch/SocketDispatcher.read" +
                        ";sun/nio/ch/FileDispatcherImpl.read0" +
                        ";__read" +
                        ";netif_skb_features_[k] 3",
                        "new ThreadInfo('\\000\\0000\\000\\000\\0003\\000', [M.g('kernel\\000netif_skb_features\\000\\000')\n" +
                                ", M.g('native\\000__read\\000\\000')\n" +
                                ", M.g('sun.nio.ch.FileDispatcherImpl\\000read0\\000Unknown\\000')\n" +
                                ", M.g('sun.nio.ch.SocketDispatcher\\000read\\000Unknown\\000')\n" +
                                ", M.g('sun.nio.ch.IOUtil\\000readIntoNativeBuffer\\000Unknown\\000')\n" +
                                ", M.g('sun.nio.ch.IOUtil\\000read\\000Unknown\\000')\n" +
                                ", M.g('sun.nio.ch.SocketChannelImpl\\000read\\000Unknown\\000')\n" +
                                ", M.g('fr.marben.diameterimpl.MARBENa.MARBENa.MARBENk\\000MARBENa\\000Unknown\\000')\n" +
                                ", ])"},
                {"fr/marben/diameterimpl/core/MARBENhd.MARBENa" +
                        ";fr/marben/diameterimpl/core/MARBENac.toString" +
                        ";jlong_disjoint_arraycopy" +
                        ";vmxnet3_poll_rx_only     [vmxnet3]_[k] 1",
                        "new ThreadInfo('\\000\\0000\\000\\000\\0001\\000', [M.g('kernel\\000vmxnet3_poll_rx_only[vmxnet3]\\000\\000')\n" +
                                ", M.g('native\\000jlong_disjoint_arraycopy\\000\\000')\n" +
                                ", M.g('fr.marben.diameterimpl.core.MARBENac\\000toString\\000Unknown\\000')\n" +
                                ", M.g('fr.marben.diameterimpl.core.MARBENhd\\000MARBENa\\000Unknown\\000')\n" +
                                ", ])"},
        };
    }

    @Test(dataProvider = "threads")
    public void thread(String line, String expected) {
        StackcollapseParser p = new StackcollapseParser(null);
        ThreadInfo res = p.parseThread(line);
        assertEquals(expected, res);
    }
}
