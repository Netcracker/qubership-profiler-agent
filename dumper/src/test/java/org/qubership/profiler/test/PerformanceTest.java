package org.qubership.profiler.test;

import gnu.trove.map.hash.TIntIntHashMap;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class PerformanceTest {

    String[] m = new String[1000];

    {
        for (int i = 0; i < m.length; i++)
            m[i] = "abcd" + i;
    }

    volatile int counter;

    public void counterProfiled() {
//        LocalBuffer buffer = Profiler.enter2("start");
        for (int i = 0; i < 1000; i++) {
//            if (Profiler.prof.containsKey("ABCD"))
//            if (Profiler.bs.get(i%64))
//            if ((Profiler.bits & 0x00020000) !=0)
//            if (Profiler.profiledThread==Thread.currentThread())
//            buffer = Profiler.enter2(m[i]);//Profiler.localState.get().initEnter(m[i]);
//            buffer = buffer.event("select * from dual", "sql");
//            Profiler.enter(m[i]);
            counter++;
//            Thread.yield();
//            if (Profiler.prof.containsKey("ABCD"))
//            if (Profiler.bs.get(i%64))
//            if ((Profiler.bits & 0x00020000) !=0)
//            if (Profiler.profiledThread==Thread.currentThread())
//            Profiler.exit();
//            if (buffer!=null)
//            buffer = buffer.initExit();//Profiler.localState.get().initExit();
//            Thread.yield();
        }
//        Profiler.exit();
    }

    public void counterNonProfiled() {
        for (int i = 0; i < 1000; i++) {
            counter++;
        }
    }

    @Test
    @Disabled("TODO: rework with jmh")
    public void threadLocalBuffer() throws InterruptedException {
        for (int i = 0; i < 80; i++)
            new ThreadLocal().get();

//        HashMap<Integer, Integer> m = new HashMap<Integer, Integer>();
//        HashMap<Integer, Integer> m2 = new HashMap<Integer, Integer>();
        TIntIntHashMap m = new TIntIntHashMap();
        TIntIntHashMap m2 = new TIntIntHashMap();

        for (int j = 0; j < 30; j++) {
            for (int i = 0; i < 20; i++) {
                long t = System.nanoTime();
                counterNonProfiled();
                t = System.nanoTime() - t - 1010;
                int x = (int) t / 1000;
                m2.put(x, m2.get(x) + 1);
            }
            for (int i = 0; i < 20; i++) {
                long t = System.nanoTime();
                counterProfiled();
                t = System.nanoTime() - t - 1010;
                int x = (int) t / 1000;
                m.put(x, m.get(x) + 1);
            }
        }
//        h.show();
        int cnt = 0, sum = 0;
        for (int i = 0; i < 600; i++) {
            int y = m.get(i);
            sum += y * i;
            cnt += y;
            if (y < 3) continue;
            System.out.printf("%3d %3d\n", i, y);
        }
        System.out.printf("%6.3f\n\n", 1.0f * sum / cnt);
        cnt = 0;
        sum = 0;
        for (int i = 0; i < 600; i++) {
            int y = m2.get(i);
            sum += y * i;
            cnt += y;
            if (y < 3) continue;
            System.out.printf("%3d %3d\n", i, y);
        }
        System.out.printf("%6.3f", 1.0f * sum / cnt);
        Thread.sleep(2000);
    }
}
