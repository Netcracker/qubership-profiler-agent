package org.qubership.profiler.sax.builders;

import org.qubership.profiler.chart.Provider;
import org.qubership.profiler.io.SuspendLog;
import org.qubership.profiler.sax.raw.SuspendLogVisitor;
import org.qubership.profiler.util.ProfilerConstants;

import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@Scope("prototype")
@Profile("filestorage")
public class SuspendLogBuilder extends SuspendLogVisitor implements Provider<SuspendLog> {
    private static int MAX_SIZE = 100000;

    protected SuspendLog log;

    protected long[] dates;
    protected int[] delays;
    protected int[] trueDelays;
    protected int size;
    protected int maxSize;
    protected String rootReference;

    public SuspendLogBuilder(String rootReference) {
        this(1000, rootReference);
    }

    public SuspendLogBuilder(int size, String rootReference) {
        this(size, MAX_SIZE, rootReference);
    }

    public SuspendLogBuilder(int size, int maxSize, String rootReference) {
        this(ProfilerConstants.PROFILER_V1, size, maxSize, rootReference);
    }

    protected SuspendLogBuilder(int api, int size, int maxSize, String rootReference) {
        super(api);
        this.size = 0;
        this.maxSize = maxSize;
        this.dates = new long[size];
        this.delays = new int[size];
        this.rootReference = rootReference;
    }

    @PostConstruct
    public void initLog(){
        this.log = new SuspendLog(new long[0], new int[0]);
    }

    @Override
    public void visitHiccup(long date, int delay) {
        ensureCapacity();
        dates[size] = date;
        delays[size] = delay;
        if (trueDelays != null) {
            trueDelays[size] = delay;
        }
        size++;
    }

    public void visitEnd() {
        if (dates.length != size) {
            realloc(size);
        }

        log.setValue(dates, delays, trueDelays);
    }

    private void ensureCapacity() {
        if (dates.length > size)
            return;

        if (2 * dates.length > maxSize && dates.length % 2 == 0) {
            compress();
            return;
        }

        realloc(dates.length * 2);
    }

    private void compress() {
        if (trueDelays == null) {
            trueDelays = new int[size];
            System.arraycopy(delays, 0, trueDelays, 0, size);
        }
        for (int i = 0; i < size / 2; i++) {
            trueDelays[i] = trueDelays[2 * i] + trueDelays[2 * i + 1];
            delays[i] = (int) (delays[2 * i] + dates[2 * i + 1] - dates[2 * i]);
            dates[i] = dates[i * 2 + 1];
        }
        size /= 2;
    }

    private void realloc(int newSize) {
        long[] newDates = new long[newSize];
        int[] newDelays = new int[newSize];

        int min = Math.min(newSize, dates.length);
        System.arraycopy(dates, 0, newDates, 0, min);
        System.arraycopy(delays, 0, newDelays, 0, Math.min(newSize, delays.length));

        if (trueDelays != null) {
            int[] newTrueDelays = new int[newSize];
            System.arraycopy(trueDelays, 0, newTrueDelays, 0, Math.min(newSize, trueDelays.length));
            trueDelays = newTrueDelays;
        }

        dates = newDates;
        delays = newDelays;
    }

    public SuspendLog get() {
        return log;
    }
}
