package org.qubership.profiler.sax.builders;

import org.qubership.profiler.chart.Provider;
import org.qubership.profiler.io.SuspendLog;
import org.qubership.profiler.sax.raw.SuspendLogVisitor;
import org.qubership.profiler.util.ProfilerConstants;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
@Scope("prototype")
@Profile("filestorage")
public class MultiRangeSuspendLogBuilder extends SuspendLogVisitor implements Provider<SuspendLog> {
    private static int MAX_SIZE = 200000;
    private static int DEFAULT_FIRST_RANGE_RATIO_PCT = 15;
    private static int DEFAULT_MIDDLE_RANGE_RATIO_PCT = 70;
    private static int DEFAULT_LAST_RANGE_RATIO_PCT = 15;

    private long middleRangeStartTime;
    private long middleRangeEndTime;
    private SuspendLogBuilder firstRangeSuspendLogBuilder;
    private SuspendLogBuilder middleRangeSuspendLogBuilder;
    private SuspendLogBuilder lastRangeSuspendLogBuilder;
    private SuspendLog log;

    public MultiRangeSuspendLogBuilder(String rootReference, long middleRangeStartTime, long middleRangeEndTime, ApplicationContext context) {
        this(1000, rootReference, middleRangeStartTime, middleRangeEndTime, context);
    }

    public MultiRangeSuspendLogBuilder(int size, String rootReference, long middleRangeStartTime, long middleRangeEndTime, ApplicationContext context) {
        this(size, MAX_SIZE, rootReference, middleRangeStartTime, middleRangeEndTime, context);
    }

    public MultiRangeSuspendLogBuilder(int size, int maxSize, String rootReference, long middleRangeStartTime, long middleRangeEndTime, ApplicationContext context) {
        this(ProfilerConstants.PROFILER_V1, size, maxSize, rootReference, middleRangeStartTime, middleRangeEndTime, context);
    }

    protected MultiRangeSuspendLogBuilder(int api, int size, int maxSize, String rootReference, long middleRangeStartTime, long middleRangeEndTime,
                                          ApplicationContext context) {
        this(ProfilerConstants.PROFILER_V1, size, maxSize, rootReference, middleRangeStartTime, middleRangeEndTime,
                DEFAULT_FIRST_RANGE_RATIO_PCT, DEFAULT_MIDDLE_RANGE_RATIO_PCT, DEFAULT_LAST_RANGE_RATIO_PCT, context);
    }

    protected MultiRangeSuspendLogBuilder(int api, int size, int maxSize, String rootReference, long middleRangeStartTime, long middleRangeEndTime,
                                          int firstRangeRatioPct, int middleRangeRatioPct, int lastRangeRatioPct, ApplicationContext context) {
        super(api);
        this.middleRangeStartTime = middleRangeStartTime;
        this.middleRangeEndTime = middleRangeEndTime;
        this.firstRangeSuspendLogBuilder = context.getBean(SuspendLogBuilder.class, api, (size * firstRangeRatioPct)/100, (maxSize * firstRangeRatioPct)/100, rootReference);
        this.middleRangeSuspendLogBuilder = context.getBean(SuspendLogBuilder.class, api, (size * middleRangeRatioPct)/100, (maxSize * middleRangeRatioPct)/100, rootReference);
        this.lastRangeSuspendLogBuilder = context.getBean(SuspendLogBuilder.class, api, (size * lastRangeRatioPct)/100, (maxSize * lastRangeRatioPct)/100, rootReference);
        this.log = new SuspendLog(new long[size], new int[size]);
    }

    @Override
    public void visitHiccup(long date, int delay) {
        if(date < middleRangeStartTime) {
            firstRangeSuspendLogBuilder.visitHiccup(date, delay);
        } else if(date > middleRangeEndTime) {
            lastRangeSuspendLogBuilder.visitHiccup(date, delay);
        } else {
            middleRangeSuspendLogBuilder.visitHiccup(date, delay);
        }
    }

    @Override
    public void visitEnd() {
        int s1 = firstRangeSuspendLogBuilder.size;
        int s2 = middleRangeSuspendLogBuilder.size;
        int s3 = lastRangeSuspendLogBuilder.size;

        int size = s1 + s2 + s3;
        long[] dates = new long[size];
        int[] delays = new int[size];
        int[] trueDelays = new int[size];

        System.arraycopy(firstRangeSuspendLogBuilder.dates, 0, dates, 0, s1);
        System.arraycopy(middleRangeSuspendLogBuilder.dates, 0, dates, s1, s2);
        System.arraycopy(lastRangeSuspendLogBuilder.dates, 0, dates, (s1 + s2), s3);

        System.arraycopy(firstRangeSuspendLogBuilder.delays, 0, delays, 0, s1);
        System.arraycopy(middleRangeSuspendLogBuilder.delays, 0, delays, s1, s2);
        System.arraycopy(lastRangeSuspendLogBuilder.delays, 0, delays, (s1 + s2), s3);

        copyTrueDelays(firstRangeSuspendLogBuilder.trueDelays, trueDelays, 0, s1);
        copyTrueDelays(middleRangeSuspendLogBuilder.trueDelays, trueDelays, s1, s2);
        copyTrueDelays(lastRangeSuspendLogBuilder.trueDelays, trueDelays, (s1 + s2), s3);

        log.setValue(dates, delays, trueDelays);
    }

    private void copyTrueDelays(int[] src, int[] dest, int destPos, int length) {
        if(src == null) {
            Arrays.fill(dest, destPos, destPos + length, -1);
        } else {
            System.arraycopy(src, 0, dest, destPos, length);
        }
    }

    @Override
    public SuspendLog get() {
        return log;
    }
}
