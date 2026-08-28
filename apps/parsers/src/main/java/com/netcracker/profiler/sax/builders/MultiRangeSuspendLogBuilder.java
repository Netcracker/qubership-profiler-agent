package com.netcracker.profiler.sax.builders;

import com.netcracker.profiler.io.SuspendLog;
import com.netcracker.profiler.sax.raw.SuspendLogVisitor;
import com.netcracker.profiler.util.ProfilerConstants;

import com.google.inject.assistedinject.Assisted;

import java.util.Arrays;
import java.util.function.Supplier;

import jakarta.inject.Inject;

/**
 * Prototype-scoped class - create instances via {@code MultiRangeSuspendLogBuilderFactory} or direct instantiation.
 */
public class MultiRangeSuspendLogBuilder extends SuspendLogVisitor implements Supplier<SuspendLog> {
    private static final int MAX_SIZE = 200000;
    private static final int DEFAULT_FIRST_RANGE_RATIO_PCT = 15;
    private static final int DEFAULT_MIDDLE_RANGE_RATIO_PCT = 70;
    private static final int DEFAULT_LAST_RANGE_RATIO_PCT = 15;

    private final long middleRangeStartTime;
    private final long middleRangeEndTime;
    private final SuspendLogBuilder firstRangeSuspendLogBuilder;
    private final SuspendLogBuilder middleRangeSuspendLogBuilder;
    private final SuspendLogBuilder lastRangeSuspendLogBuilder;
    private final SuspendLog log;

    @Inject
    public MultiRangeSuspendLogBuilder(
            @Assisted("rootReference") String rootReference,
            @Assisted("middleRangeStartTime") long middleRangeStartTime,
            @Assisted("middleRangeEndTime") long middleRangeEndTime) {
        this(1000, rootReference, middleRangeStartTime, middleRangeEndTime);
    }

    public MultiRangeSuspendLogBuilder(int size, String rootReference, long middleRangeStartTime, long middleRangeEndTime) {
        this(size, MAX_SIZE, rootReference, middleRangeStartTime, middleRangeEndTime);
    }

    public MultiRangeSuspendLogBuilder(int size, int maxSize, String rootReference, long middleRangeStartTime, long middleRangeEndTime) {
        this(ProfilerConstants.PROFILER_V1, size, maxSize, rootReference, middleRangeStartTime, middleRangeEndTime);
    }

    protected MultiRangeSuspendLogBuilder(int api, int size, int maxSize, String rootReference, long middleRangeStartTime, long middleRangeEndTime) {
        this(ProfilerConstants.PROFILER_V1, size, maxSize, rootReference, middleRangeStartTime, middleRangeEndTime,
                DEFAULT_FIRST_RANGE_RATIO_PCT, DEFAULT_MIDDLE_RANGE_RATIO_PCT, DEFAULT_LAST_RANGE_RATIO_PCT);
    }

    protected MultiRangeSuspendLogBuilder(
            int api, int size, int maxSize, String rootReference, long middleRangeStartTime, long middleRangeEndTime,
            int firstRangeRatioPct, int middleRangeRatioPct, int lastRangeRatioPct) {
        super(api);
        this.middleRangeStartTime = middleRangeStartTime;
        this.middleRangeEndTime = middleRangeEndTime;
        // Create SuspendLogBuilder instances directly instead of using ApplicationContext
        this.firstRangeSuspendLogBuilder = new SuspendLogBuilder(api, (size * firstRangeRatioPct)/100, (maxSize * firstRangeRatioPct)/100, rootReference);
        this.middleRangeSuspendLogBuilder = new SuspendLogBuilder(api, (size * middleRangeRatioPct)/100, (maxSize * middleRangeRatioPct)/100, rootReference);
        this.lastRangeSuspendLogBuilder = new SuspendLogBuilder(api, (size * lastRangeRatioPct)/100, (maxSize * lastRangeRatioPct)/100, rootReference);
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
