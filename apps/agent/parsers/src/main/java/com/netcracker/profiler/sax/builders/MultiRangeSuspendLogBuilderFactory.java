package com.netcracker.profiler.sax.builders;

import com.google.inject.assistedinject.Assisted;

/**
 * Factory interface for creating MultiRangeSuspendLogBuilder instances.
 */
public interface MultiRangeSuspendLogBuilderFactory {
    MultiRangeSuspendLogBuilder create(
            @Assisted("rootReference") String rootReference,
            @Assisted("middleRangeStartTime") long middleRangeStartTime,
            @Assisted("middleRangeEndTime") long middleRangeEndTime);
}
