package com.netcracker.profiler.io.searchconditions;

import com.google.inject.assistedinject.Assisted;

import java.util.Date;

/**
 * Factory interface for creating BaseSearchConditions instances with runtime parameters.
 */
public interface BaseSearchConditionsFactory {
    BaseSearchConditions create(
            @Assisted("searchConditionsStr") String searchConditionsStr,
            @Assisted("dateFrom") Date dateFrom,
            @Assisted("dateTo") Date dateTo
    );
}
