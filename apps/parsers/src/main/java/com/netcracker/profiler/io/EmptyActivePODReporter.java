package com.netcracker.profiler.io;

import java.util.List;

import jakarta.inject.Singleton;

@Singleton
public class EmptyActivePODReporter implements IActivePODReporter {
    @Override
    public List<ActivePODReport> reportActivePODs(String searchConditionsStr, TemporalRequestParams temporalRequestParams) {
        throw new RuntimeException("Not implemented");
    }
}
