package com.netcracker.profiler.io;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Profile("filestorage")
public class EmptyActivePODReporter implements IActivePODReporter {
    @Override
    public List<ActivePODReport> reportActivePODs(String searchConditionsStr, TemporalRequestParams temporalRequestParams) {
        throw new RuntimeException("Not implemented");
    }
}
