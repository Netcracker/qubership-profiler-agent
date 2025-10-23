package com.netcracker.persistence;

import java.util.List;

import com.netcracker.cdt.ui.rest.v2.dto.Requests;
import com.netcracker.cdt.ui.services.calls.tasks.ReloadTaskState;
import com.netcracker.common.models.DurationRange;
import com.netcracker.common.models.TimeRange;

public interface CallSequenceLoader {
    ReloadTaskState getCallSequence(List<Requests.Service> services, String queryFilter, TimeRange range,
            DurationRange durationRange, ReloadTaskState reloadTaskState);
}
