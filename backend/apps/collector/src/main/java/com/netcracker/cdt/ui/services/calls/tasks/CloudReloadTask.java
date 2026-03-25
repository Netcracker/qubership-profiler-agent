package com.netcracker.cdt.ui.services.calls.tasks;

import com.netcracker.cdt.ui.rest.v2.dto.Requests;
import com.netcracker.cdt.ui.services.calls.CallsListRequest;
import com.netcracker.common.models.DurationRange;
import com.netcracker.common.models.TimeRange;
import com.netcracker.persistence.CallSequenceLoader;


import io.quarkus.logging.Log;
import java.util.List;

/**
 * The main orchestrator task to retrieve and filter calls after getting request
 * for search from UI
 * <br>
 */
public class CloudReloadTask implements ReloadTask {

    CallSequenceLoader cloud;

    private final String windowId;
    private final TimeRange period;
    private final DurationRange durationRange;
    private final List<Requests.Service> services;
    private final String queryFilter;

    private ReloadTaskState taskState; // main "context" (keep state for current search)

    public CloudReloadTask(CallsListRequest request, CallSequenceLoader cloud) {
        period = request.timeRange();
        windowId = request.windowId();
        durationRange = request.durationRange();
        services = request.services();
        queryFilter = request.query();
        this.cloud = cloud;
    }

    @Override
    public ReloadTaskState prepare() {
        taskState = new ReloadTaskState(windowId, 0);
        return this.taskState;
    }

    @Override
    public void run() {
        if (taskState.shouldStop())
            return; // against latecomers (after timeout, for example)
        try {

            var start = System.currentTimeMillis();

            this.taskState = cloud.getCallSequence(services, queryFilter, period, durationRange, taskState);
            taskState.finish();
            var ms = System.currentTimeMillis() - start;
            Log.infof("Returns %d results in %d ms, got all info from %d pods",
                    taskState.fetchedCalls(), ms, taskState.totalPods());
        } catch (Exception e) {
            Log.errorf(e, "problem?");
            throw new RuntimeException(e);
        }
    }
}
