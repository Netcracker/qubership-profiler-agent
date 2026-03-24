package com.netcracker.cdt.ui.services.calls.view;

import com.netcracker.cdt.ui.models.UiServiceConfig;
import com.netcracker.cdt.ui.services.calls.CallsListRequest;
import com.netcracker.cdt.ui.services.calls.CallsListResult;
import com.netcracker.cdt.ui.services.calls.models.CallRecord;
import com.netcracker.cdt.ui.services.calls.tasks.CallsMetaLoader;
import com.netcracker.cdt.ui.services.calls.tasks.ReloadTask;
import com.netcracker.cdt.ui.services.calls.tasks.ReloadTaskState;
import com.netcracker.common.utils.DB;
import io.quarkus.logging.Log;

import java.util.*;
import java.util.function.Predicate;

public class ClientWindowInfo {
    private final UiServiceConfig config;
    private final String windowId;

    private String searchParamsHash;
    private CallsListRequest lastRequest;
    private ReloadTaskState lastSearch;
    private CallsList calls; // list of (enriched) calls from last request

    public ClientWindowInfo(UiServiceConfig config, String windowId) {
        this.config = config;
        this.windowId = windowId;
        this.calls = LocalCallsList.create();
    }

    @DB("reloadData")
    public synchronized void reloadData(CallsMetaLoader metaLoader, CallsListRequest newRequest, int concurrent) {
        lastRequest = newRequest;
        searchParamsHash = lastRequest.searchHash();
        calls.clear();

        Log.infof("[%s] Reloading data for window: %s", windowId, this.toString());

        ReloadTask task = metaLoader.createTask(metaLoader, newRequest, concurrent); // create new Object by comparing Persistence
        lastSearch = task.prepare()
                .uiLimits(config.getUiMaxLimit(), config.getUiFirstPage())
                .timeout(config.getMaxRequestTime());
        calls.setState(lastSearch);
        task.run();
    }

    public String searchHash() { // to differentiate search results
        return searchParamsHash;
    }

    public String displayHash() { // to differentiate response (sort order, etc.) of already found data
        return String.format("%s_%s_%d_%b", searchParamsHash,
                lastRequest.query(), lastRequest.sortColumn(), lastRequest.sortOrder());
    }

    public CallsListResult asResponse(CallsListRequest request) {
        calls = lastSearch.getCallsList();
        var startTime = System.currentTimeMillis();
        calls.sortCalls(request.sortColumn(), request.sortOrder());
        Log.infof("[%s] Sorted %d calls in %d ms. Sort index: %d, asc? %b",
                windowId, calls.count(), System.currentTimeMillis() - startTime,
                request.sortColumn(), request.sortOrder());

        // filter for already enriched CallRecord
        Predicate<CallRecord> filter = UiCallRecordFilter.create(request.query())::filter;

        long total = calls.count(filter);
        List<CallRecord> res = calls.filter(filter, request.beginIndex(), request.pageSize());

        var status = lastSearch != null ? lastSearch.getStatus(total) : CallsListResult.emptyStatus();

        return new CallsListResult(displayHash(), status, res);
    }

    @Override
    public String toString() {
        return String.format("Window '%s'. Dates: [%s], durations: [%s] . Pod filter: %s, query: %s",
                windowId,
                lastRequest.timeRange(), lastRequest.durationRange(),
                lastRequest.podFilter(), lastRequest.query());
    }

}
