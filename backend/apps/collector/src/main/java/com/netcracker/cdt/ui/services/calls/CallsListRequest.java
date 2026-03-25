package com.netcracker.cdt.ui.services.calls;

import com.google.common.collect.TreeMultimap;
import com.netcracker.cdt.ui.rest.v2.dto.Requests;
import com.netcracker.common.models.DurationRange;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.IPodFilter;
import com.netcracker.common.models.pod.PodInfo;
import com.netcracker.common.search.SearchConditions;

import java.util.List;

public record CallsListRequest(
        String windowId,
        long clientUTC,

        TimeRange timeRange,
        DurationRange durationRange,

        String query,
        String podFilter,
        List<Requests.Service> services,

        int beginIndex, int pageSize,
        int sortColumn, boolean sortOrder
) {

    // hash to differentiate requests from the user:
    // change of time-range, duration or pods list should trigger another search
    public String searchHash() {
        return timeRange.hash() + "_" + durationRange.hash() + "_" + podFilter;
    }

    public CallsListRequest fixUTCRange() {
        return new CallsListRequest(windowId, -1,
                timeRange.alignWithClient(clientUTC), durationRange,
                query, podFilter, services,
                beginIndex, pageSize, sortColumn, sortOrder );
    }

    public CallsListRequest overrideLimit(int offset, int limit) {
        return new CallsListRequest(windowId, -1,
                timeRange.alignWithClient(clientUTC), durationRange,
                query, podFilter, services,
                offset, limit, sortColumn, sortOrder );
    }

    public IPodFilter getServicesFilter() {
        if (this.services != null && !this.services.isEmpty()) {
            var namespaces = TreeMultimap.<String, String>create();
            for (var s: this.services) {
                namespaces.put(s.namespace(), s.service());
            }
            return p -> {
                if (!namespaces.containsKey(p.namespace())) return false;
                var services = namespaces.get(p.namespace());
                if (!services.contains(p.service())) return false;
                return true;
            };
        }
        // TODO add podFilter
        // else
        return p -> p.wasActive(timeRange.from(), timeRange.to());
    }

    public String getPodFilter() {
        // TODO replace to IPodFilter
        // TODO use filters from `query` too
        return podFilter;
    }

    public List<PodInfo> filterPods(List<PodInfo> activePods) {
        if (!services().isEmpty()) {
            return activePods.stream().filter(getServicesFilter()).toList();
        } else if (!podFilter().isEmpty()) {
            return SearchConditions.filter(activePods, getPodFilter());
        } else {
            return activePods;
        }
    }
}
