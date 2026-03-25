package com.netcracker.cdt.ui.rest.v2.dto;

import com.google.common.collect.TreeMultimap;
import com.netcracker.cdt.ui.rest.ExceptionMappers;
import com.netcracker.cdt.ui.services.calls.CallsListRequest;
import com.netcracker.common.models.DurationRange;
import com.netcracker.common.models.StreamType;
import com.netcracker.common.models.TimeRange;
import com.netcracker.common.models.pod.IPodFilter;

import java.util.List;

import static com.netcracker.common.Consts.UI_COLUMNS;

public class Requests {

    public record Services(TimeRange timeRange, String query, List<Service> services) {
        public IPodFilter getFilter() {
            if (this.services != null && !this.services.isEmpty()) {
                var namespaces = TreeMultimap.<String, String>create();
                for (var s: this.services) {
                    namespaces.put(s.namespace, s.service);
                }

                // TODO add query

                return p -> {
                    if (!namespaces.containsKey(p.namespace())) return false;
                    var services = namespaces.get(p.namespace());
                    if (!services.contains(p.service())) return false;
                    if (!p.wasActive(timeRange.from(), timeRange.to())) return false;
                    return true;
                };
            }
            return p -> p.wasActive(timeRange.from(), timeRange.to());
        }
    }

    public record ServicePod(TimeRange timeRange, String query, int limit, int page) {
    }

    public record DumpDownload(StreamType type, TimeRange timeRange, String query, List<String> pods) {
    }

    public record CallsList(Parameters parameters, Filters filters, View view) {
        public record Parameters(String windowId, long clientUTC) {
        }
        public record Filters(TimeRange timeRange, DurationMs duration, String query, List<Service> services) {
        }
        public record View(int limit, int page, String sortColumn, boolean sortOrder) {
        }
        public boolean validate() {
            if (!filters().timeRange().isValid()) {
                throw new ExceptionMappers.InvalidRequest("invalid time range");
            }
            if (!filters().duration().isValid()) {
                throw new ExceptionMappers.InvalidRequest("invalid duration range");
            }
            if (view().page() <= 0 || view().limit() <= 0 || view().limit() > 10000) {
                throw new ExceptionMappers.InvalidRequest("invalid paging parameters");
            }
            var column = UI_COLUMNS.get(view().sortColumn());
            if (column == null) { // TODO
                throw new ExceptionMappers.InvalidRequest("invalid sorting column");
            }
            return true;
        }

        public CallsListRequest prepareSearchRequest() {
            var column = UI_COLUMNS.get(view().sortColumn());

            int limit = view().limit();
            int offset = Math.max(0, limit * (view().page() - 1));
            var search = new CallsListRequest(
                    parameters().windowId(),
                    parameters().clientUTC(),
                    filters().timeRange(),
                    filters().duration().asModel(),
                    filters().query(), "",
                    filters().services(),
                    offset, limit,
                    column, view().sortOrder() );
            return search;
        }
    }

    // internal utility
    public record Service(String namespace, String service) implements Comparable<Service> {
        @Override
        public int compareTo(Requests.Service o) {
            if (!namespace.equals(o.namespace)) {
                return namespace.compareTo(o.namespace);
            }
            return service.compareTo(o.service);
        }
    }

    public record DurationMs(int from, int to) {
        public boolean isValid() {
            return from >= 0 && to > 0 && from < to && to < 12*60*60*1000; // 12h for calls' duration is too much
        }

        public DurationRange asModel() {
            return DurationRange.ofMillis(from, to);
        }

        public static DurationMs of(DurationRange m) {
            return new DurationMs((int) m.from().toMillis(), (int) m.to().toMillis());
        }
    }
}
