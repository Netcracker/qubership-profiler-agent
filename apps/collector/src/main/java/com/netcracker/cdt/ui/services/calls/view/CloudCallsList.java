package com.netcracker.cdt.ui.services.calls.view;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Predicate;

import com.netcracker.cdt.ui.services.calls.models.CallRecord;
import com.netcracker.cdt.ui.services.calls.models.CallSeqResult;
import com.netcracker.cdt.ui.services.calls.tasks.ReloadTaskState;

import io.quarkus.logging.Log;

public class CloudCallsList implements CallsList {
    private int sortedIndex;
    private boolean sortAsc;
    private ConcurrentSkipListSet<CallRecord> calls;
    // TODO check if ArrayList is suitable for this according to
    // performance and all aspects.
    private ReloadTaskState state;

    private CloudCallsList(int lastSortedIndex, boolean asc) {
        this.sortedIndex = lastSortedIndex;
        this.sortAsc = asc;
        this.calls = new ConcurrentSkipListSet<>(CallRecord.comparator(sortedIndex, sortAsc));
    }

    public static CloudCallsList create() { // with default sorting
        return new CloudCallsList(0, false);
    }

    @Override
    public void setState(ReloadTaskState state) { // should use latest search state if the user runs new search
        this.state = state;
        state.setCallsList(this);
    }

    @Override
    public void clear() {
        calls.clear();
    }

    @Override
    public boolean isEmpty() {
        return calls.isEmpty();
    }

    @Override
    public boolean isAlreadySorted(int sortIndex, boolean asc) {
        if (calls.isEmpty()) {
            return true;
        }
        return this.sortedIndex == sortIndex && this.sortAsc == asc;
    }

    @Override
    public long count() {
        return calls.size();
    }

    @Override
    public synchronized long count(Predicate<CallRecord> matcher) {
        return count();
    }

    @Override
    public synchronized boolean append(CallSeqResult res) {
        // only successful results
        var filtered = res.calls().toList();
        Log.debugf("[%s] got filtered %d calls from %d parsed", res.subTask(), filtered.size(), res.parsedCalls());
        calls.addAll(filtered);

        return state.append(res, filtered.size());
    }

    @Override
    public synchronized CloudCallsList sortCalls(int indexToSort, boolean asc) {
        if (isAlreadySorted(indexToSort, asc)
                || !CallRecord.isComparable(indexToSort)) {
            return this;
        }

        sortedIndex = indexToSort;
        sortAsc = asc;
        var reSorted = new ConcurrentSkipListSet<>(CallRecord.comparator(sortedIndex, sortAsc));
        reSorted.addAll(this.calls);

        calls = reSorted;
        return this;
    }

    @Override
    public synchronized List<CallRecord> filter(Predicate<CallRecord> matcher) {
        return new ArrayList<>(calls);
    }

    @Override
    public synchronized List<CallRecord> filter(Predicate<CallRecord> matcher, int firstIndex, int limit) {
        if (firstIndex < 0) {
            return List.of();
        }
        List<CallRecord> list = filter(matcher);
        return list.subList(firstIndex, Math.min(limit, list.size()));
    }

    @Override
    public synchronized List<CallRecord> page(int page, int limit) {
        var start = (page - 1) * limit;
        if (start < 0) {
            return List.of();
        }
        ArrayList<CallRecord> list = new ArrayList<>(calls);
        return list.subList(start, Math.min(limit, list.size()));
    }

    @Override
    public synchronized List<CallRecord> all() {
        return new ArrayList<>(calls);
    }

    public synchronized void setCalls(List<CallRecord> callRecords) {
        calls.addAll(callRecords);
    }

}
