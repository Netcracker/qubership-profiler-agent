package com.netcracker.cdt.ui.services.calls.view;

import com.netcracker.cdt.ui.services.calls.models.CallRecord;
import com.netcracker.cdt.ui.services.calls.models.CallSeqResult;
import com.netcracker.cdt.ui.services.calls.tasks.ReloadTaskState;
import io.quarkus.logging.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Predicate;

/**
 * Entity to keep and sort found calls.
 * <br>
 * It is reusable by `clientWindow`: it will be cleared for the next search from same user.
 */
public class LocalCallsList implements CallsList{
    private int sortedIndex;
    private boolean sortAsc;
    private ConcurrentSkipListSet<CallRecord> calls;
    private ReloadTaskState state;

    private LocalCallsList(int lastSortedIndex, boolean asc) {
        this.sortedIndex = lastSortedIndex;
        this.sortAsc = asc;
        this.calls = new ConcurrentSkipListSet<>(CallRecord.comparator(sortedIndex, sortAsc));
    }

    public static LocalCallsList create() { // with default sorting
        return new LocalCallsList(0, false);
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
    public synchronized long count() {
        return calls.size();
    }

    @Override
    public synchronized long count(Predicate<CallRecord> matcher) {
        return calls.stream().filter(matcher).count();
    }

    @Override
    public synchronized boolean append(CallSeqResult res) {
        // only successful results
        var filtered = res.calls().toList();
        Log.debugf("[%s] got filtered %d calls from %d parsed", res.subTask(), filtered.size(), res.parsedCalls());
        calls.addAll(filtered);

        var shouldStop = state.append(res, filtered.size());
        return shouldStop;
    }

    @Override
    public synchronized CallsList sortCalls(int indexToSort, boolean asc) {
        if (isAlreadySorted(indexToSort, asc)
                || !CallRecord.isComparable(indexToSort)) {
            return this;
        }

        sortedIndex = indexToSort; sortAsc = asc;
        var reSorted = new ConcurrentSkipListSet<>(CallRecord.comparator(sortedIndex, sortAsc));
        reSorted.addAll(this.calls);

        calls = reSorted;
        return this;
    }

    @Override
    public synchronized List<CallRecord> filter(Predicate<CallRecord> matcher) {
        return calls.stream().filter(matcher).toList();
    }

    @Override
    public synchronized List<CallRecord> filter(Predicate<CallRecord> matcher, int firstIndex, int limit) {
        if (firstIndex < 0) {
            return List.of();
        }
        return calls.stream().filter(matcher).skip(firstIndex).limit(limit).toList();
    }

    @Override
    public synchronized List<CallRecord> page(int page, int limit) {
        var start = (page - 1) * limit;
        if (start < 0) {
            return List.of();
        }
        return calls.stream().skip(start).limit(limit).toList();
    }

    @Override
    public synchronized List<CallRecord> all() {
        return new ArrayList<>(calls);
    }

}
