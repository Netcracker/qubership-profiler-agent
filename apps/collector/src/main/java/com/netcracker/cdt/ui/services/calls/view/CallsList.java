package com.netcracker.cdt.ui.services.calls.view;

import java.util.List;
import java.util.function.Predicate;

import com.netcracker.cdt.ui.services.calls.models.CallRecord;
import com.netcracker.cdt.ui.services.calls.models.CallSeqResult;
import com.netcracker.cdt.ui.services.calls.tasks.ReloadTaskState;

public interface CallsList {

    void setState(ReloadTaskState state);

    void clear();

    boolean isEmpty();

    boolean isAlreadySorted(int sortIndex, boolean asc);

    long count();

    long count(Predicate<CallRecord> matcher);

    boolean append(CallSeqResult res);

    CallsList sortCalls(int indexToSort, boolean asc);

    List<CallRecord> filter(Predicate<CallRecord> matcher);

    List<CallRecord> filter(Predicate<CallRecord> matcher, int firstIndex, int limit);

    List<CallRecord> page(int page, int limit);

    List<CallRecord> all();
}
