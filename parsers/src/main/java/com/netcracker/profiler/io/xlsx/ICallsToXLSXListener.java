package com.netcracker.profiler.io.xlsx;

import com.netcracker.profiler.io.CallListener;

public interface ICallsToXLSXListener extends CallListener {
    void postProcess();
    void processError(Throwable ex);
}
