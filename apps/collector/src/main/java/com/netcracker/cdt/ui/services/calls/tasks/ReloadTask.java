package com.netcracker.cdt.ui.services.calls.tasks;

public interface ReloadTask {

    void run();

    ReloadTaskState prepare();
}
