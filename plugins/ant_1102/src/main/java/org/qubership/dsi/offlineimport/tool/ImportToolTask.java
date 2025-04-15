package org.qubership.dsi.offlineimport.tool;

import org.qubership.profiler.agent.Profiler;

import org.apache.tools.ant.Task;

public class ImportToolTask extends Task {
    public native String getImportFile();

    void logImportFile$profiler() {
        Profiler.event(getImportFile(), "ai.zip");
    }
}
