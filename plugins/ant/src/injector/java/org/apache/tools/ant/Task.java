package org.apache.tools.ant;

import com.netcracker.profiler.agent.Profiler;

import org.apache.tools.ant.taskdefs.Ant;

public class Task {
    public native String getTaskName();

    public native String getTaskType();

    public native Location getLocation();

    public native Project getProject();

    public native void log(String msg);

    private boolean shouldIgnore$profiler() {
//        return "org.apache.tools.ant.taskdefs.CallTarget".equals(getClass().getName());
        return "antcall".equals(getTaskName());
    }

    private void logEntry$profiler() {
        try {
            if (shouldIgnore$profiler()) {
                return;
            }
            String location = Ant.locationToString$profiler(getLocation());
            String taskName = getTaskName();
            taskName = taskName.replace('.', '_');
            Profiler.enter("void org.apache.tools.ant.Task_." + taskName + "() (" + location + ") [unknown jar]");
        } catch (Throwable t) {
            Profiler.enter("void org.apache.tools.ant.Task_.null() (null) [unknown jar]");
            throw t;
        }
    }

    private void logExit$profiler(Throwable t) {
        logExit$profiler();
    }

    private void logExit$profiler() {
        if (shouldIgnore$profiler()) {
            return;
        }
        Profiler.exit();
    }
}
