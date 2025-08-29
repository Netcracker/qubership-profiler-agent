package org.apache.tools.ant;

import org.qubership.profiler.agent.Profiler;

import java.util.Hashtable;

public class Project {
    public native String getProperty(String propertyName);
    public native Hashtable<String, Target> getTargets();

    private void logEntry$profiler(String targetName) {
        Profiler.enter("void org.apache.tools.ant.Project_." + targetName + "() (Project.java:1362) [unknown jar]");
    }

    private void logExit$profiler(Throwable t) {
        logExit$profiler();
    }

    private void logExit$profiler() {
        Profiler.exit();
    }
}
