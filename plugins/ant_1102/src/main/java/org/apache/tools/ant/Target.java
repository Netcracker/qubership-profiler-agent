package org.apache.tools.ant;

import org.qubership.profiler.agent.Profiler;

import org.apache.tools.ant.taskdefs.Ant;

public class Target {
    public native String getName();
    public native Location getLocation();
    public native Project getProject();

    private void logEntry$profiler() {
        try {
            String location = Ant.locationToString$profiler(getLocation());
            String name = getName();
            if (name == null || name.length() == 0) {
                name = "global";
            }
            name = name.replace('.', '_');
            Profiler.enter("void org.apache.tools.ant.Target_." + name + "() (" + location + ") [unknown jar]");
            if (System.getProperty("execution-statistics-collector.instrument.ant.java") == null) {
                Project project = getProject();
                String antJava = project.getProperty("execution-statistics-collector.instrument.ant.java");
                if (antJava == null) {
                    antJava = project.getProperty("e-s-c.instrument.ant.java");
                }
                if (antJava == null) {
                    antJava = project.getProperty("profiler.instrument.ant.java");
                }
                if (antJava == null) {
                    antJava = project.getProperty("instrument.ant.java");
                }
                System.setProperty("execution-statistics-collector.instrument.ant.java", Boolean.valueOf(antJava).toString());
            }
            if ("StartInstall".equals(name)) {
                Project p = getProject();
                Profiler.event(p.getProperty("package_name"), "ai.package");
            }
        } catch (Throwable t) {
            Profiler.enter("void org.apache.tools.ant.Target_.null() (null) [unknown jar]");
            throw t;
        }
    }

    private void logExit$profiler(Throwable t) {
        logExit$profiler();
    }

    private void logExit$profiler() {
        Profiler.exit();
    }
}
