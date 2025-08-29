package org.apache.tools.ant.taskdefs;

import org.qubership.profiler.agent.Profiler;

import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.Commandline;

public class ExecTask extends Task {
    protected Commandline cmdl;

    public static String cleanCmdLine$profiler(String cmdLine) {
        cmdLine = cmdLine.replaceAll("(?im)(pass[^=]*+=)\\S+", "$1***");
        int pos = cmdLine.indexOf("The \' characters around the executable and arguments are");
        if (pos != -1) {
            cmdLine = cmdLine.substring(0, pos - System.getProperty("line.separator").length());
        }
        if (cmdLine.startsWith("Executing 'sql")) {
            // Mask oracle password
            cmdLine = cmdLine.replaceAll("(?m)^'([^/]+/)[^@]+@", "$1***");
        }
        return cmdLine;
    }

    protected void dumpCommandLine$profiler() {
        Profiler.event(cleanCmdLine$profiler(cmdl.describeCommand()), "command.line.pre");
    }
}
