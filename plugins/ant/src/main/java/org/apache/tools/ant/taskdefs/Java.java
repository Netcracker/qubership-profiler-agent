package org.apache.tools.ant.taskdefs;

import static org.apache.tools.ant.taskdefs.ExecTask.cleanCmdLine$profiler;

import org.qubership.profiler.agent.DumpRootResolverAgent;
import org.qubership.profiler.agent.Profiler;

import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.CommandlineJava;

import java.io.File;

public class Java extends Task {
    private boolean fork;

    public native CommandlineJava getCommandLine();

    protected void dumpCommandLine$profiler() {
        CommandlineJava cmdl = getCommandLine();
        String cmdLine = cmdl.toString();
        if (fork && !cmdLine.contains("javaagent")
                && Boolean.getBoolean("execution-statistics-collector.instrument.ant.java")) {
            // add ESC
            File agentJar = new File(new File(new File("applications", "execution-statistics-collector"), "lib"), "agent.jar");
            if (agentJar.exists()) {
                cmdl.createVmArgument().setValue("-javaagent:" + agentJar.getAbsolutePath());
                String configXmlPath = new File(DumpRootResolverAgent.CONFIG_FILE).getAbsolutePath();
                cmdl.createVmArgument().setValue("-Dexecution-statistics-collector.config=" + configXmlPath);

                String escServerName;
                if (cmdl.getJar() != null) {
                    escServerName = new File(cmdl.getJar()).getName();
                    if (escServerName.endsWith(".jar")) {
                        escServerName = escServerName.substring(0, escServerName.length() - 4);
                    }
                } else if (cmdl.getClassname() != null) {
                    escServerName = cmdl.getClassname();
                    int lastDot = escServerName.lastIndexOf('.');
                    if (lastDot != -1) {
                        escServerName = escServerName.substring(lastDot + 1);
                    }
                } else {
                    escServerName = "antjava";
                }
                // Avoid invalid characters so e-s-c/dump/${serverName} would work
                escServerName = escServerName.replaceAll("[^A-Za-z0-9-]+", "_");
                if ("WLSTInterpreterInvoker".equals(escServerName)) {
                    cmdl.createVmArgument().setValue("-XX:TieredStopAtLevel=1");
                }
                cmdl.createVmArgument().setValue("-Dorg.qubership.esc.serverName=" + escServerName);
            }
        }
        Profiler.event(cleanCmdLine$profiler(cmdl.describeCommand()), "command.line.pre");
    }
}
