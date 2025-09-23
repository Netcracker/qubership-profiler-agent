package org.apache.tools.ant.taskdefs;

import com.netcracker.profiler.agent.JSHelper;
import com.netcracker.profiler.agent.Profiler;

import org.apache.tools.ant.Location;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Target;
import org.apache.tools.ant.Task;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Ant extends Task {
    private String antFile;
    private Vector targets;
    private Vector<Property> properties = new Vector<Property>();

    private void logEntry$profiler() {
        try {
            String method;
            switch (targets.size()) {
                case 0:
                    method = "default";
                    break;
                case 1:
                    method = targets.get(0).toString();
                    break;
                default:
                    StringBuilder sbStr = new StringBuilder();
                    for (int i = 0, il = targets.size(); i < il; i++) {
                        if (i > 0)
                            sbStr.append("___");
                        sbStr.append(targets.get(i));
                    }
                    method = sbStr.toString();
            }

            Project project = getProject();
            String currentFile = project.getProperty("ant.file");
            Location location = null;
            String targetFile = null;
            if (currentFile != null && currentFile.equals(antFile)) {
                Target target = project.getTargets().get(method);
                if (target != null) {
                    location = target.getLocation();
                    if (location != null) {
                        targetFile = location.getFileName();
                    }
                }

            }
            if (location == null) {
                location = getLocation();
                targetFile = antFile;
            }
            if (targetFile == null || targetFile.length() == 0) {
                targetFile = "unknown";
            }
            if (targetFile.endsWith(".xml")) {
                targetFile = targetFile.substring(0, targetFile.length() - 4);
            }
            if (targetFile.startsWith("/")) {
                targetFile = targetFile.substring(1);
            }
            targetFile = targetFile
                    .replace('.', '_')
                    .replace('/', '.');

            String locationString = locationToString$profiler(location);
            method = method.replace('.', '_');
            Profiler.enter("void " + targetFile + "." + method + "() (" + locationString + ") [unknown jar]");
            if (properties.size() == 0) {
                return;
            }
            if (properties.size() == 1 && "install.step".equals(properties.get(0).getName()) && method.startsWith("step")) {
                // Ignore install.step=42 kind of parameters
                return;
            }
            StringWriter sw = new StringWriter();
            sw.append('{');
            boolean needComma = false;
            Pattern pass = Pattern.compile("pass", Pattern.CASE_INSENSITIVE);
            Matcher passFinder = pass.matcher("");
            for (Property p : properties) {
                try {
                    if (needComma) {
                        sw.append(',');
                    }
                    needComma = true;
                    sw.append('"');
                    JSHelper.escapeJS(sw, p.getName());
                    sw.append("\":\"");
                    passFinder.reset(p.getName());
                    if (passFinder.find()) {
                        sw.append("***");
                    } else {
                        JSHelper.escapeJS(sw, p.getValue());
                    }
                    sw.append('"');

                    if ("install_zip".equals(method) && "zip_name".equals(p.getName())) {
                        Profiler.event(p.getValue(), "ai.zip");
                        return;
                    }
                } catch (IOException e) {
                    // ignore
                }
            }
            sw.append('}');
            Profiler.event(sw.toString(), "antcall.json");
        } catch (Throwable t) {
            Profiler.enter("void null.null() (null) [unknown jar]");
            throw t;
        }
    }

    public static String locationToString$profiler(Location location) {
        String result = location == null ? null : location.toString();
        if (result == null || result.length() == 0) {
            result = "Ant.java:0";
        } else if (result.length() > 5) {
            result = result.substring(0, result.length() - 2);
        }
        return result;
    }

    private void logExit$profiler(Throwable t) {
        logExit$profiler();
    }

    private void logExit$profiler() {
        Profiler.exit();
    }
}
