package com.netcracker.profiler.util;

import static com.netcracker.profiler.Dumper.*;
import static com.netcracker.profiler.agent.PropertyFacadeBoot.getPropertyOrEnvVariable;

import com.netcracker.profiler.ServerNameResolver;
import com.netcracker.profiler.agent.ESCLogger;

import java.io.File;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VariableFinder {
    private static final ESCLogger escLogger = ESCLogger.getLogger(VariableFinder.class);

    public static String getNamespace() {
        String namespace;
        namespace = getPropertyOrEnvVariable(PARAM_CLOUD_NAMESPACE);
        if (namespace != null) return namespace;
        escLogger.log(ESCLogger.DEBUG, PARAM_CLOUD_NAMESPACE + " param was not found, trying using " + PARAM_NAMESPACE + " now", null);
        namespace = getPropertyOrEnvVariable(PARAM_NAMESPACE);
        if (namespace != null) return namespace;
        escLogger.log(ESCLogger.DEBUG, PARAM_NAMESPACE + " param was not found, trying using param from " + FILE_NAMESPACE + " now", null);
        namespace = getNamespaceFromFile();
        if (namespace != null) return namespace;
        else return "unknown";
    }

    public static String getServicename() {
        String servicename;
        servicename = getPropertyOrEnvVariable(PARAM_MICROSERVICE_NAME);
        if (servicename != null) return servicename;
        escLogger.log(ESCLogger.DEBUG, PARAM_MICROSERVICE_NAME + " param was not found, trying using " + PARAM_SERVICE_NAME + " now", null);
        servicename = getPropertyOrEnvVariable(PARAM_SERVICE_NAME);
        if (servicename != null) return servicename;
        escLogger.log(ESCLogger.DEBUG, PARAM_SERVICE_NAME + " param was not found, trying using Server Name RegEx now", null);
        servicename = getServicenameFromRegEx(getServerName());
        if (servicename != null) return servicename;
        else return "unknown";
    }
    public static String getNamespaceFromFile () {
        try {
            File namespaceFile = new File(FILE_NAMESPACE);
            Scanner namespaceReader = new Scanner(namespaceFile);
            return namespaceReader.nextLine();
        } catch (Exception e) {
            escLogger.log(ESCLogger.DEBUG, "Namespace was not found, using default value", null);
            return null;
        }
    }

    // Proxy method used for creating unit-tests
    public static String getServerName() {
        return ServerNameResolver.SERVER_NAME;
    }
    public static String getServicenameFromRegEx(String podName){
        Pattern pod_name_pattern = Pattern.compile("\\b(?<!-)(?<name>[a-z0-9]+(?:-[a-z0-9]+)*?)(?:-([a-f0-9]*))?-([a-z0-9]+)\\b(?!-)");
        Matcher matcher = pod_name_pattern.matcher(podName);
        if (matcher.find()) return matcher.group("name");
        else return null;
    }
}
