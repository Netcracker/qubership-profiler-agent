package com.netcracker.common.models.pod;

import org.apache.commons.lang.StringUtils;

public record PodName(String namespace, String service, String podName, String id) implements Comparable<PodName> {

    public boolean isEmpty() {
        return isEmpty(namespace) || isEmpty(service) || isEmpty(podName);
    }

    private static boolean isEmpty(String s) {
        return StringUtils.isEmpty(s) || "unknown".equals(s);
    }

    public String name() {
        return id;
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public int compareTo(PodName o) {
        if (!namespace.equals(o.namespace)) return namespace.compareTo(o.namespace);
        if (!service.equals(o.service)) return service.compareTo(o.service);
        if (!podName.equals(o.podName)) return podName.compareTo(o.podName);
        return 0;
    }

    public static PodName empty() {
        return new PodName("", "", "", "");
    }

    public static PodName of(String namespace, String service, String podName) {
        return new PodName(namespace, service, podName, podName);
        // return new PodId(namespace, service, podName, namespace + ':' + service + ':' + podName); // TODO: too lengthy
    }
}
