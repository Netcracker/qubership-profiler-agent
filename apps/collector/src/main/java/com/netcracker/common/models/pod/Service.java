package com.netcracker.common.models.pod;

import com.netcracker.common.utils.Utils;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

public record Service(String namespace, String serviceName, SortedMap<String, PodInfo> pods) {
    public static Service create(String namespace, String serviceName) {
        return new Service(namespace, serviceName, new TreeMap<>());
    }

    public void put(PodInfo s) {
        pods.put(s.podName(), s);
    }

    public PodInfo computeIfAbsent(String key, Function<? super String, ? extends PodInfo> mappingFunction) {
        return pods.computeIfAbsent(key, mappingFunction);
    }

    public boolean isValid() {
        return namespace != null && !namespace.isEmpty() && !Utils.EMPTY.equals(namespace) &&
                serviceName != null && !serviceName.isEmpty() && !Utils.EMPTY.equals(serviceName);
    }

    public String getNamespace() {
        return namespace;
    }

    public String getServiceName() {
        return serviceName;
    }

    public Collection<PodInfo> getPods() {
        return pods.values();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Service service = (Service) o;
        if (!namespace.equals(service.namespace)) return false;
        return serviceName.equals(service.serviceName);
    }

    @Override
    public int hashCode() {
        int result = namespace.hashCode();
        result = 31 * result + serviceName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Service[{" + namespace + "," + serviceName + "}: " + getPods() + ']';
    }

}