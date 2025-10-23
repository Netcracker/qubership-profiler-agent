package com.netcracker.common.models.pod;

import com.netcracker.common.utils.Utils;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

public record Namespace(String namespace, SortedMap<String, Service> services) {
        public  static Namespace create(String namespace) {
            return new Namespace(namespace, new TreeMap<>());
        }

        public void put(Service s) {
            services.put(s.getServiceName(), s);
        }

        public Service computeIfAbsent(String key, Function<? super String, ? extends Service> mappingFunction) {
            return services.computeIfAbsent(key, mappingFunction);
        }

        public boolean isValid() {
            return namespace != null && !namespace.isEmpty() && !Utils.EMPTY.equals(namespace);
        }

        public String getNamespace() {
            return namespace;
        }

        public Collection<Service> getServices() {
            return services.values();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return namespace.equals(((Namespace) o).namespace());
        }

        @Override
        public int hashCode() {
            return namespace.hashCode();
        }

        @Override
        public String toString() {
            return "Namespace[" + namespace + ": " + getServices() + ']';
        }
}
