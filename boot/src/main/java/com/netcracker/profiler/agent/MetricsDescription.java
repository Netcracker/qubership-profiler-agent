package com.netcracker.profiler.agent;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class MetricsDescription {
    private String name;
    private Map<String, String> parameters = new HashMap<String, String>();

    public MetricsDescription(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameter(String key, String value) {
        parameters.put(key, value);
    }

    public void print(PrintWriter out) {
        if (name == null) {
            return;
        }

        out.println(name);

        if (parameters == null) {
            out.println("No params");
            return;
        }

        for (Map.Entry<String, String> param : parameters.entrySet()) {
            out.println(param.getKey() + " " + param.getValue());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricsDescription that = (MetricsDescription) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return parameters != null ? parameters.equals(that.parameters) : that.parameters == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (parameters != null ? parameters.hashCode() : 0);
        return result;
    }
}
