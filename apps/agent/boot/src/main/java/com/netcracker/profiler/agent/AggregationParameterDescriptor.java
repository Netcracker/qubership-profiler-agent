package com.netcracker.profiler.agent;

public class AggregationParameterDescriptor {

    private String name;
    private String displayName;

    public AggregationParameterDescriptor(String name) {
        this.name = name;
        this.displayName = name;
    }

    public AggregationParameterDescriptor(String name, String displayName) {
        this.name = name;
        this.displayName = displayName;
    }

    public String getName() {
        return name;
    }

    public String getDisplayName() {
        return displayName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AggregationParameterDescriptor that = (AggregationParameterDescriptor) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return displayName != null ? displayName.equals(that.displayName) : that.displayName == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (displayName != null ? displayName.hashCode() : 0);
        return result;
    }
}
