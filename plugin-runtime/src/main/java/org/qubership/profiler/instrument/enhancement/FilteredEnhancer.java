package org.qubership.profiler.instrument.enhancement;

import org.objectweb.asm.ClassVisitor;

import java.util.Objects;

public class FilteredEnhancer implements ClassEnhancer {
    EnhancerPlugin filter;
    private final ClassEnhancer enhancer;

    protected FilteredEnhancer(EnhancerPlugin filter, ClassEnhancer enhancer) {
        this.filter = filter;
        this.enhancer = enhancer;
    }

    public EnhancerPlugin getFilter(){
        return filter;
    }

    public void enhance(ClassVisitor cv) {
        enhancer.enhance(cv);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FilteredEnhancer that = (FilteredEnhancer) o;

        if (enhancer != null ? enhancer.getClass() != that.enhancer.getClass() : that.enhancer != null) return false;
        if (filter != null ? !filter.equals(that.filter) : that.filter != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(filter);
        result = 31 * result + Objects.hashCode(enhancer);
        return result;
    }

    @Override
    public String toString() {
        return "FilteredEnhancer{" +
                "filter=" + filter +
                ", enhancer=" + enhancer +
                '}';
    }
}
