package com.netcracker.profiler.configuration.callfilters;

import com.netcracker.profiler.agent.FilterOperator;

import java.util.ArrayList;
import java.util.List;

public abstract class FilterOperatorLogical implements FilterOperator {

    protected List<FilterOperator> children = new ArrayList<FilterOperator>();

    public void addChild(FilterOperator child) {
        children.add(child);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FilterOperatorLogical that = (FilterOperatorLogical) o;

        return children != null ? children.equals(that.children) : that.children == null;
    }

    @Override
    public int hashCode() {
        return children != null ? children.hashCode() : 0;
    }
}
