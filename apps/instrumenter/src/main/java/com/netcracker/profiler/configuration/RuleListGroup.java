package com.netcracker.profiler.configuration;

import java.util.ArrayList;
import java.util.List;

public class RuleListGroup extends RuleGroup {
    private final List<Rule> rules = new ArrayList<Rule>();

    @Override
    public boolean add(Rule rule) {
        if (!rule.getClassNames().isEmpty())
            return false;
        rules.add(rule);
        return true;
    }

    @Override
    public List<Rule> getRulesForClass(String className) {
        return rules;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RuleListGroup)) return false;

        RuleListGroup that = (RuleListGroup) o;

        return rules.equals(that.rules);

    }

    @Override
    public int hashCode() {
        return rules.hashCode();
    }

    @Override
    public String toString() {
        return "RuleListGroup{" +
                "rules=" + rules +
                '}';
    }
}
