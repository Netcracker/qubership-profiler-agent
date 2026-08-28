package com.netcracker.profiler.configuration;

import java.util.List;

public abstract class RuleGroup {
    public static RuleGroup of(Rule rule) {
        RuleGroup group;
        if (rule.getClassNames().isEmpty()) {
            group = new RuleListGroup();
        } else {
            group = new RuleHashGroup();
        }
        group.add(rule);
        return group;
    }

    public abstract boolean add(Rule rule);

    public abstract List<Rule> getRulesForClass(String className);
}
