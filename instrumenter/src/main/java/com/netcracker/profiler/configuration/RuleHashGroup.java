package com.netcracker.profiler.configuration;


import java.util.*;

public class RuleHashGroup extends RuleGroup {
    private final Map<String, List<Rule>> rules = new HashMap<String, List<Rule>>();

    public boolean add(Rule rule) {
        if (rule.getClassNames().isEmpty())
            return false;
        for (String className : rule.getClassNames()) {
            className = className.replace('.', '/');
            List<Rule> rules = this.rules.get(className);
            if (rules == null) {
                this.rules.put(className, rules = new ArrayList<Rule>());
            }
            rules.add(rule);
        }
        return true;
    }

    public List<Rule> getRulesForClass(String className) {
        return rules.get(className);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RuleHashGroup)) return false;

        RuleHashGroup that = (RuleHashGroup) o;

        return rules.equals(that.rules);

    }

    @Override
    public int hashCode() {
        return rules.hashCode();
    }

    @Override
    public String toString() {
        return "RuleHashGroup{" +
                "rules=" + rules +
                '}';
    }
}
