package com.netcracker.profiler.io.searchconditions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ComparisonCondition implements Condition, Cloneable{
    public enum Comparator {
        EQ("="),
        NE("!="),
        GT(">"),
        GE(">="),
        LT("<"),
        LE("<="),
        IN("in"),
        NOTIN("not in"),
        LIKE("like"),
        NOTLIKE("not like");

        private String representation;

        Comparator(String representation) {
            this.representation = representation;
        }

        public static Comparator from(String str){
            for(Comparator cmp: Comparator.values()){
                if(cmp.representation.equalsIgnoreCase(str)){
                    return cmp;
                }
            }
            throw new RuntimeException("Unknown comparator " + str);
        }

        @Override
        public String toString() {
            return representation;
        }
    }
    private Comparator comparator;
    private String lValue;
    private List<String> rValues = new ArrayList<String>();

    public ComparisonCondition() {
    }

    public Comparator getComparator() {
        return comparator;
    }

    public void setComparator(Comparator comparator) {
        this.comparator = comparator;
    }

    public List<String> getrValues() {
        return rValues;
    }

    public void addRValue(String str){
        this.rValues.add(str);
    }

    public String getlValue() {
        return lValue;
    }

    public void setlValue(String lValue) {
        this.lValue = lValue;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("(");
        result.append(lValue).append(" ").append(comparator).append(" (");
        for(Iterator it=rValues.iterator(); it.hasNext(); ){
            result.append(it.next().toString());
            if(it.hasNext()) result.append(", ");
        }
        result.append("))");
        return result.toString();
    }
}
