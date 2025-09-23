package com.netcracker.profiler.io.searchconditions;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class LogicalCondition implements Condition, Cloneable{
    public enum Operation {
        AND("and"),
        OR("or"),
        NOT("not");

        private String representation;

        Operation(String representation) {
            this.representation = representation;
        }

        public static Operation from(String str){
            for(LogicalCondition.Operation op: Operation.values()){
                if(op.representation.equalsIgnoreCase(str)){
                    return op;
                }
            }
            throw new RuntimeException("Unknown operation " + str);
        }

        @Override
        public String toString() {
            return representation;
        }
    }

    public LogicalCondition() {
    }

    public LogicalCondition(Operation op, Condition... conditions) {
        this.conditions = Arrays.asList(conditions);
        this.operation = op;
    }

    public LogicalCondition(Operation op, List<Condition> conditions) {
        this.conditions = conditions;
        this.operation = op;
    }

    private List<Condition> conditions = new ArrayList<Condition>();
    private Operation operation;

    public Operation getOperation() {
        return operation;
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    public void setConditions(List<Condition> conditions) {
        this.conditions = conditions;
    }

    public void addCondition(Condition condition) {
        this.conditions.add(condition);
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        if(conditions.size() == 0){
            result.append("true[").append(operation).append("]");
            return result.toString();
        }
        result.append("(");
        for(Iterator<Condition> it = conditions.iterator(); it.hasNext() ;) {
            result.append(it.next());
            if(it.hasNext()){
                result.append(" ").append(operation);
            }
        }
        result.append(")");

        return result.toString();
    }
}
