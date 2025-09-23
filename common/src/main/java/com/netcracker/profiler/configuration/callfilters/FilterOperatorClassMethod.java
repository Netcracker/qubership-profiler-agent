package com.netcracker.profiler.configuration.callfilters;

import com.netcracker.profiler.agent.FilterOperator;
import com.netcracker.profiler.agent.ProfilerData;
import com.netcracker.profiler.dump.ThreadState;

import java.util.BitSet;
import java.util.Map;

public class FilterOperatorClassMethod implements FilterOperator {

    private String className;
    private String methodName;
    private BitSet matchedCalls = new BitSet();
    private BitSet unmatchedCalls = new BitSet();

    public FilterOperatorClassMethod() {}

    public FilterOperatorClassMethod(String className, String methodName) {
        this.className = className;
        this.methodName = methodName;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    @Override
    public boolean evaluate(Map<String, Object> params) {
        ThreadState threadState = (ThreadState) params.get(THREAD_STATE_PARAM);
        return checkClassMethodMatching(threadState);
    }

    private boolean checkClassMethodMatching(ThreadState threadState) {
        if(matchedCalls.get(threadState.method)) {
            return true;
        }
        if(unmatchedCalls.get(threadState.method)) {
            return false;
        }

        // Example:
        // java.sql.ResultSet weblogic.jdbc.wrapper.Statement.executeQuery(java.lang.String) (Statement.java:497) [mw1036\/modules\/com.bea.core.datasource6_1.10.0.0.jar]
        String callName = ProfilerData.resolveMethodId(threadState.method);

        // weblogic.jdbc.wrapper.Statement.executeQuery(java.lang.String) (Statement.java:497) [mw1036\/modules\/com.bea.core.datasource6_1.10.0.0.jar]
        callName = callName.substring(callName.indexOf(' ') + 1);

        // weblogic.jdbc.wrapper.Statement.executeQuery
        callName = callName.substring(0, callName.indexOf('('));

        // weblogic.jdbc.wrapper.Statement
        String actualClassName = callName.substring(0, callName.lastIndexOf('.'));

        // executeQuery
        String actualMethodName = callName.substring(callName.lastIndexOf('.') + 1);

        if (actualClassName.equals(className) && ("".equals(methodName) || actualMethodName.equals(methodName))) {
            matchedCalls.set(threadState.method);
            return true;
        } else {
            unmatchedCalls.set(threadState.method);
            return false;
        }
    }
}
