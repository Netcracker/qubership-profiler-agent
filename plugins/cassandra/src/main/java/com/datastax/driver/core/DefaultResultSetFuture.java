package com.datastax.driver.core;

import org.qubership.profiler.agent.Profiler;
import org.qubership.profiler.agent.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultResultSetFuture {
    private static transient Logger logger$profiler = LoggerFactory.getLogger(DefaultResultSetFuture.class);

    private Object threadLocalTraces$profiler;

    private static boolean methodsInitialized$profiler = false;
    private static boolean tracingAvailable$profiler = false;
    private static Method currentTraceContextxtractThreadLocalTraces$profiler;
    private static Method currentTraceContextlogThreadLocalTraces$profiler;
    private static boolean ableToParseBinds$profiler = true;

    public void onSet$profiler(com.datastax.driver.core.Statement statement, long latency) {

        long lattencyMillis = latency / 1000000L;
        if (statement == null) {
            return;
        }
        ensureMethodsTracingMethodsInitialized$profiler();

        Profiler.enterWithDuration("void com.datastax.driver.core.DefaultResultSetFuture.onSet() (DefaultResultSetFuture.java:333) [unknown jar]", lattencyMillis);
        try {
            if (tracingAvailable$profiler) {
                currentTraceContextlogThreadLocalTraces$profiler.invoke(null, threadLocalTraces$profiler);
            }

            //simple test whether the call to cassandra is reactive or not. Of course, need some better test to cover cases when long waits are coupled with short reactive calls
            if (Profiler.getState().callInfo.waitTime > lattencyMillis) {
                lattencyMillis = 0;
            }

            String query = null;
            String binds = null;
            if (statement instanceof RegularStatement) {
                query = ((RegularStatement) statement).getQueryString();
            } else if (statement instanceof com.datastax.driver.core.BoundStatement) {
                BoundStatement bound = (BoundStatement) statement;
                query = bound.preparedStatement().getQueryString();
                binds = parseBinds$profiler(bound);
            }
            if (!StringUtils.isBlank(query)) {
                Profiler.event(query, "sql");
            }
            if (!StringUtils.isBlank(binds)) {
                Profiler.event(binds, "binds");
            }
        } catch (Throwable t) {
            logger$profiler.error("Failed to log statement", t);
        } finally {
            Profiler.exit();
        }
    }

    private String parseBinds$profiler(BoundStatement bound) {
        if (!ableToParseBinds$profiler) {
            return "";
        }
        try {
            PreparedStatement preparedStatement = bound.preparedStatement();
            StringBuilder bindsBuilder = new StringBuilder();
            if (preparedStatement.getVariables().size() > 0) {
                List<ColumnDefinitions.Definition> defs = preparedStatement.getVariables().asList();
                for (int i = 0; i < defs.size(); i++) {
                    ColumnDefinitions.Definition def = defs.get(i);
                    bindsBuilder.append(def.getType().name).append(": ").append(def.getName()).append(": ");
                    stringify$profiler(bindsBuilder, def, bound, i);
                }
            }
            return bindsBuilder.toString();
        } catch (NoSuchFieldError e) {
            if (logger$profiler.isDebugEnabled()) {
                logger$profiler.warn("Failed to log cassandra statement statement bindings. probably mismatch in a driver version", e);
            } else {
                logger$profiler.warn("Failed to log cassandra statement statement bindings. probably mismatch in a driver version");
            }
            ableToParseBinds$profiler = false;
            return "";
        }
    }

    private static Map<DataType.Name, TypeCodec> typeCodecs$profiler = new HashMap<DataType.Name, TypeCodec>();

    static {
        typeCodecs$profiler.put(DataType.Name.ASCII, TypeCodec.ascii());
        typeCodecs$profiler.put(DataType.Name.BIGINT, TypeCodec.bigint());
        typeCodecs$profiler.put(DataType.Name.BLOB, TypeCodec.blob());
        typeCodecs$profiler.put(DataType.Name.BOOLEAN, TypeCodec.cboolean());
        typeCodecs$profiler.put(DataType.Name.COUNTER, TypeCodec.counter());
        typeCodecs$profiler.put(DataType.Name.DECIMAL, TypeCodec.decimal());
        typeCodecs$profiler.put(DataType.Name.DOUBLE, TypeCodec.cdouble());
        typeCodecs$profiler.put(DataType.Name.FLOAT, TypeCodec.cfloat());
        typeCodecs$profiler.put(DataType.Name.INT, TypeCodec.cint());
        typeCodecs$profiler.put(DataType.Name.TEXT, TypeCodec.varchar());
        typeCodecs$profiler.put(DataType.Name.TIMESTAMP, TypeCodec.timestamp());
        typeCodecs$profiler.put(DataType.Name.UUID, TypeCodec.uuid());
        typeCodecs$profiler.put(DataType.Name.VARCHAR, TypeCodec.varchar());
        typeCodecs$profiler.put(DataType.Name.VARINT, TypeCodec.varint());
        typeCodecs$profiler.put(DataType.Name.TIMEUUID, TypeCodec.timeUUID());
        typeCodecs$profiler.put(DataType.Name.INET, TypeCodec.inet());
        typeCodecs$profiler.put(DataType.Name.DATE, TypeCodec.date());
        typeCodecs$profiler.put(DataType.Name.TIME, TypeCodec.time());
        typeCodecs$profiler.put(DataType.Name.SMALLINT, TypeCodec.smallInt());
        typeCodecs$profiler.put(DataType.Name.TINYINT, TypeCodec.tinyInt());
        typeCodecs$profiler.put(DataType.Name.DURATION, TypeCodec.duration());
    }

    private void stringify$profiler(StringBuilder result, ColumnDefinitions.Definition def, BoundStatement bound, int i) {
        TypeCodec codec = typeCodecs$profiler.get(def.getType().getName());
        if (codec == null) {
            result.append(">>>>").append(def.getType().getName().toString()).append(" not supported yet");
            return;
        }

        result.append(bound.get(i, codec));
    }

    public void postConstruct$profiler() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, ClassNotFoundException {
        ensureMethodsTracingMethodsInitialized$profiler();
        if (!tracingAvailable$profiler) {
            return;
        }

        threadLocalTraces$profiler = currentTraceContextxtractThreadLocalTraces$profiler.invoke(null);

    }

    @SuppressWarnings("unchecked")
    private void ensureMethodsTracingMethodsInitialized$profiler() {
        try {
            if (methodsInitialized$profiler) {
                return;
            }

            Class currentTraceContextClass;
            try {
                currentTraceContextClass = Class.forName("brave.propagation.CurrentTraceContext");
            } catch (ClassNotFoundException e) {
                logger$profiler.info("Failed to find class \"brave.propagation.CurrentTraceContext\". Tracing not available. {}", e.getMessage());
                tracingAvailable$profiler = false;
                methodsInitialized$profiler = true;
                return;
            }

            try {
                currentTraceContextxtractThreadLocalTraces$profiler = currentTraceContextClass.getDeclaredMethod("extractThreadLocalTraces$profiler");
                currentTraceContextlogThreadLocalTraces$profiler = currentTraceContextClass.getDeclaredMethod("logThreadLocalTraces$profiler", Object.class);
                currentTraceContextxtractThreadLocalTraces$profiler.setAccessible(true);
                currentTraceContextlogThreadLocalTraces$profiler.setAccessible(true);
                tracingAvailable$profiler = true;
            } catch (NoSuchMethodException e) {
                logger$profiler.info("Tracing not available: unable to find method. {}", e.getMessage());
                tracingAvailable$profiler = false;
            }

            methodsInitialized$profiler = true;
        } catch (Throwable t) {
            Profiler.pluginException(t);
            logger$profiler.error("Exception during initialization of methods", t);
        }
    }

    public void handleException$profiler(Throwable t, Statement statement, long latency) {
        Profiler.logInfo("DMNE reached 2");
        logger$profiler.error("Exception {}", t);
    }
}
