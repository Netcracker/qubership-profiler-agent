package org.postgresql.core.v3;

import org.qubership.profiler.agent.Profiler;

import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;
import org.postgresql.core.ResultHandler;
import org.postgresql.core.Utils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.SQLException;

public class QueryExecutorImpl {

    public static final int MAX_BIND_SIZE$profiler = 4096;
    private static volatile Boolean pre4230$profiler;

    static transient Field detailMessage$profiler;
    static transient boolean hasNativeSql$profiler;
    static transient Method getNativeSql$profiler;

    private static String getSql$profiler(Query query) {
        if (query == null) {
            return null;
        }

        if (!hasNativeSql$profiler) {
            hasNativeSql$profiler = true;
            try {
                getNativeSql$profiler = query.getClass().getMethod("getNativeSql");
            } catch (NoSuchMethodException e) {
                // ignore
            }
        }
        if (getNativeSql$profiler == null) {
            return query.toString();
        }
        try {
            return (String) getNativeSql$profiler.invoke(query);
        } catch (Throwable e) {
            return query.toString();
        }
    }


    public static String getBinds$profiler(ParameterList parameters) {
        if (parameters == null)
            return null;
        final int count = parameters.getParameterCount();
        if (count == 0)
            return null;

        SimpleParameterList[] subparams = ((V3ParameterList) parameters).getSubparams();

        StringBuilder sb = new StringBuilder();
        if (subparams == null) {
            if (parameters instanceof SimpleParameterList) {
                stringifyParameterList$profiler((SimpleParameterList) parameters, sb);
            } else {
                return parameters.toString();
            }
        } else {
            for (int i = 0; i < subparams.length; i++) {
                if (i > 0) sb.append('\n');
                sb.append("/* query#").append(i).append(" */");
                final SimpleParameterList param = subparams[i];
                if (param == null) continue;
                stringifyParameterList$profiler(param, sb);
            }
        }
        return sb.toString();
    }

    public static void stringifyParameterList$profiler(SimpleParameterList spl, StringBuilder target) {
        int[] paramTypes = spl.getParamTypes();
        Object[] values = spl.getValues();
        //list of potentially huge parameters.
        target.append("<[");
        for (int i = 0; i < paramTypes.length; i++) {
            if (i != 0) {
                target.append(", ");
            }
            if (values[i] instanceof String) {
                appendTrimmed$profiler((String) values[i], target);
            } else if (values[i] instanceof String[]) {
                String[] stringValues = (String[]) values[i];
                target.append("[");
                for (int j = 0; j < stringValues.length; j++) {
                    if (j != 0) {
                        target.append(", ");
                    }
                    appendTrimmed$profiler(stringValues[j], target);
                }
                target.append("]");
            } else {
                String potentiallyBigString = spl.toString(i + 1, true);
                appendTrimmed$profiler(potentiallyBigString, target, false);
            }
        }
        target.append("]>");
    }

    private static void appendTrimmed$profiler(String toAppend, StringBuilder target) {
        appendTrimmed$profiler(toAppend, target, true);
    }

    private static void appendTrimmed$profiler(String toAppend, StringBuilder target, boolean escape) {
        if (toAppend.length() > MAX_BIND_SIZE$profiler) {
            toAppend = toAppend.substring(0, MAX_BIND_SIZE$profiler) + "...";
        }
        if (escape) {
            target.append('\'');
            try {
                Utils.escapeLiteral(target, toAppend, true);
            } catch (SQLException sqle) {
                target.append(toAppend);
            }
            target.append('\'');
        } else {
            target.append(toAppend);
        }
    }

    // Callbacks from the instrumented code

    /*
     Conditional methods

     Before 42.3.0 we have methods:
       execute(org.postgresql.core.Query, ParameterList, ResultHandler, int, int, int)
       execute(org.postgresql.core.Query[], ParameterList[], BatchResultHandler, int, int, int)
     Since 42.3.0 following methods were added:
       execute(org.postgresql.core.Query, ParameterList, ResultHandler, int, int, int, boolean)
       execute(org.postgresql.core.Query[], ParameterList[], BatchResultHandler, int, int, int, boolean)
     ... and old methods just call these new.

     So, in order to instrument both old and new versions, we add dumpSql/dumpBinds/handleException invocations
     to new methods, while old methods are instrumented with 'conditional' variants, which delegate calls to regular
     methods only if current class has no new methods (to avoid duplicate events)
    */

    private static boolean isPre4230Version$profiler() {
        Boolean pre4230 = pre4230$profiler;

        if (pre4230 == null) {
            try {
                QueryExecutorImpl.class.getMethod("execute", Query.class, ParameterList.class, ResultHandler.class, Integer.TYPE, Integer.TYPE, Integer.TYPE, Boolean.TYPE);
                pre4230$profiler = pre4230 = false;
            } catch (Throwable t) {
                pre4230$profiler = pre4230 = true;
            }
        }

        return pre4230;
    }

    public static void dumpSqlConditional$profiler(Query query) {
        if (isPre4230Version$profiler()) {
            dumpSql$profiler(query);
        }
    }

    public static void dumpSqlConditional$profiler(Query[] queries) {
        if (isPre4230Version$profiler()) {
            dumpSql$profiler(queries);
        }
    }

    public static void dumpBindsConditional$profiler(ParameterList parameters) {
        if (isPre4230Version$profiler()) {
            dumpBinds$profiler(parameters);
        }
    }

    public static void dumpBindsConditional$profiler(ParameterList[] parameters) {
        if (isPre4230Version$profiler()) {
            dumpBinds$profiler(parameters);
        }
    }

    public void handleExceptionConditional$profiler(Throwable t, Query query, ParameterList params) {
        if (isPre4230Version$profiler()) {
            handleException$profiler(t, query, params);
        }
    }

    public void handleExceptionConditional$profiler(Throwable t, Query[] queries, ParameterList[] params) {
        if (isPre4230Version$profiler()) {
            handleException$profiler(t, queries, params);
        }
    }

    /*
     Unconditional methods
    */

    public static void dumpSql$profiler(Query query) {
        Profiler.event(getSql$profiler(query), "sql");
    }

    public static void dumpSql$profiler(Query[] queries) {
        for (Query query : queries) {
            Profiler.event(getSql$profiler(query), "sql");
        }
    }

    public static void dumpBinds$profiler(ParameterList parameters) {
        String s = getBinds$profiler(parameters);
        if (s == null) return;
        Profiler.event(s, "binds");
    }

    public static void dumpBinds$profiler(ParameterList[] parameters) {
        for (ParameterList parameter : parameters) {
            String s = getBinds$profiler(parameter);
            if (s == null) return;
            Profiler.event(s, "binds");
        }
    }

    public void handleException$profiler(Throwable t, Query query, ParameterList params) {
        String message = t.getMessage();
        if (message == null) {
            message = "";
        }
        if (message.contains("\n\tsql=")) return;

        message = message + "\n\tsql=" + getSql$profiler(query)
                + "\n\tbinds=" + getBinds$profiler(params);

        Profiler.event(message, "exception");

        try { // Overwrite an original message with more informative one
            Field detailMessage = detailMessage$profiler;
            if (detailMessage == null) {
                detailMessage = Class.forName("java.lang.Throwable").getDeclaredField("detailMessage");
                detailMessage.setAccessible(true);
                detailMessage$profiler = detailMessage;
            }
            detailMessage.set(t, message);
            return;
        } catch (Throwable e) {
            // Did not succeed. Will try to add cause exception
        }

        // Try to fetch the original SQLState to create a new exception with it
        // The SQLState value can be used for handle errors
        String originalSQLState = null;
        if (t instanceof SQLException) {
            originalSQLState = ((SQLException) t).getSQLState();
        }

        if (t.getCause() == null) {
            final SQLException details = new SQLException(message, originalSQLState);
            try {
                t.initCause(details);
            } catch (Throwable e) {
                // Did not succeed. Will print exception to StdErr
            }
            return;
        }

        SQLException sqlException = new SQLException(message, originalSQLState, t);
        Profiler.logError("", sqlException);
    }

    public void handleException$profiler(Throwable t, Query[] queries, ParameterList[] params) {
        for (int i = 0; i < queries.length; i++) {
            if (params.length > i) {
                handleException$profiler(t, queries[i], params[i]);
            }
        }
    }

}
