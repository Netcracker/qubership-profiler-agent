package org.postgresql.jdbc2;

import org.qubership.profiler.agent.CallInfo;
import org.qubership.profiler.agent.LocalState;
import org.qubership.profiler.agent.Profiler;
import org.qubership.profiler.agent.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;

public abstract class AbstractJdbc2Connection implements Connection {
    protected transient Statement statement$profiler;
    protected static int SET_E2E$profiler;
    protected String prevUser$profiler;
    protected String prevAppName$profiler;
    protected SimpleDateFormat sdf$profiler;

    void setSessionInfo$profiler() {
        final LocalState state = Profiler.getState();
        final CallInfo callInfo = state.callInfo;

        final boolean connectionIsOk = callInfo.checkConnection(this);
        if (!callInfo.anyFieldChanged() && connectionIsOk) {
            return;
        }

        if (statement$profiler == null) {
            try {
                statement$profiler = this.createStatement();
            } catch (SQLException e) {
                Profiler.event(StringUtils.throwableToString(e), "exception: create setSessionInfo$profiler");
            }
            sdf$profiler = new SimpleDateFormat("MMddHHmmss");
        }
        try {
            String ncUser = String.valueOf(callInfo.getNcUser());
            if (!ncUser.equals(prevUser$profiler)) {
                prevUser$profiler = ncUser;
                statement$profiler.execute("set session \"esc.nc.user\" = '" + ncUser.replace('\'', '"') + "'");
            }
        } catch (SQLException e) {
            Profiler.event(StringUtils.throwableToString(e), "exception: setSessionInfo$profiler.executeUpdate");
        }

        if (SET_E2E$profiler == 1) {
            return;
        } else if (SET_E2E$profiler == 0) {
            if (Boolean.getBoolean("org.qubership.execution-statistics-collector.postgresql.e2e.disabled")) {
                SET_E2E$profiler = 1;
            } else {
                SET_E2E$profiler = 2;
            }
        }
        try {
            long now = System.currentTimeMillis();
            StringBuilder sb = new StringBuilder(100);
            sb.append(state.thread.getId());
            sb.append(',');
            sb.append(sdf$profiler.format(now)); // MMddHHmmss
            sb.append(',');
            sb.append(state.shortThreadName.substring(0, Math.min(15, state.shortThreadName.length())));
            sb.append(',');
            sb.append(now);
            String appName = sb.toString();
            if (!appName.equals(prevAppName$profiler)) {
                prevAppName$profiler = appName;
                statement$profiler.execute("set session application_name = '" + appName.replace('\'', '"') + "'");
            }
        } catch (SQLException e) {
            Profiler.pluginException(e);
        }

    }
}
