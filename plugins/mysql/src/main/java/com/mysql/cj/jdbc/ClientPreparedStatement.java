package com.mysql.cj.jdbc;

import com.mysql.cj.PreparedQuery;
import com.mysql.cj.Query;
import org.qubership.profiler.agent.Profiler;
import org.qubership.profiler.agent.ProfilerData;

public class ClientPreparedStatement {

    private Query query;

    public void dumpSql$profiler() {
        if(query == null) return;
        String sql = ((PreparedQuery)query).getOriginalSql();
        Profiler.event(sql, ProfilerData.PARAM_SQL);
    }

}
