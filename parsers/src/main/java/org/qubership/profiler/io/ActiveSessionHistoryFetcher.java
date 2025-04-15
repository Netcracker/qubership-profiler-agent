package org.qubership.profiler.io;

import org.qubership.profiler.chart.StackedChart;
import org.qubership.profiler.chart.UnaryFunction;
import org.qubership.profiler.configuration.PropertyFacade;
import org.qubership.profiler.dom.ProfiledTree;
import org.qubership.profiler.dom.TagDictionary;
import org.qubership.profiler.sax.values.StringValue;
import org.qubership.profiler.util.ThrowableHelper;
import org.qubership.profiler.util.TimeHelper;

import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.sql.*;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.naming.InitialContext;
import javax.sql.DataSource;

public class ActiveSessionHistoryFetcher {
    public static final org.slf4j.Logger log = LoggerFactory.getLogger(ActiveSessionHistoryFetcher.class);

    private final PrintWriter out;
    Set<String> sids;
    Collection<Throwable> exceptions;
    int oracleSidTagID;
    boolean oracleSidTagIsSidSerial = true;

    NumberFormat nf = NumberFormat.getInstance();
    NumberFormat nfTime = NumberFormat.getInstance();

    public ActiveSessionHistoryFetcher(PrintWriter out) {
        this.out = out;
        nf.setMaximumFractionDigits(2);
        nf.setMinimumFractionDigits(2);
        nf.setMaximumIntegerDigits(4);

        nfTime.setMaximumFractionDigits(3);
        nfTime.setMinimumFractionDigits(3);
    }

    public void read(ProfiledTree agg) {
        if (agg == null) {
            return;
        }
        TagDictionary dict = agg.getDict();
        final List<String> tags = dict.getTags();
        int tagId, length = tags.size();
        for (tagId = 0; tagId < length; tagId++)
            if ("oracle.sid".equals(tags.get(tagId))) break;

        if (tagId == length)
            for (tagId = 0; tagId < length; tagId++)
                if ("oracle.audsid".equals(tags.get(tagId))) break;
                else oracleSidTagIsSidSerial = false;

        if (tagId == length)
            return; // oracle.sid and oracle.audsid not found
        oracleSidTagID = tagId;

        sids = new HashSet<String>();
        collectSessions(agg.getRoot());
        sids.remove(HotspotTag.OTHER);

        if (sids.isEmpty()) return;

        fetchReport(agg);
    }

    private void collectSessions(Hotspot root) {
        if (root.children != null)
            for (Hotspot child : root.children)
                collectSessions(child);
        if (root.tags == null)
            return;

        for (HotspotTag tag : root.tags.values())
            if (tag.id == oracleSidTagID) {
                Object val = tag.value;
                String value = null;
                if (val instanceof String) {
                    value = (String) val;
                } else if (val instanceof StringValue) {
                    value = ((StringValue) val).value;
                }
                if (value != null) {
                    // 303.56675(U86_S43_6910) -> 303.56675
                    int brace = value.indexOf('(');
                    if (brace != -1) {
                        value = value.substring(0, brace);
                    }
                }
                sids.add(value);
            }
    }

    private static int idx = 1;
    public static final int COL_SQL_ID = idx++;
    public static final int COL_SQL_CHILD = idx++;
    public static final int COL_PLAN_HASH_VALUE = idx++;
    public static final int COL_COUNT = idx++;
    public static final int COL_DB_TIME = idx++;
    public static final int COL_DB_CPU_TIME = idx++;
    public static final int COL_SQL_TEXT = idx++;
    public static final int COL_SQL_PLANA = idx++;
    public static final int COL_SQL_PLANB = idx++;
    public static final int COL_READ_REQS = idx++;
    public static final int COL_WRITE_REQS = idx++;
    public static final int COL_READ_BYTES = idx++;
    public static final int COL_WRITE_BYTES = idx++;
    public static final int COL_PGA = idx++;
    public static final int COL_TEMP = idx++;

    public static final boolean AWR_DISABLED =
            Boolean.getBoolean("org.qubership.profiler.awr.disabled") ||
                    !(Boolean.getBoolean("org.qubership.profiler.awr.enabled") ||
                            System.getProperty("qubership.naming.provider.url", "").toLowerCase().
                                    contains(".qubership.org"));

    private static final BigDecimal THOUSAND = BigDecimal.valueOf(1000);
    private static final BigDecimal FIVE_THOUSAND = BigDecimal.valueOf(5000);

    private void fetchReport(ProfiledTree agg) {
        Connection con = null;
        PreparedStatement ps = null;
        Statement s = null;
        ResultSet rs = null;
        out.print("CT.dbStats=[\"");
        try {
            StringBuilder errorMessage = new StringBuilder();
            if (PropertyFacade.getProperty("org.qubership.profiler.awr.disabled", false)) {
                errorMessage.append("Property -Dorg.qubership.profiler.awr.disabled=").
                        append(PropertyFacade.getProperty("org.qubership.profiler.awr.disabled", false)).
                        append("disables AWR usage. ");
            }
            if (!(PropertyFacade.getProperty("org.qubership.profiler.awr.enabled", false) ||
                    System.getProperty("qubership.naming.provider.url", "").toLowerCase().
                            contains(".qubership.org"))) {
                if (errorMessage.length() > 0)
                    errorMessage.append('\n');
                errorMessage.append("Property -Dorg.qubership.profiler.awr.enabled is equal to").
                        append(PropertyFacade.getProperty("org.qubership.profiler.awr.enabled", false)).
                        append(" (is not set to positive value), nor -Dqubership.naming.provider.url=").
                        append(System.getProperty("qubership.naming.provider.url", "")).
                        append(" contains '.qubership.org'");
            }
            if (errorMessage.length() > 0) {
                throw new IllegalAccessException(errorMessage.toString()) {
                    @Override
                    public Throwable fillInStackTrace() {
                        return this;
                    }
                };
            }
            DataSource ds;
            final long aTime = agg.getRoot().startTime;
            final long zTime = agg.getRoot().endTime;
            final InitialContext ctx = new InitialContext();
            ds = (DataSource) ctx.lookup("QubershipDataSource");

            con = ds.getConnection();
            s = con.createStatement();

            Calendar UTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            long t0 = System.currentTimeMillis();
            rs = s.executeQuery("select d.dbid            dbid\n" +
                    "     , systimestamp systime\n" +
                    "     , version\n" +
                    "  from v$database d, v$instance");
            if (!rs.next())
                throw new IllegalStateException("Unable to fetch v$database and v$instance info");

            final Timestamp nowAtDatabase = rs.getTimestamp(2, UTC);
            long t1 = System.currentTimeMillis();
            long dbTime = nowAtDatabase.getTime();

            final long dbid = rs.getLong(1);
            final boolean oracle11gR2 = rs.getString(3).compareTo("11.2") >= 0;

            rs.close();
            rs = null;
            s.close();
            s = null;

            final Timestamp aTimestamp = new Timestamp(aTime - t1 + dbTime);
            final Timestamp zTimestamp = new Timestamp(zTime - t0 + dbTime);

            if (oracle11gR2)
                ps = con.prepareStatement("select systimestamp+(oldest_sample_time-cast(systimestamp as timestamp)) from v$ash_info");
            else {
                ps = con.prepareStatement("select systimestamp+(" +
                        "coalesce(\n         " +
                        "(select min(sample_time)\n            " +
                        "from v$active_session_history\n           " +
                        "where SAMPLE_TIME < cast(from_tz(?, 'UTC') as timestamp with local time zone)+(cast(systimestamp as timestamp)-systimestamp) and rownum=1\n         ),\n         " +
                        "(select min(sample_time)\n            " +
                        "from v$active_session_history\n           " +
                        "where SAMPLE_TIME < cast(from_tz(?, 'UTC') as timestamp with local time zone)+(cast(systimestamp as timestamp)-systimestamp))\n        " +
                        ")-cast(systimestamp as timestamp))\n  from dual\n"
                );
                ps.setTimestamp(1, aTimestamp, UTC);
                ps.setTimestamp(2, zTimestamp, UTC);
            }
            rs = ps.executeQuery();
            if (!rs.next())
                throw new IllegalStateException("Unable to fetch oldest_sample_time");
            final Timestamp oldestASHSample = rs.getTimestamp(1, UTC);
            rs.close();
            rs = null;
            ps.close();
            ps = null;

            int minSnap = 0, maxSnap = 0;

            if (oldestASHSample != null && oldestASHSample.getTime() > aTimestamp.getTime()) {
                // ASH data is not sufficient, try AWR
                if (oracle11gR2) {
                    ps = con.prepareStatement("select min(snap_id) a, max(snap_id) b from dba_hist_ash_snapshot\n" +
                            " where from_tz(END_INTERVAL_TIME - SNAP_TIMEZONE, 'UTC') > from_tz(?, 'UTC')\n" +
                            "   and from_tz(BEGIN_INTERVAL_TIME - SNAP_TIMEZONE, 'UTC') < from_tz(?, 'UTC')"
                    );
                } else {
                    ps = con.prepareStatement("select min(snap_id) a, max(snap_id) b from dba_hist_snapshot\n" +
                            " where END_INTERVAL_TIME > cast(from_tz(?, 'UTC') as timestamp with local time zone)+(cast(systimestamp as timestamp)-systimestamp)\n" +
                            "   and BEGIN_INTERVAL_TIME < cast(from_tz(?, 'UTC') as timestamp with local time zone)+(cast(systimestamp as timestamp)-systimestamp)"
                    );
                }
                ps.setTimestamp(1, aTimestamp, UTC);
                ps.setTimestamp(2, oldestASHSample, UTC);

                rs = ps.executeQuery();
                if (!rs.next())
                    throw new IllegalStateException("Unable to fetch relevant dba_hist_snaphost ids");
                minSnap = rs.getInt(1);
                maxSnap = rs.getInt(2);
                rs.close();
                rs = null;
                ps.close();
                ps = null;
            }

            ps = con.prepareStatement("SELECT sql_id, SQL_CHILD_NUMBER, plan_hash_value, CNT, TM_DELTA_DB_TIME, TM_DELTA_CPU_TIME\n" +
                    "  ,coalesce((SELECT SQL_TEXT from DBA_HIST_SQLTEXT WHERE dbid=? and sql_id = x.sql_id), (select sql_fulltext from v$sql where sql_id=x.sql_id and rownum=1)) sql_text\n" +
                    "  ,cursor(select depth" +
                    "               , operation || ' ' || options operation\n" +
                    "               , decode(object_type,'PROCEDURE',object_owner||'.')||nvl(object_name,' ') object_name\n" +
                    "               , object_alias\n" +
                    "               , cost\n" +
                    "               , cardinality\n" +
                    "               , access_predicates\n" +
                    "               , filter_predicates\n" +
                    "               , nvl(search_columns,0)\n" +
//                        "               , bytes\n" +
                    "            from v$sql_plan\n" +
                    "            where sql_id=x.sql_id and plan_hash_value=x.plan_hash_value and child_number=SQL_CHILD_NUMBER" +
                    "             order by child_number, id\n" +
                    "   ) plana\n" +
                    "  ,cursor(select depth" +
                    "               , operation || ' ' || options operation\n" +
                    "               , decode(object_type,'PROCEDURE',object_owner||'.')||nvl(object_name,' ') object_name\n" +
                    "               , object_alias\n" +
                    "               , cost\n" +
                    "               , cardinality\n" +
                    "               , access_predicates\n" +
                    "               , filter_predicates\n" +
                    "               , nvl(search_columns,0)\n" +
//                        "               , bytes\n" +
                    "            from dba_hist_sql_plan\n" +
                    "            where dbid=? and sql_id=x.sql_id and plan_hash_value=x.plan_hash_value\n" +
                    "             order by id\n" +
                    "   ) planb\n" +
                    (oracle11gR2 ? ", DELTA_READ_IO_REQUESTS, DELTA_WRITE_IO_REQUESTS, DELTA_READ_IO_BYTES, DELTA_WRITE_IO_BYTES, PGA_ALLOCATED, TEMP_SPACE_ALLOCATED\n" : "") +
                    "   FROM (SELECT\n" +
                    "           SQL_ID, SQL_CHILD_NUMBER, plan_hash_value, count(*) cnt, sum(TM_DELTA_DB_TIME)/1000000 TM_DELTA_DB_TIME, sum(TM_DELTA_CPU_TIME)/1000000 TM_DELTA_CPU_TIME\n" +
                    (oracle11gR2 ? ", sum(DELTA_READ_IO_REQUESTS) DELTA_READ_IO_REQUESTS, sum(DELTA_WRITE_IO_REQUESTS) DELTA_WRITE_IO_REQUESTS, sum(DELTA_READ_IO_BYTES) DELTA_READ_IO_BYTES, sum(DELTA_WRITE_IO_BYTES) DELTA_WRITE_IO_BYTES" +
                            ", max(PGA_ALLOCATED) PGA_ALLOCATED, max(TEMP_SPACE_ALLOCATED) TEMP_SPACE_ALLOCATED\n" : "") +
                    "   FROM\n" +
                    "  (\n" +
                    (minSnap != 0 ?
                            "     SELECT SESSION_ID, SESSION_SERIAL#, SQL_ID, SQL_CHILD_NUMBER, sql_plan_hash_value plan_hash_value, SESSION_STATE, SAMPLE_TIME\n" +
                                    (oracle11gR2 ? ", CASE WHEN TM_DELTA_DB_TIME<15000000 THEN TM_DELTA_DB_TIME ELSE DECODE(SESSION_STATE,'ON CPU',0,10000000) END TM_DELTA_DB_TIME\n" +
                                            ", CASE WHEN TM_DELTA_CPU_TIME<15000000 THEN TM_DELTA_CPU_TIME ELSE DECODE(SESSION_STATE,'ON CPU',10000000) END TM_DELTA_CPU_TIME\n" +
                                            ", DELTA_READ_IO_REQUESTS, DELTA_WRITE_IO_REQUESTS, DELTA_READ_IO_BYTES, DELTA_WRITE_IO_BYTES\n" +
                                            ", PGA_ALLOCATED, TEMP_SPACE_ALLOCATED\n" :
                                            ", DECODE(SESSION_STATE,'ON CPU',0,10000000) TM_DELTA_DB_TIME, DECODE(SESSION_STATE,'ON CPU',10000000) TM_DELTA_CPU_TIME\n") +
                                    "       FROM DBA_HIST_ACTIVE_SESS_HISTORY\n" +
                                    "      WHERE DBID=? AND INSTANCE_NUMBER=USERENV('INSTANCE')\n" +
                                    "        AND SNAP_ID BETWEEN ? AND ?\n" +
                                    "        AND SAMPLE_TIME < cast(from_tz(?, 'UTC') as timestamp with local time zone)+(cast(systimestamp as timestamp)-systimestamp)\n" +
                                    "  UNION ALL\n" : "") +
                    "     SELECT SESSION_ID, SESSION_SERIAL#, SQL_ID, SQL_CHILD_NUMBER, sql_plan_hash_value plan_hash_value, SESSION_STATE, SAMPLE_TIME\n" +
                    (oracle11gR2 ? ", TM_DELTA_DB_TIME, TM_DELTA_CPU_TIME, DELTA_READ_IO_REQUESTS, DELTA_WRITE_IO_REQUESTS, DELTA_READ_IO_BYTES, DELTA_WRITE_IO_BYTES\n" +
                            ", PGA_ALLOCATED, TEMP_SPACE_ALLOCATED\n" :
                            ", DECODE(SESSION_STATE,'ON CPU',0,1000000) TM_DELTA_DB_TIME, DECODE(SESSION_STATE,'ON CPU',1000000) TM_DELTA_CPU_TIME\n") +
                    "       FROM V$ACTIVE_SESSION_HISTORY\n" +
                    "  )\n" +
                    "  x\n" +
                    "  WHERE SAMPLE_TIME BETWEEN cast(from_tz(?, 'UTC') as timestamp with local time zone)+(cast(systimestamp as timestamp)-systimestamp) AND cast(from_tz(?, 'UTC') as timestamp with local time zone)+(cast(systimestamp as timestamp)-systimestamp)\n" +
                    (oracleSidTagIsSidSerial ? "    AND (SESSION_ID, SESSION_SERIAL#) in (select /*+ cardinality(t 10) */ substr(column_value,1,instr(column_value,'.')-1), substr(column_value,instr(column_value,'.')+1) from table(cast(? as arrayofstrings)) t)\n" :
                            "    AND SESSION_SERIAL# in (select /*+ cardinality(t 10) */ column_value from table(cast(? as arrayofstrings)) t)\n") +
                    "  GROUP BY SQL_ID, SQL_CHILD_NUMBER, plan_hash_value) x\n" +
                    "ORDER BY nvl(TM_DELTA_DB_TIME,0)+nvl(TM_DELTA_CPU_TIME,0) DESC NULLS LAST");

            int bind = 1;
            ps.setLong(bind++, dbid);
            ps.setLong(bind++, dbid);

            if (minSnap != 0) {
                ps.setLong(bind++, dbid);
                ps.setInt(bind++, minSnap);
                ps.setInt(bind++, maxSnap);
                ps.setTimestamp(bind++, oldestASHSample, UTC);
            }

//            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS z");
            ps.setTimestamp(bind++, aTimestamp, UTC);
            ps.setTimestamp(bind++, zTimestamp, UTC);
            ps.setArray(bind, createArray(con, "ARRAYOFSTRINGS", sids.toArray()));

            rs = ps.executeQuery();

            out.print("Begin timestamp: \",new Date(");
            out.print(aTime);
            out.print(").toLocaleString(),\"<br>\",\n\"");
            out.print("End timestamp: \",new Date(");
            out.print(zTime);
            out.print(").toLocaleString(),\"<br>\",\n\"");
            out.print("Oracle session sid.serial#: ");
            Iterator<String> sidsIter = sids.iterator();
            out.print(sidsIter.next());
            while (sidsIter.hasNext()) {
                Object sid = sidsIter.next();
                out.print(',');
                out.print(' ');
                out.print(sid);
            }
            out.print("<br>\",\n\"");
            out.print("ASH condition: <pre class='prettyprint lang-sql' style='max-height:100%;'>\",prettyPrintOne(\"select * from v$active_session_history -- dba_hist_active_sess_history\\n");
            out.print(" where ");
            out.print("sample_time between");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            out.print(" cast(to_timestamp_tz('");
            out.print(sdf.format(aTimestamp));
            out.print(" UTC','YYYY-MM-DD HH24:MI:SS.FF TZR') as timestamp with local time zone)+(cast(systimestamp as timestamp)-systimestamp)");
            out.print("\\n   and cast(to_timestamp_tz('");
            out.print(sdf.format(zTimestamp));
            out.print(" UTC','YYYY-MM-DD HH24:MI:SS.FF TZR') as timestamp with local time zone)+(cast(systimestamp as timestamp)-systimestamp)");
            out.print(oracleSidTagIsSidSerial ? "\\n   and (session_id, session_serial#) in (select /*+ cardinality(t 10) */ substr(column_value,1,instr(column_value,'.')-1), substr(column_value,instr(column_value,'.')+1) " : "\\n   and session_serial# in (select /*+ cardinality(t 10) */ column_value ");
            out.print("from table(arrayofstrings('");
            sidsIter = sids.iterator();
            out.print(sidsIter.next());
            out.print('\'');
            while (sidsIter.hasNext()) {
                Object sid = sidsIter.next();
                out.print(',');
                out.print('\'');
                out.print(sid);
                out.print('\'');
            }
            out.print(")) t)\\n--   and dbid=");
            out.print(dbid);
            if (minSnap > 0) {
                out.print("\\n--   and snap_id between ");
                out.print(minSnap);
                out.print(" and ");
                out.print(maxSnap);
            }
            out.print(" -- for dba_hist_active_session_history\",\"sql\"),\"</pre><br>");
            if (Math.max(Math.abs(dbTime - t0), Math.abs(dbTime - t1)) > 2000) {
                out.print("<span class=r>Database clock is ");
                out.print(TimeHelper.humanizeDifference(null, dbTime - t0));
                if (t1 - t0 > 500) {
                    out.print(" .. ");
                    out.print(TimeHelper.humanizeDifference(null, dbTime - t1));
                }
                out.print(" compared to application server</span><br>\",\n\"");
                out.print("Database thinks current time is: \",new Date(");
                out.print(dbTime);
                out.print(").toUTCString(),\" (\",new Date(");
                out.print(dbTime);
                out.print(").toLocaleString(),\" in browser time zone)<br>\",\n\"");
            }
            out.print("Elapsed seconds: ");
            out.print((zTime - aTime) / 1000);
            double timeFactor = 0;
            if (zTime - aTime > 0)
                timeFactor = 60.0 * 1000 / (zTime - aTime);
            final double cutoffDuration = 0.1 / 1000 * (zTime - aTime);

            out.print("<table border=1 cellpadding=1 cellspacing=0>\",\n\"");
            if (oracle11gR2)
                out.print("<tr><th>sql_id</th><th>plan hash value</th><th>time</th><th>cpu time</th><th>IO read</th><th>IO write</th><th>Memory</th><th>SQL</th><th>plan</th></tr>\",\n\"");
            else
                out.print("<tr><th>sql_id</th><th>plan hash value</th><th>time</th><th>cpu time</th><th>SQL</th><th>plan</th></tr>\",\n\"");
            for (int i = 0; rs.next(); i++) {
                final String sql = rs.getString(COL_SQL_TEXT);
                boolean bigSql = JSHelper.hasNLines(sql, 12);
                out.print(i % 2 == 0 ? "<tr w=1 class='e" : "<tr w=1 class='o");
                out.print("'><td>");
                final String sqlId = rs.getString(COL_SQL_ID);
                printSqlId(out, sqlId, oracle11gR2);
                out.print("<br>child# ");
                out.print(rs.getString(COL_SQL_CHILD));
                out.print("</td><td class=nmbr>");
                out.print(rs.getString(COL_PLAN_HASH_VALUE));
                out.print("<br>");
                final String samples = rs.getString(COL_COUNT);
                out.print(samples);
                out.print("1".equals(samples) ? " sample" : " samples");
                out.print("</td><td class=nmbr>");
                final double tim = rs.getDouble(COL_DB_TIME);
                final double cpuTim = rs.getDouble(COL_DB_CPU_TIME);
                out.print(nfTime.format(tim));
                out.print('s');
                if (tim > cutoffDuration) {
                    out.print("<br>");
                    out.print("<span style='vertical-align:2px;font-size:7px; background:#8b0000;'><img src='data:image/gif;base64,R0lGODlhAQABAPABAP///wAAACH5BAEKAAAALAAAAAABAAEAAAICRAEAOw==' height=1 width=");
                    out.print(Math.round(tim * timeFactor));
                    out.print("></span>");
                }
                out.print("</td><td class=nmbr>");
                out.print(nfTime.format(cpuTim));
                out.print('s');
                if (cpuTim > cutoffDuration) {
                    out.print("<br>");
                    out.print("<span style='vertical-align:2px;font-size:7px; background:#8b0000;'><img src='data:image/gif;base64,R0lGODlhAQABAPABAP///wAAACH5BAEKAAAALAAAAAABAAEAAAICRAEAOw==' height=1 width=");
                    out.print(Math.round(cpuTim * timeFactor));
                    out.print("></span>");
                }
                if (oracle11gR2) {
                    out.print("</td><td class=nmbr>");
                    printIOStats(out, rs.getLong(COL_READ_REQS), rs.getLong(COL_READ_BYTES));
                    out.print("</td><td class=nmbr>");
                    printIOStats(out, rs.getLong(COL_WRITE_REQS), rs.getLong(COL_WRITE_BYTES));
                    out.print("</td><td class=nmbr>");
                    final long pga = rs.getLong(COL_PGA);
                    out.print(humanizedToString(pga, 1024, KILO_BYTES, pga > 20 * 1024 * 1024));
                    out.print(" PGA<br>");
                    final long temp = rs.getLong(COL_TEMP);
                    out.print(humanizedToString(temp, 1024, KILO_BYTES, temp > 10 * 1024 * 1024));
                    out.print(" TEMP");
                }
                out.print("</td><td");
                if (bigSql)
                    out.print(" class=b");
                out.print(">\"\n,CT.printReformatted([], \"");
                JSHelper.escapeJS(out, sql);
                out.print("\",\"sql\",0,\"lang-sql s\").join(\"\")\n,\"</pre></td>\",\n\"");
                out.print("</td><td class=");
                out.print(bigSql ? 'b' : 'y');
                out.print("><div class=t>\",\n\"");
                ResultSet plan = (ResultSet) rs.getObject(COL_SQL_PLANA);
                boolean shouldClose8 = true;
                if (!plan.next()) {
                    plan.close();
                    plan = (ResultSet) rs.getObject(COL_SQL_PLANB);
                    shouldClose8 = false;
                }
                if (shouldClose8 || plan.next()) {
                    out.print("<table class=ms border=0 cellpadding=1 cellspacing=0>\",\n\"");
                    out.print("<tr><th>operation</th><th>object</th><th>cost</th><th>cardinality</th><th>predicates</th></tr>\",\n\"");
                    for (int j = 0; ; j++) {
                        out.print(j % 2 == 0 ? "<tr class='e" : "<tr class='o");
                        out.flush();
                        final String operation = plan.getString(2);
                        final String objectName = plan.getString(3);
                        int searchColumns = plan.getInt(9);
                        final BigDecimal cost = plan.getBigDecimal(5);
                        final BigDecimal cardinality = plan.getBigDecimal(6);

                        if (operation != null && (operation.indexOf("FULL") != -1 || operation.indexOf("SKIP") != -1) ||
                                ("XIF26NC_PARAMS".equals(objectName) && searchColumns <= 1) ||
                                "XIF01NC_REFERENCES".equals(objectName) ||
                                "XIF10NC_OBJECTS".equals(objectName) ||
                                (cardinality != null && cardinality.compareTo(THOUSAND) >= 0) ||
                                (operation != null && operation.indexOf("COLLECTION") != -1 && BigDecimal.ZERO.equals(cardinality))
                                )
                            out.print(" r");
                        out.print("'>");
                        out.print("<td style='padding-left:");
                        out.print(plan.getInt(1) * 0.7f);
                        out.print("em;'>");
                        out.print(operation != null ? operation.replaceAll(" ", "&nbsp;") : "");
                        out.print("</td><td>");
                        out.print(objectName);
                        String alias = plan.getString(4);

                        if (alias != null) {
                            out.print("&nbsp;");
                            final int at = alias.indexOf('@');
                            if (at != -1)
                                alias = alias.substring(0, at);
                            if (!objectName.endsWith(alias)) {
                                out.print("<span class=g>");
                                JSHelper.escapeJS(out, JSHelper.escapeHTML(alias));
                                out.print("</span>");
                            }
                        }
                        out.print("</td><td class=nmbr>");
                        out.print(humanizedToString(cost, 1000, KILOS, cost == null ? false : cost.compareTo(FIVE_THOUSAND) > 0));
                        out.print("</td><td class=nmbr>");
                        out.print(humanizedToString(cardinality, 1000, KILOS, cardinality == null ? false : cardinality.compareTo(THOUSAND) > 0));
                        out.print("</td><td>");
                        String accessPredicates = plan.getString(7);
                        String filterPredicates = plan.getString(8);
                        if (accessPredicates != null) {
                            out.print("<b>ACCESS:</b> \",\n\"");
                            JSHelper.escapeJS(out, JSHelper.escapeHTML(accessPredicates));
                            if (filterPredicates != null) out.print("<br>");
                        }
                        if (filterPredicates != null) {
                            out.print("<b>FILTER:</b> \",\n\"");
                            JSHelper.escapeJS(out, JSHelper.escapeHTML(filterPredicates));
                        }
                        out.print("</td><td>");
                        out.print("</tr>\",\n\"");
                        if (!plan.next()) break;
                    }
                    out.print("</table>\",\n\"");
                }
                plan.close();
                if (shouldClose8) ((ResultSet) rs.getObject(COL_SQL_PLANB)).close();
                out.print("</div></td>\",\n\"");
                out.print("</tr>\",\n\"");
            }
            out.print("</table>");
            StackedChart[][] ashChartData = getASHChartData(con, UTC, dbid, aTimestamp, zTimestamp, oldestASHSample, minSnap, maxSnap);
            if (ashChartData != null) {
                out.print("\"].join('');");
                String[] groups = new String[]{"event", "sql_id", "user"};
                List<UnaryFunction<String, String>> labelMappers = Arrays.<UnaryFunction<String, String>>asList(
                        new UnaryFunction<String, String>() {
                            public String evaluate(String arg) {
                                String htmlSafe = JSHelper.escapeHTML(arg);
                                try {
                                    return "<a target=_blank href='https://www.google.com/search?q=" +
                                            URLEncoder.encode(arg, "UTF-8") + "'>" + htmlSafe + "</a>";
                                } catch (UnsupportedEncodingException e) {
                                    return htmlSafe;
                                }
                            }
                        }
                        , new UnaryFunction<String, String>() {
                            public String evaluate(String arg) {
                                return printSqlId(new StringWriter(), arg, oracle11gR2).toString();
                            }
                        }
                        , null
                );
                for (int i = 0; i < ashChartData.length; i++) {
                    StackedChart[] charts = ashChartData[i];
                    out.print("\nCT.ashData.");
                    out.print(groups[i]);
                    out.print("= [");
                    for (int j = 0; j < charts.length; j++) {
                        if (j > 0)
                            out.print("\n,");
                        StackedChart chart = charts[j];
                        chart.toJS(out, labelMappers.get(i));
                    }
                    out.print("];");
                }
                out.print("\n[\"");
            }
        } catch (Throwable e) {
            log.warn("Unable to get active session history for {}", agg, e);
            saveException(e);
        } finally {
            if (rs != null)
                try {
                    rs.close();
                } catch (SQLException e) { /**/}
            if (ps != null)
                try {
                    ps.close();
                } catch (SQLException e) { /**/}
            if (s != null)
                try {
                    s.close();
                } catch (SQLException e) { /**/}
            if (con != null)
                try {
                    con.close();
                } catch (SQLException e) { /**/}
        }
        if (exceptions != null) {
            out.print("\"].join('');\nCT.dbExceptions=[\"<h2>Errors detected when accessing active session information</h2><br><pre>");
            try {
                for (Throwable t : exceptions) {
                    JSHelper.escapeJS(out, JSHelper.escapeHTML(ThrowableHelper.throwableToString(t)));
                    out.print("\\n\",\n\"");
                }
            } catch (IOException e) {
                log.error("", e);
            }
        }
        out.print("\"].join('');\n");
    }

    private static Writer printSqlId(Writer out, String sqlId, boolean oracle11gR2) {
        try {
            out.append("<a target=_blank href='/tools/db150.jsp?sql=");
            out.append(oracle11gR2 ? "select%20dbms_sqltune.report_sql_monitor(%27" : "select%20*%20from%20table(dbms_xplan.display_awr(%27");
            out.append(sqlId);
            out.append(oracle11gR2 ? "%27,report_level=>%27ALL%27)%20plan_table_output%20from%20dual" : "%27,null,null,%27ALL%27))");
            out.append(";%0aselect+*+from+table(dbms_xplan.display_cursor(%27");
            out.append(sqlId);
            out.append("%27,null,%27allstats%20last%20%2Bpeeked_binds%27));&lob_output=fulllobs&act=execute'>");
            out.append(sqlId);
            out.append("</a>");
        } catch (IOException e) {
            log.warn("Unable to write sql_id {} to resulting javascript", sqlId, e);
        }
        return out;
    }

    private StackedChart[][] getASHChartData(Connection con, Calendar UTC, long dbid, Timestamp aTimestamp, Timestamp zTimestamp, Timestamp oldestASHSample, int minSnap, int maxSnap) throws SQLException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement("SELECT round(((((extract(day from st)*24+extract(hour from st))*60+extract(minute from st))*60)+extract(second from st))*1000) st\n" +
                    "  , our_session\n" +
                    "  , group_id\n" +
                    "  , cnt\n" +
                    "  , v_event\n" +
                    "  , v_sql_id\n" +
                    "  , v_user\n" +
                    " FROM (\n" +
                    "  SELECT sample_time\n" +
                    "  , ((systimestamp-TIMESTAMP '1970-01-01 00:00:00 +00:00')+(sample_time-cast(systimestamp as timestamp))) st\n" +
                    "  , grouping(decode(sids.session_id,null,1)) our_session\n" +
                    "  , grouping_id(user_id, sql_id, event) group_id\n" +
                    "  , count(*) cnt\n" +
                    "  , decode(grouping(event), 0, nvl(event, 'CPU')) v_event\n" +
                    "  , decode(grouping(sql_id), 0, nvl(sql_id, 'no sql_id')) v_sql_id\n" +
                    "  , decode(grouping(user_id), 0, (select username from dba_users where user_id=s.user_id)) v_user\n" +
                    " FROM (\n" +
                    "   SELECT sample_time, event, sql_id, user_id, session_id, session_serial# FROM v$active_session_history\n" +
                    (minSnap != 0 ? " UNION ALL SELECT sample_time, event, sql_id, user_id, session_id, session_serial# FROM DBA_HIST_ACTIVE_SESS_HISTORY\n" +
                            "    WHERE DBID=? AND INSTANCE_NUMBER=USERENV('INSTANCE')\n" +
                            "      AND SNAP_ID BETWEEN ? AND ?\n" +
                            "      AND SAMPLE_TIME < cast(from_tz(?, 'UTC') as timestamp with local time zone)+(cast(systimestamp as timestamp)-systimestamp)\n"
                            : "") +
                    ") s \n" +
                    (oracleSidTagIsSidSerial ?
                            ", (select /*+ cardinality(t 10) */ substr(column_value,1,instr(column_value,'.')-1) session_id, substr(column_value,instr(column_value,'.')+1) session_serial# from table(cast(? as arrayofstrings)) t) sids\n" :
                            ", (select /*+ cardinality(t 10) */ column_value session_id from table(cast(? as arrayofstrings)) t) sids\n"
                    ) +
                    "  WHERE SAMPLE_TIME BETWEEN cast(from_tz(?, 'UTC') as timestamp with local time zone)+(cast(systimestamp as timestamp)-systimestamp) AND cast(from_tz(?, 'UTC') as timestamp with local time zone)+(cast(systimestamp as timestamp)-systimestamp)\n" +
                    "    and sids.session_id(+) = s.session_id\n" +
                    (oracleSidTagIsSidSerial ? "    and sids.session_serial#(+) = s.session_serial#\n" : "") +
                    "GROUP BY sample_time, rollup(decode(sids.session_id,null,1)), grouping sets((event),(sql_id),(user_id))\n" +
                    "HAVING decode(sids.session_id,null,1) is null /* We need just our sessions or totals. */\n" +
                    ")\n" +
                    "ORDER BY sample_time, group_id"
            );
            int bind = 1;

            if (minSnap != 0) {
                ps.setLong(bind++, dbid);
                ps.setInt(bind++, minSnap - 1);
                ps.setInt(bind++, maxSnap + 1);
                ps.setTimestamp(bind++, oldestASHSample, UTC);
            }

            ps.setArray(bind++, createArray(con, "ARRAYOFSTRINGS", sids.toArray()));
            Timestamp chartA = aTimestamp;
            Timestamp chartZ = zTimestamp;
            long dt = chartZ.getTime() - chartA.getTime();
            if (dt != 0) {
                chartA.setTime(chartA.getTime() - Math.min(dt / 10, 5000));
                chartZ.setTime(chartZ.getTime() + Math.min(dt / 10, 5000));
            }
            ps.setTimestamp(bind++, chartA, UTC);
            ps.setTimestamp(bind++, chartZ, UTC);

            int idx = 1;
            int COL_SAMPLE_TIME = idx++;
            int COL_OUR_SESSION = idx++;
            int COL_GROUP_ID = idx++;
            int COL_COUNT = idx++;
            int COL_EVENT = idx++;
            int COL_SQL_ID = idx++;
            int COL_USER = idx++;

            StackedChart[][] data = new StackedChart[3][2];
            String[] groups = new String[]{"event", "sql_id", "user"};
            for (int i = 0; i < data.length; i++) {
                for (int j = 0; j < data[i].length; j++) {
                    String gr = groups[i];
                    data[i][j] = new StackedChart(
                            j == 0 ?
                                    Character.toUpperCase(gr.charAt(0)) + gr.substring(1) + "s in our session" :
                                    "All sessions by " + gr
                    );
                }
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                long time = rs.getLong(COL_SAMPLE_TIME);
                int groupingId = rs.getInt(COL_GROUP_ID);
                int chartTypeIndex = Integer.numberOfTrailingZeros(~groupingId);
                int ourSession = rs.getInt(COL_OUR_SESSION);
                int cnt = rs.getInt(COL_COUNT);
                String label = rs.getString(COL_EVENT + chartTypeIndex);

                data[chartTypeIndex][ourSession].add(time, label, cnt);
            }
            return data;
        } finally {
            if (rs != null)
                try {
                    rs.close();
                } catch (SQLException e) { /**/}
            if (ps != null)
                try {
                    ps.close();
                } catch (SQLException e) { /**/}
        }
    }

    private void printIOStats(PrintWriter out, long ios, long bytes) {
        out.print(humanizedToString(ios, 1000, KILO_IOPS, ios > 5000));
        out.print("<br>");
        out.print(humanizedToString(bytes, 1024, KILO_BYTES, bytes > 5000 * 8192));
    }

    private static final String[] KILOS = new String[]{"&nbsp;", "K", "M", "G", "T"};
    private static final String[] KILO_BYTES = new String[]{"&nbsp;", "KB", "MB", "GB", "TB"};
    private static final String[] KILO_IOPS = new String[]{"&nbsp; IOs", "K IOs", "M IOs", "G IOs", "T IOs"};

    private String humanizedToString(long value, int k, String[] magnitudes, boolean red) {
        StringBuffer sb = new StringBuffer();
        if (red) sb.append("<ins>");
        if (value < 100000)
            sb.append("<s>").append(value).append(magnitudes[0]).append("</s>");
        else if (value < 10000000)
            sb.append(Math.round(value / 1000)).append(magnitudes[1]);
        else if (value < 10000000000L)
            sb.append(Math.round(value / 1000000)).append(magnitudes[2]);
        else
            sb.append(Math.round(value / 1000000000)).append(magnitudes[3]);
        if (red) sb.append("</ins>");
        return sb.toString();
    }

    private final static BigDecimal X_1_000 = BigDecimal.valueOf(1000);
    private final static BigDecimal X_10_0000 = BigDecimal.valueOf(100000);
    private final static BigDecimal X_1_000_000 = BigDecimal.valueOf(1000000);
    private final static BigDecimal X_10_000_000 = BigDecimal.valueOf(10000000);
    private final static BigDecimal X_1_000_000_000 = BigDecimal.valueOf(1000000000);
    private final static BigDecimal X_10_000_000_000 = BigDecimal.valueOf(10000000000L);

    private String humanizedToString(BigDecimal value, int k, String[] magnitudes, boolean red) {
        if (value == null) return "";
        try {
            return humanizedToString(value.longValue(), k, magnitudes, red);
        } catch (ArithmeticException e) {
            /* ignore overflow */
        }
        StringBuffer sb = new StringBuffer();
        if (red) sb.append("<ins>");
        if (value.compareTo(X_10_0000) < 0)
            sb.append("<s>").append(value).append(magnitudes[0]).append("</s>");
        else if (value.compareTo(X_10_000_000) < 0)
            sb.append(value.divideToIntegralValue(X_1_000)).append(magnitudes[1]);
        else if (value.compareTo(X_10_000_000_000) < 0)
            sb.append(value.divideToIntegralValue(X_1_000_000)).append(magnitudes[2]);
        else
            sb.append(value.divideToIntegralValue(X_1_000_000_000)).append(magnitudes[3]);
        if (red) sb.append("</ins>");
        return sb.toString();
    }


    private static Array createArray(Connection con, String sqlType, Object[] objects) throws ClassNotFoundException, SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
        final Method mGetVendorConnection = con.getClass().getMethod("getVendorConnection");
        final Connection oracleConnection = (Connection) mGetVendorConnection.invoke(con);

        final Class<?> cArrayDescriptor = Class.forName("oracle.sql.ArrayDescriptor");
        final Method mCreateDescriptor = cArrayDescriptor.getMethod("createDescriptor", String.class, Connection.class);
        final Object arrayofnumbers = mCreateDescriptor.invoke(null, sqlType, oracleConnection);
        final Class<?> cARRAY = Class.forName("oracle.sql.ARRAY");
        final Constructor<?> constructorARRAY = cARRAY.getConstructor(cArrayDescriptor, Connection.class, Object.class);
        return (Array) constructorARRAY.newInstance(arrayofnumbers, oracleConnection, objects);
    }

    private void saveException(Throwable e) {
        if (exceptions == null) exceptions = new ArrayList<Throwable>();
        exceptions.add(e);
    }
}
