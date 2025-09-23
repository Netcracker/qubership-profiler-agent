package com.netcracker.profiler.test;

import com.netcracker.profiler.agent.LocalState;
import com.netcracker.profiler.agent.Profiler;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.*;

public class JDBCPostgresTest extends JDBCBaseTest {
    protected String getJDBCUrl() {
        return "jdbc:postgresql://localhost/postgres";
    }

    protected String getJDBCUsername() {
        return "postgres";
    }

    protected String getJDBCPassword() {
        return "";
    }

    protected String getDatabaseName() {
        return "PostgreSQL";
    }

    @Test
    public void postgresql() throws SQLException {
        Connection con = getConnection("postgresql");
        Profiler.enter("void com.netcracker.profiler.test.JDBCPostgresTest.postgresql() (JDBCPostgresTest.java:24) [unknown jar]");
        try {
            LocalState state = Profiler.getState();
            state.callInfo.setNcUser("user");
            state.callInfo.setAction("testAction");
            state.callInfo.setEndToEndId("e2e");
            PreparedStatement ps = con.prepareStatement("select 1");
            try {
                for (int i = 0; i < 6; i++) {
                    ps.execute();
                }
            } finally {
                ps.close();
            }

            ps = con.prepareStatement("select current_setting('esc.nc.user'), ?");
            try {
                ps.setString(1, "abcd");
                ResultSet rs = ps.executeQuery();
                try {
                    Assert.assertTrue(rs.next(), "select current_setting('esc.nc.user') must have a row");
                    Assert.assertEquals(rs.getString(1), "user", "current_setting('esc.nc.user') should have valud from callInfo.setNcUser");
                } finally {
                    rs.close();
                }
            } finally {
                ps.close();
            }

            String myId = Thread.currentThread().getId() + ",";
            String myName = "," + Thread.currentThread().getName() + ",";
            boolean hasAppName = false;
            ps = con.prepareStatement("select application_name from pg_stat_activity");
            try {
                ResultSet rs = ps.executeQuery();
                try {
                    while (rs.next()) {
                        String appName = rs.getString(1);
                        hasAppName |= appName != null && appName.startsWith(myId) && appName.contains(myName);
                        System.out.println("pg_stat_activity.application_name from PostgreSQL = " + appName);
                    }
                } finally {
                    rs.close();
                }
            } finally {
                ps.close();
            }

            Assert.assertTrue(hasAppName, "No rows in pg_stat_activity.application_name detected that start with <<" + myId + ">> and contains <<" + myName + ">>");
        } finally {
            Profiler.exit();
            con.close();
        }
    }

    @Test
    public void postgresqlBinds() throws SQLException {
        Connection con = getConnection("postgresqlBinds");
        Profiler.enter("void com.netcracker.profiler.test.JDBCPostgresTest.postgresql() (JDBCPostgresTest.java:82) [unknown jar]");
        try {
            PreparedStatement ps = con.prepareStatement("select pg_sleep(1), ?");
            try {
                ps.setString(1, "testtesttest");
                ps.execute();
            } finally {
                ps.close();
            }
        } finally {
            Profiler.exit();
            con.close();
        }
    }
}
