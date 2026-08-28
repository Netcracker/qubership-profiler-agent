package com.netcracker.profiler.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public abstract class BaseConfigurationTest {
    protected static void println(String message) {
        System.out.println(message);
    }

    protected static void println(String message, Throwable t) {
        println(message);
        t.printStackTrace();
    }

    protected static void printf(String format, Object... args) {
        System.out.printf(format, args);
    }

    protected static void execute(Connection con, String sql) throws Exception {
        PreparedStatement ps = con.prepareStatement(sql);
        try {
            ps.executeUpdate();
        } finally {
            ps.close();
        }
    }

    protected static void selectAll(ResultSet rs) throws Exception {
        ResultSetMetaData metaData = rs.getMetaData();
        int rows = 0;
        while (rs.next()) {
            rows++;
            StringBuilder sb = new StringBuilder();
            sb.append('(');
            for(int i = 1; i<=metaData.getColumnCount(); i++) {
                if (i > 1) {
                    sb.append(',');
                }
                sb.append(metaData.getColumnLabel(i)).append('=').append(rs.getObject(i));
            }
            sb.append(')');
            println(sb.toString());
        }
        printf("%d rows\n", rows);
    }

    protected static void selectAll(PreparedStatement ps) throws Exception {
        ResultSet rs = ps.executeQuery();
        try {
            selectAll(rs);
        } finally {
            rs.close();
        }
    }

    protected static void selectAll(Connection con, String query) throws Exception {
        PreparedStatement ps = con.prepareStatement(query);
        try {
            selectAll(ps);
        } finally {
            ps.close();
        }
    }
}
