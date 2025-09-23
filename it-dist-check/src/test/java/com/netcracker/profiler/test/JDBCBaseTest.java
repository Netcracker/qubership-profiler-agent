package com.netcracker.profiler.test;

import com.netcracker.profiler.agent.Profiler;
import org.testng.Assert;
import org.testng.SkipException;

import java.sql.*;

public abstract class JDBCBaseTest extends ConfigurationTest {
    protected abstract String getJDBCUrl();

    protected abstract String getJDBCUsername();

    protected abstract String getJDBCPassword();

    protected abstract String getDatabaseName();

    protected Connection getConnection(String testName) {
        try {
            return DriverManager.getConnection(getJDBCUrl(), getJDBCUsername(), getJDBCPassword());
        } catch (SQLException e) {
            println("Skipping test '" + testName + "' - unable to connect to " + getDatabaseName() + ": " + e.getMessage());
            throw new SkipException("Unable to connect to " + getDatabaseName(), e);
        }
    }

    protected void checkExceptionMessageForBinds(String testMethod, SQLException e, boolean requiresBinds, boolean expectValue, String... values) throws SQLException {
        String message = e.getMessage();
        if (message == null) {
            throw e;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(testMethod).append(" (").append(expectValue ? "contains " : "not contains ");
        boolean first = true;
        for (String value : values) {
            if (first) {
                first = false;
            } else {
                sb.append(',');
            }
            sb.append('\'').append(value).append('\'');
        }
        sb.append(") = ");
        String testDescription = sb.toString();
//        System.out.println(testMethod + " (" + (expectValue ? "contains " : "not contains ") + testDescription + "): " + message);
        int bindsIndex = message.indexOf("Binds:");
        if (requiresBinds && (message.contains("(no binds values were captured)") || bindsIndex == -1)) {
            Assert.fail(testDescription + "SQLException should have binds captured. Actual message: " + message);
        } else if (requiresBinds) {
            for (String value : values) {
                int valueIndex = message.indexOf(value, bindsIndex + "Binds:".length());
                if (expectValue && valueIndex == -1) {
                    Assert.fail(testDescription + "SQLException should include value for bind variable (" + value + "). Actual message: " + message);
                } else if (!expectValue && valueIndex != -1) {
                    Assert.fail(testDescription + "SQLException should not include value for bind variable (" + value + "). Actual message: " + message);
                }
            }
        }
    }

    protected void checkBindByException(String testMethod, Binder binder, String expectedValue) throws Exception {
        checkBindByException(testMethod, binder, expectedValue, true);
    }

    protected void checkBindByException(String testMethod, Binder binder, String value, boolean expectValue) throws Exception {
        Connection con = getConnection(testMethod);
        Profiler.enter("void " + getClass().getName() + "." + testMethod + "() (" + getClass().getSimpleName() + ".java:111) [unknown jar]");
        try {
            PreparedStatement ps = con.prepareStatement("select ? a, 1/0 b from dual");
            try {
                binder.bind(ps, 1);
                ResultSet rs = ps.executeQuery();
                try {
                    rs.next();
                } finally {
                    rs.close();
                }
                Assert.fail("Invalid statement should cause SQLException");
            } catch (SQLException e) {
                checkExceptionMessageForBinds(testMethod, e, expectValue, expectValue, value);
            } finally {
                ps.close();
            }
        } finally {
            Profiler.exit();
            con.close();
        }
    }
}
