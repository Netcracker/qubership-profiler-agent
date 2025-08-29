package org.postgresql.jdbc2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractJdbc2Statement implements Statement {

    protected void setSessionInfo$profiler() {
        try {
            Connection con = getConnection();
            AbstractJdbc2Connection con2 = con.unwrap(AbstractJdbc2Connection.class);
            con2.setSessionInfo$profiler();
        } catch (SQLException e) {
            /* ignore */
        }
    }
}
