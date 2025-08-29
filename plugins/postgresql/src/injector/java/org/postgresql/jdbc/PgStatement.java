package org.postgresql.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class PgStatement implements Statement {

    protected void setSessionInfo$profiler() {
        try {
            Connection con = getConnection();
            PgConnection con2 = con.unwrap(PgConnection.class);
            con2.setSessionInfo$profiler();
        } catch (SQLException e) {
            /* ignore */
        }
    }
}
