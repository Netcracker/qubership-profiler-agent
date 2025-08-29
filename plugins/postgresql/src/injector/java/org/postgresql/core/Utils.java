package org.postgresql.core;

import java.sql.SQLException;

public class Utils {
    public static native StringBuilder escapeLiteral(StringBuilder sbuf, String value,
                                              boolean standardConformingStrings) throws SQLException;
}
