package com.netcracker.profiler.test;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface Binder {
    void bind(PreparedStatement ps, int column) throws SQLException;
}
