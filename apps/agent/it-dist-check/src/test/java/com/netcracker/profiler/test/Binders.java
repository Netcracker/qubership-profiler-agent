package com.netcracker.profiler.test;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;

public class Binders {
    private static class ObjectBinder implements Binder {
        private final Object value;

        public ObjectBinder(Object value) {
            this.value = value;
        }

        public void bind(PreparedStatement ps, int column) throws SQLException {
            ps.setObject(column, value);
        }
    }

    public static Binder object(Object value) {
        return new ObjectBinder(value);
    }

    public static Binder chain(final Binder ... binders) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                for(Binder binder : binders) {
                    binder.bind(ps, column);
                }
            }
        };
    }

    public static Binder clear() {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.clearParameters();
            }
        };
    }

    public static Binder nullBinder(final int sqlType) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setNull(column, sqlType);
            }
        };
    }

    public static Binder booleanBinder(final boolean value) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setBoolean(column, value);
            }
        };
    }

    public static Binder byteBinder(final byte value) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setByte(column, value);
            }
        };
    }

    public static Binder shortBinder(final short value) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setShort(column, value);
            }
        };
    }

    public static Binder intBinder(final int value) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setInt(column, value);
            }
        };
    }

    public static Binder longBinder(final long value) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setLong(column, value);
            }
        };
    }

    public static Binder floatBinder(final float value) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setFloat(column, value);
            }
        };
    }

    public static Binder doubleBinder(final double value) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setDouble(column, value);
            }
        };
    }

    public static Binder stringBinder(final String value) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setString(column, value);
            }
        };
    }

    public static Binder bigDecimalBinder(final BigDecimal value) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setBigDecimal(column, value);
            }
        };
    }

    public static Binder timeBinder(final Time value) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setTime(column, value);
            }
        };
    }

    public static Binder timeBinder(final Time value, final Calendar calendar) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setTime(column, value, calendar);
            }
        };
    }

    public static Binder dateBinder(final Date value) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setDate(column, value);
            }
        };
    }

    public static Binder dateBinder(final Date value, final Calendar calendar) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setDate(column, value, calendar);
            }
        };
    }

    public static Binder timestampBinder(final Timestamp value) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setTimestamp(column, value);
            }
        };
    }

    public static Binder timestampBinder(final Timestamp value, final Calendar calendar) {
        return new Binder() {
            public void bind(PreparedStatement ps, int column) throws SQLException {
                ps.setTimestamp(column, value, calendar);
            }
        };
    }
}
