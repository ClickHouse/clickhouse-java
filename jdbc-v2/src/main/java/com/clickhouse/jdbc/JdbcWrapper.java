package com.clickhouse.jdbc;

import java.sql.SQLException;
import java.sql.Wrapper;

public interface JdbcWrapper extends Wrapper {
    default boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    @SuppressWarnings("unchecked")
    default <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }
}
