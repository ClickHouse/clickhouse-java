package com.clickhouse.jdbc;

import java.sql.SQLException;

interface JdbcWrapper {
    default boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    @SuppressWarnings("unchecked")
    default <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            iface.cast(this);
        }
        throw SqlExceptionUtils.unsupportedError("Cannot unwrap to " + iface.getName());
    }
}
