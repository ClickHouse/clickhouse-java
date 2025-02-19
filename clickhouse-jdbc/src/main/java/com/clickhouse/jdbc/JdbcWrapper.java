package com.clickhouse.jdbc;

import java.sql.SQLException;

@Deprecated
public abstract class JdbcWrapper {
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }

        throw SqlExceptionUtils.unsupportedError("Cannot unwrap to " + iface.getName());
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
}
