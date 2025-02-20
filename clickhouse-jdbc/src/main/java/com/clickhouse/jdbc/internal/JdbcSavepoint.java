package com.clickhouse.jdbc.internal;

import java.sql.SQLException;
import java.sql.Savepoint;

import com.clickhouse.jdbc.SqlExceptionUtils;

@Deprecated
public class JdbcSavepoint implements Savepoint {
    final int id;
    final String name;

    JdbcSavepoint(int id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public int getSavepointId() throws SQLException {
        if (name != null) {
            throw SqlExceptionUtils
                    .clientError("Cannot get ID from a named savepoint, please use getSavepointName() instead");
        }

        return id;
    }

    @Override
    public String getSavepointName() throws SQLException {
        if (name == null) {
            throw SqlExceptionUtils
                    .clientError("Cannot get name from an un-named savepoint, please use getSavepointId() instead");
        }

        return name;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("JdbcSavepoint [id=").append(id).append(", name=").append(name)
                .append(']').toString();
    }
}