package com.clickhouse.jdbc.types;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.jdbc.internal.ExceptionUtils;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;

public class Struct implements java.sql.Struct {

    private final Object[] attributes;

    private final ClickHouseColumn column;

    public Struct(ClickHouseColumn column, Object[] attributes) {
        this.column = column;
        this.attributes = attributes;
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        return column.getOriginalTypeName();
    }

    @Override
    public Object[] getAttributes() throws SQLException {
        return attributes;
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getAttributes(Map<String, Class<?>>) is not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    public ClickHouseColumn getColumn() {
        return column;
    }
}
