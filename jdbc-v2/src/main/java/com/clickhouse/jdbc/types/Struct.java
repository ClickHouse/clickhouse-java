package com.clickhouse.jdbc.types;

import com.clickhouse.data.ClickHouseColumn;

import java.sql.SQLException;
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
        return attributes;
    }

    public ClickHouseColumn getColumn() {
        return column;
    }
}
