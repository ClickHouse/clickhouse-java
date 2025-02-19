package com.clickhouse.jdbc;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;

import com.clickhouse.data.ClickHouseChecker;

@Deprecated
public class ClickHouseStruct implements Struct {
    private final String typeName;
    private final Object[] values;

    protected ClickHouseStruct(String typeName, Object[] values) {
        this.typeName = ClickHouseChecker.nonNull(typeName, "SQLTypeName");
        this.values = ClickHouseChecker.nonNull(values, "values");
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        return typeName;
    }

    @Override
    public Object[] getAttributes() throws SQLException {
        return values;
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        return getAttributes();
    }
}
