package com.clickhouse.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.value.ClickHouseArrayValue;

public class ClickHouseArray implements Array {
    private final int columnIndex;
    private ClickHouseResultSet resultSet;

    protected ClickHouseArray(ClickHouseResultSet resultSet, int columnIndex) throws SQLException {
        this.resultSet = ClickHouseChecker.nonNull(resultSet, "ResultSet");
        resultSet.ensureRead(columnIndex);
        this.columnIndex = columnIndex;
    }

    protected void ensureValid() throws SQLException {
        if (resultSet == null) {
            throw SqlExceptionUtils.clientError("Cannot operate on a freed Array object");
        }
    }

    /**
     * Returns Array base column
     * @return
     */
    protected ClickHouseColumn getBaseColumn() {
        return resultSet.columns.get(columnIndex - 1).getArrayBaseColumn();
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        ensureValid();

        return getBaseColumn().getDataType().name();
    }

    @Override
    public int getBaseType() throws SQLException {
        ensureValid();

        return resultSet.mapper.toSqlType(getBaseColumn(), resultSet.defaultTypeMap);
    }

    @Override
    public Object getArray() throws SQLException {
        ensureValid();
        return getArray(null);
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        ensureValid();

        ClickHouseValue v = resultSet.getValue(columnIndex);
        Class<?> targetClass = map != null ? map.get(getBaseTypeName()) : null;
        switch (getBaseColumn().getDataType()) {
            case Date:
            case Date32:
                return ((ClickHouseArrayValue)v).asArray(targetClass == null ? java.sql.Date.class : targetClass);
            case DateTime:
            case DateTime32:
            case DateTime64:
                return ((ClickHouseArrayValue)v).asArray(targetClass == null ? java.sql.Timestamp.class : targetClass);
            default:
                return targetClass == null ? v.asArray() : v.asArray(targetClass);
        }
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        ensureValid();
        Object[] arr = (Object[]) getArray();
        return Arrays.copyOfRange(arr, (int) index, (int) (index + count));
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        ensureValid();

        Object[] arr = (Object[]) getArray(map);
        return Arrays.copyOfRange(arr, (int) index, (int) (index + count));
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        ensureValid();

        throw SqlExceptionUtils.unsupportedError("getResultSet not implemented");
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        return getResultSet();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        return getResultSet();
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return getResultSet();
    }

    @Override
    public void free() throws SQLException {
        this.resultSet = null;
    }
}
