package ru.yandex.clickhouse;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;

import ru.yandex.clickhouse.domain.ClickHouseDataType;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class ClickHouseArray implements Array {

    private ClickHouseDataType elementType;
    private Object array;

    public ClickHouseArray(ClickHouseDataType elementType, Object array) {
        if (array == null) {
            throw new IllegalArgumentException("array cannot be null");
        }
        if (!array.getClass().isArray()) {
            throw new IllegalArgumentException("not array");
        }
        this.elementType = elementType;
        this.array = array;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return elementType.name();
    }

    @Override
    public int getBaseType() throws SQLException {
        return elementType.getSqlType();
    }

    @Override
    public Object getArray() throws SQLException {
        if (array == null){
            throw new SQLException("Call after free");
        }
        return array;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return getArray();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void free() throws SQLException {
        array = null;
    }
}
