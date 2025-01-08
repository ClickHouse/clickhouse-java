package com.clickhouse.jdbc.types;

import com.clickhouse.jdbc.internal.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

public class Array implements java.sql.Array {
    private static final Logger log = LoggerFactory.getLogger(Array.class);
    Object[] array;
    int type; //java.sql.Types
    String typeName;

    public Array(List<Object> list, String itemTypeName, int itemType) throws SQLException {
        if (list == null) {
            throw ExceptionUtils.toSqlState(new IllegalArgumentException("List cannot be null"));
        }

        this.array = list.toArray();
        this.type = itemType;
        this.typeName = itemTypeName;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return typeName;
    }

    @Override
    public int getBaseType() throws SQLException {
        return type;
    }

    @Override
    public Object getArray() throws SQLException {
        return array;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getArray(Map<String, Class<?>>) is not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        try {
            Object[] smallerArray = new Object[count];
            System.arraycopy(array, (int) index, smallerArray, 0, count);
            return smallerArray;
        } catch (Exception e) {
            log.error("Failed to get array", e);
            throw new SQLException(e.getMessage(), ExceptionUtils.SQL_STATE_CLIENT_ERROR, e);
        }
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getArray(long, int, Map<String, Class<?>>) is not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLFeatureNotSupportedException("getResultSet() is not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getResultSet(Map<String, Class<?>>) is not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException("getResultSet(long, int) is not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getResultSet(long, int, Map<String, Class<?>>) is not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void free() throws SQLException {
        array = null;
    }
}
