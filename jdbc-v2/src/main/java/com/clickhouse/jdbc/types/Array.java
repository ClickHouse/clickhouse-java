package com.clickhouse.jdbc.types;

import com.clickhouse.jdbc.internal.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Map;

public class Array implements java.sql.Array {
    private static final Logger log = LoggerFactory.getLogger(Array.class);

    private Object[] array;
    private final int type; //java.sql.Types
    private final String elementTypeName;
    private boolean valid;

    /**
     * @deprecated this constructor should not be used. Elements array should be constructed externally.
     */
    public Array(List<Object> list, String elementTypeName, int itemType) throws SQLException {
        this(list.toArray(), elementTypeName, itemType);
    }

    public Array(Object[] elements, String elementTypeName,  int itemType) throws SQLException {
        if (elements == null) {
            throw ExceptionUtils.toSqlState(new IllegalArgumentException("Array cannot be null"));
        }
        if  (elementTypeName == null) {
            throw ExceptionUtils.toSqlState(new IllegalArgumentException("Array element type name cannot be null"));
        }
        this.array = elements;
        this.type = itemType;
        this.elementTypeName = elementTypeName;
        this.valid = true;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        ensureValid();
        return elementTypeName;
    }

    @Override
    public int getBaseType() throws SQLException {
        ensureValid();
        return type;
    }

    @Override
    public Object getArray() throws SQLException {
        ensureValid();
        return array;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getArray(Map<String, Class<?>>) is not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        ensureValid();
        if (index < 0) {
            throw new SQLException("Index cannot be negative");
        }
        if (count < 0) {
            throw new SQLException("Count cannot be negative");
        }
        if (count > (array.length - index)) {
            throw new SQLException("Not enough elements after index " + index);
        }

        Object[] smallerArray = new Object[count];
        System.arraycopy(array, (int) index, smallerArray, 0, count);
        return smallerArray;
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
        valid = false;
        array = null;
    }

    private void ensureValid() throws SQLException {
        if (!valid) {
            throw ExceptionUtils.toSqlState(new SQLFeatureNotSupportedException("Array is not valid. Possible free() was called."));
        }
    }
}
