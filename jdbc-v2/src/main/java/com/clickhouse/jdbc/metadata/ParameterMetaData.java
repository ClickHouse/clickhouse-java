package com.clickhouse.jdbc.metadata;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.jdbc.JdbcWrapper;
import com.clickhouse.jdbc.internal.JdbcUtils;

import java.sql.SQLException;
import java.util.List;

public class ParameterMetaData implements java.sql.ParameterMetaData, JdbcWrapper {
    private final List<ClickHouseColumn> params;

    protected ParameterMetaData(List<ClickHouseColumn> params) {
        if (params == null) {
            throw new IllegalArgumentException("Parameters array cannot be null.");
        }

        this.params = params;
    }

    protected ClickHouseColumn getParam(int param) throws SQLException {
        if (param < 1 || param > params.size()) {
            throw new SQLException("Parameter index out of range: " + param);
        }

        return params.get(param - 1);
    }

    @Override
    public int getParameterCount() throws SQLException {
        return params.size();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return getParam(param).isNullable() ? parameterNullable : parameterNoNulls;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return getParam(param).getDataType().isSigned();
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return getParam(param).getPrecision();
    }

    @Override
    public int getScale(int param) throws SQLException {
        return getParam(param).getScale();
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        //TODO: Should we implement .getSQLType()?
        return JdbcUtils.convertToSqlType(getParam(param).getDataType());
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return getParam(param).getDataType().name();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        //TODO: Should we implement .getClassName()?
        return getParam(param).getDataType().getObjectClass().getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return parameterModeIn;
    }
}
