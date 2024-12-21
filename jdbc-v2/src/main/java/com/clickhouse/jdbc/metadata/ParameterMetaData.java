package com.clickhouse.jdbc.metadata;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.jdbc.JdbcV2Wrapper;
import com.clickhouse.jdbc.internal.JdbcUtils;
import com.clickhouse.jdbc.internal.ExceptionUtils;

import java.sql.SQLException;
import java.util.List;

public class ParameterMetaData implements java.sql.ParameterMetaData, JdbcV2Wrapper {
    private final List<ClickHouseColumn> params;

    protected ParameterMetaData(List<ClickHouseColumn> params) throws SQLException {
        if (params == null) {
            throw ExceptionUtils.toSqlState(new IllegalArgumentException("Parameters array cannot be null."));
        }

        this.params = params;
    }

    protected ClickHouseColumn getParam(int param) throws SQLException {
        if (param < 1 || param > params.size()) {
            throw new SQLException("Parameter index out of range: " + param, ExceptionUtils.SQL_STATE_CLIENT_ERROR);
        }

        return params.get(param - 1);
    }

    @Override
    public int getParameterCount() throws SQLException {
        try {
            return params.size();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public int isNullable(int param) throws SQLException {
        try {
            return getParam(param).isNullable() ? parameterNullable : parameterNoNulls;
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        try{
            return getParam(param).getDataType().isSigned();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        try {
            return getParam(param).getPrecision();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public int getScale(int param) throws SQLException {
        try {
            return getParam(param).getScale();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        //TODO: Should we implement .getSQLType()?
        try {
            return JdbcUtils.convertToSqlType(getParam(param).getDataType()).getVendorTypeNumber();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        try {
            return getParam(param).getDataType().name();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        //TODO: Should we implement .getClassName()?
        try {
            return getParam(param).getDataType().getObjectClass().getName();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return parameterModeIn;
    }
}
