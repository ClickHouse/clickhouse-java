package com.clickhouse.jdbc.metadata;

import com.clickhouse.jdbc.JdbcV2Wrapper;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Implement ParameterMetaData interface and provides minimal information about parameters.
 * This class will return only actual number of parameters.
 * This class cannot be used to determine exact datatype for a parameter.
 */
public class ParameterMetaDataImpl implements ParameterMetaData, JdbcV2Wrapper {

    private static final int FAIL_SAFE_PRECISION = 0;

    private static final int FAIL_SAFE_SCALE = 0;

    private final int paramCount;

    public ParameterMetaDataImpl(int paramCount) {
        this.paramCount = paramCount;
    }

    @Override
    public int getParameterCount() {
        return paramCount;
    }

    private void checkParamIndex(int index) throws SQLException {
        if (index > paramCount || index < 1) {
            throw new SQLException("Parameter index out of range. " + (paramCount == 0 ? "There are no parameters" : "[1," + paramCount + "]"));
        }
    }

    /**
     * Always returns {@code ParameterMetaData.parameterNullableUnknown}.
     *
     * @param param parameter index starting from 1
     * @return ParameterMetaData.parameterNullableUnknown
     */
    @Override
    public int isNullable(int param) throws SQLException {
        checkParamIndex(param);
        return ParameterMetaData.parameterNullableUnknown;
    }

    /**
     * Always returns {@code false}.
     *
     * @param param parameter index starting from 1
     * @return false
     */
    @Override
    public boolean isSigned(int param) throws SQLException {
        checkParamIndex(param);
        return false;
    }

    /**
     * Always returns 0.
     *
     * @param param parameter index starting from 1
     * @return 0
     */
    @Override
    public int getPrecision(int param) throws SQLException {
        checkParamIndex(param);
        return FAIL_SAFE_PRECISION;
    }

    /**
     * Always returns 0.
     *
     * @param param parameter index starting from 1
     * @return 0
     */
    @Override
    public int getScale(int param) throws SQLException {
        checkParamIndex(param);
        return FAIL_SAFE_SCALE;
    }

    /**
     * Always returns {@code Types.OTHER}.
     *
     * @param param parameter index starting from 1
     * @return {@code Types.OTHER}
     */
    @Override
    public int getParameterType(int param) throws SQLException {
        checkParamIndex(param);
        return Types.OTHER;
    }

    /**
     * Always returns "UNKNOWN".
     *
     * @param param parameter index starting from 1
     * @return String {@code "UNKNOWN"}
     */
    @Override
    public String getParameterTypeName(int param) throws SQLException {
        checkParamIndex(param);
        return "UNKNOWN";
    }

    /**
     * Always returns {@code Object.class.getName()}.
     *
     * @param param the first parameter is 1, the second is 2, ...
     * @return String {@code Object.class.getName()}
     */
    @Override
    public String getParameterClassName(int param) throws SQLException {
        checkParamIndex(param);
        return Object.class.getName();
    }

    /**
     * Always return {@code java.sql.ParameterMetaData#parameterModeIn}.
     *
     * @param param parameter index starting from 1
     * @return {@code java.sql.ParameterMetaData#parameterModeIn}
     */
    @Override
    public int getParameterMode(int param) throws SQLException {
        checkParamIndex(param);
        // only in parameter mode IN is supported by prepared statement
        // other modes are designed for callable statements
        return parameterModeIn;
    }
}
