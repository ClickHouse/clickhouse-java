package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.client.api.ConnectionInitiationException;
import com.clickhouse.client.api.ServerException;

import java.sql.SQLException;

/**
 * Helper class for building {@link SQLException}.
 */
public final class ExceptionUtils {
    public static final String SQL_STATE_CLIENT_ERROR = "HY000";
    public static final String SQL_STATE_OPERATION_CANCELLED = "HY008";
    public static final String SQL_STATE_CONNECTION_EXCEPTION = "08000";
    public static final String SQL_STATE_SQL_ERROR = "07000";
    public static final String SQL_STATE_NO_DATA = "02000";
    public static final String SQL_STATE_INVALID_SCHEMA = "3F000";
    public static final String SQL_STATE_INVALID_TX_STATE = "25000";
    public static final String SQL_STATE_DATA_EXCEPTION = "22000";
    public static final String SQL_STATE_FEATURE_NOT_SUPPORTED = "0A000";

    private ExceptionUtils() {}//Private constructor

    // https://en.wikipedia.org/wiki/SQLSTATE

    /**
     * Convert a {@link Exception} to a {@link SQLException}.
     * @param e {@link Exception} to convert
     * @return Converted {@link SQLException}
     */
    public static SQLException toSqlState(Exception e) {
        if (e == null) {
            return new SQLException("Unknown client error", SQL_STATE_CLIENT_ERROR);
        } else if (e instanceof SQLException) {
            return (SQLException) e;
        } else if (e instanceof ClientMisconfigurationException) {
            return new SQLException(e.getMessage(), SQL_STATE_CLIENT_ERROR, e);
        } else if (e instanceof ConnectionInitiationException) {
            return new SQLException(e.getMessage(), SQL_STATE_CONNECTION_EXCEPTION, e);
        } else if (e instanceof ServerException) {
            return new SQLException(e.getMessage(), SQL_STATE_DATA_EXCEPTION, e);
        } else if (e instanceof ClientException) {
            return new SQLException(e.getMessage(), SQL_STATE_CLIENT_ERROR, e);
        }

        return new SQLException(e.getMessage(), SQL_STATE_CLIENT_ERROR, e);//Default
    }
}
