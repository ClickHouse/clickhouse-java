package com.clickhouse.jdbc;

import java.net.ConnectException;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import com.clickhouse.client.ClickHouseException;

/**
 * Helper class for building {@link SQLException}.
 */
@Deprecated
public final class SqlExceptionUtils {
    public static final String SQL_STATE_CLIENT_ERROR = "HY000";
    public static final String SQL_STATE_OPERATION_CANCELLED = "HY008";
    public static final String SQL_STATE_CONNECTION_EXCEPTION = "08000";
    public static final String SQL_STATE_SQL_ERROR = "07000";
    public static final String SQL_STATE_NO_DATA = "02000";
    public static final String SQL_STATE_INVALID_SCHEMA = "3F000";
    public static final String SQL_STATE_INVALID_TX_STATE = "25000";
    public static final String SQL_STATE_DATA_EXCEPTION = "22000";
    public static final String SQL_STATE_FEATURE_NOT_SUPPORTED = "0A000";

    private SqlExceptionUtils() {
    }

    private static SQLException create(Throwable e) {
        if (e == null) {
            return unknownError();
        } else if (e instanceof ClickHouseException) {
            return handle((ClickHouseException) e);
        } else if (e instanceof SQLException) {
            return (SQLException) e;
        }

        Throwable cause = e.getCause();
        if (cause instanceof ClickHouseException) {
            return handle((ClickHouseException) cause);
        } else if (cause instanceof SQLException) {
            return (SQLException) cause;
        } else if (cause == null) {
            cause = e;
        }

        return new SQLException(cause);
    }

    // https://en.wikipedia.org/wiki/SQLSTATE
    private static String toSqlState(ClickHouseException e) {
        final String sqlState;
        switch (e.getErrorCode()) {
            case ClickHouseException.ERROR_ABORTED:
            case ClickHouseException.ERROR_CANCELLED:
                sqlState = SQL_STATE_OPERATION_CANCELLED;
                break;
            case ClickHouseException.ERROR_NETWORK:
            case ClickHouseException.ERROR_POCO:
                sqlState = SQL_STATE_CONNECTION_EXCEPTION;
                break;
            case 0:
                sqlState = e.getCause() instanceof ConnectException ? SQL_STATE_CONNECTION_EXCEPTION
                        : SQL_STATE_CLIENT_ERROR;
                break;
            default:
                sqlState = e.getCause() instanceof ConnectException ? SQL_STATE_CONNECTION_EXCEPTION
                        : SQL_STATE_SQL_ERROR;
                break;
        }
        return sqlState;
    }

    public static SQLException clientError(String message) {
        return new SQLException(message, SQL_STATE_CLIENT_ERROR, null);
    }

    public static SQLException clientError(Throwable e) {
        return e != null ? new SQLException(e.getMessage(), SQL_STATE_CLIENT_ERROR, e) : unknownError();
    }

    public static SQLException clientError(String message, Throwable e) {
        return new SQLException(message, SQL_STATE_CLIENT_ERROR, e);
    }

    public static SQLException handle(ClickHouseException e) {
        return e != null ? new SQLException(e.getMessage(), toSqlState(e), e.getErrorCode(), e.getCause())
                : unknownError();
    }

    public static SQLException handle(Throwable e, Throwable... more) {
        SQLException rootEx = create(e);
        if (more != null) {
            for (Throwable t : more) {
                rootEx.setNextException(create(t));
            }
        }
        return rootEx;
    }

    public static BatchUpdateException batchUpdateError(Throwable e, long[] updateCounts) {
        if (e == null) {
            return new BatchUpdateException("Something went wrong when performing batch update", SQL_STATE_CLIENT_ERROR,
                    0, updateCounts, null);
        } else if (e instanceof BatchUpdateException) {
            return (BatchUpdateException) e;
        } else if (e instanceof SQLException) {
            SQLException sqlExp = (SQLException) e;
            return new BatchUpdateException(sqlExp.getMessage(), sqlExp.getSQLState(), sqlExp.getErrorCode(),
                    updateCounts, null);
        }

        Throwable cause = e.getCause();
        if (e instanceof BatchUpdateException) {
            return (BatchUpdateException) e;
        } else if (cause instanceof ClickHouseException) {
            return batchUpdateError(cause, updateCounts);
        } else if (cause instanceof SQLException) {
            SQLException sqlExp = (SQLException) cause;
            return new BatchUpdateException(sqlExp.getMessage(), sqlExp.getSQLState(), sqlExp.getErrorCode(),
                    updateCounts, null);
        } else if (cause == null) {
            cause = e;
        }

        return new BatchUpdateException("Unexpected error", SQL_STATE_SQL_ERROR, 0, updateCounts, cause);
    }

    public static BatchUpdateException queryInBatchError(int[] updateCounts) {
        return new BatchUpdateException("Query is not allowed in batch update", SQL_STATE_CLIENT_ERROR, updateCounts);
    }

    public static BatchUpdateException queryInBatchError(long[] updateCounts) {
        return new BatchUpdateException("Query is not allowed in batch update", SQL_STATE_CLIENT_ERROR, 0, updateCounts,
                null);
    }

    public static SQLException undeterminedExecutionError() {
        return clientError("Please either call clearBatch() to clean up context first, or use executeBatch() instead");
    }

    public static SQLException forCancellation(Exception e) {
        Throwable cause = e.getCause();
        if (cause == null) {
            cause = e;
        }

        // operation canceled
        return new SQLException(e.getMessage(), SQL_STATE_OPERATION_CANCELLED, ClickHouseException.ERROR_ABORTED,
                cause);
    }

    public static SQLFeatureNotSupportedException unsupportedError(String message) {
        return new SQLFeatureNotSupportedException(message, SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    public static SQLException unknownError() {
        return new SQLException("Unknown error", SQL_STATE_CLIENT_ERROR);
    }
}
