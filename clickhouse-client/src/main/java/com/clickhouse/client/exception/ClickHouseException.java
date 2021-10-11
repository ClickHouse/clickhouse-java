package com.clickhouse.client.exception;

import com.clickhouse.client.ClickHouseNode;

/**
 * Exception thrown from ClickHouse server.
 */
public class ClickHouseException extends Exception {
    /**
     * Generated ID.
     */
    private static final long serialVersionUID = -2417038200885554382L;

    private final int errorCode;

    private static String buildErrorMessage(ClickHouseErrorCode error, ClickHouseNode server, Throwable cause) {
        if (error == null) {
            error = ClickHouseErrorCode.UNKNOWN_EXCEPTION;
        }

        return buildErrorMessage(error.name(), error.code, server, cause);
    }

    private static String buildErrorMessage(String message, int code, ClickHouseNode server, Throwable cause) {
        StringBuilder builder = new StringBuilder();

        builder.append("ClickHouse exception, ");
        if (message != null) {
            builder.append(" message: ").append(message);
        } else {
            builder.append(" code: ").append(code);
        }

        builder.append(", server: ").append(server).append(';');

        if (cause != null) {
            builder.append(' ').append(cause.getMessage());
        }

        return builder.toString();
    }

    public ClickHouseException(int code, Throwable cause, ClickHouseNode server) {
        super(buildErrorMessage(
                ClickHouseErrorCode.fromCodeOrDefault(code, ClickHouseErrorCode.UNKNOWN_EXCEPTION).name(), code, server,
                cause), cause);

        errorCode = code;
    }

    public ClickHouseException(int code, String message, Throwable cause, ClickHouseNode server) {
        super(buildErrorMessage(
                message != null ? message
                        : ClickHouseErrorCode.fromCodeOrDefault(code, ClickHouseErrorCode.UNKNOWN_EXCEPTION).name(),
                code, server, cause), cause);

        errorCode = code;
    }

    public ClickHouseException(int code, String message, Throwable cause) {
        super(buildErrorMessage(
                message != null ? message
                        : ClickHouseErrorCode.fromCodeOrDefault(code, ClickHouseErrorCode.UNKNOWN_EXCEPTION).name(),
                code, null, cause), cause);

        errorCode = code;
    }

    public ClickHouseException(ClickHouseErrorCode error, Throwable cause, ClickHouseNode server) {
        super(buildErrorMessage(error, server, cause), cause);

        errorCode = error != null ? error.code : ClickHouseErrorCode.UNKNOWN_EXCEPTION.code;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
