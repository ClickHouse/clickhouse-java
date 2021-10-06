package com.clickhouse.client.exception;

public class ClickHouseException extends Exception {
    /**
     * Generated ID.
     */
    private static final long serialVersionUID = -2417038200885554382L;

    private final int errorCode;

    private static String buildErrorMessage(String message, int code, String host, int port, Throwable cause) {
        StringBuilder builder = new StringBuilder();

        builder.append("ClickHouse exception, ");
        if (message != null) {
            builder.append(" message: ").append(message);
        } else {
            builder.append(" code: ").append(code);
        }

        if (host != null) {
            builder.append(", host: ").append(host).append(", port: ").append(port);
        }

        builder.append(';');
        if (cause != null) {
            builder.append(' ').append(cause.getMessage());
        }

        return builder.toString();
    }

    public ClickHouseException(int code, Throwable cause, String host, int port) {
        super(buildErrorMessage(null, code, host, port, cause), cause);

        errorCode = code;
    }

    public ClickHouseException(int code, String message, Throwable cause, String host, int port) {
        super(buildErrorMessage(message, code, host, port, cause), cause);

        errorCode = code;
    }

    public ClickHouseException(int code, String message, Throwable cause) {
        super(buildErrorMessage(message, code, null, 0, cause), cause);

        errorCode = code;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
