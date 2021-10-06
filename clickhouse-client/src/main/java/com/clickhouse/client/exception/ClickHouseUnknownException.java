package com.clickhouse.client.exception;

public class ClickHouseUnknownException extends ClickHouseException {
    /**
     * Generated ID.
     */
    private static final long serialVersionUID = -1724790228244438601L;

    public ClickHouseUnknownException(Throwable cause, String host, int port) {
        super(ClickHouseErrorCode.UNKNOWN_EXCEPTION.code, cause, host, port);
    }

    public ClickHouseUnknownException(String message, Throwable cause, String host, int port) {
        super(ClickHouseErrorCode.UNKNOWN_EXCEPTION.code, message, cause, host, port);
    }

    public ClickHouseUnknownException(String message, Throwable cause) {
        super(ClickHouseErrorCode.UNKNOWN_EXCEPTION.code, message, cause);
    }

    public ClickHouseUnknownException(Integer code, Throwable cause, String host, int port) {
        super(code, cause, host, port);
    }
}
