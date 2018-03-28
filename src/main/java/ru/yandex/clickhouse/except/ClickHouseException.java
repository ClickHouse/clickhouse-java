package ru.yandex.clickhouse.except;

import java.sql.SQLException;

public class ClickHouseException extends SQLException {

    private final ClickHouseErrorCode errorCode;

    public ClickHouseException(ClickHouseErrorCode errorCode, Throwable cause, String host, int port) {
        super("ClickHouse exception, code: " + errorCode.name() + ", host: " + host + ", port: " + port + "; "
                + (cause == null ? "" : cause.getMessage()), null, errorCode.getCode(), cause);
        this.errorCode = errorCode;
    }

    public ClickHouseException(ClickHouseErrorCode errorCode, String message, Throwable cause, String host, int port) {
        super("ClickHouse exception, message: " + message + ", host: " + host + ", port: " + port + "; "
                + (cause == null ? "" : cause.getMessage()), null, errorCode.getCode(), cause);
        this.errorCode = errorCode;
    }

    protected ClickHouseException(int errorCode, String message, Throwable cause, String host, int port) {
        super("ClickHouse exception, message: " + message + ", host: " + host + ", port: " + port + "; "
            + (cause == null ? "" : cause.getMessage()), null, errorCode, cause);
        this.errorCode = ClickHouseErrorCode.UNKNOWN_EXCEPTION;
    }

    public ClickHouseErrorCode getClickHouseErrorCode() {
        return errorCode;
    }
}
