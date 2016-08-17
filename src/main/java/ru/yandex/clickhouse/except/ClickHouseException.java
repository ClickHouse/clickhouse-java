package ru.yandex.clickhouse.except;

import java.sql.SQLException;

public class ClickHouseException extends SQLException {

    public ClickHouseException(int code, Throwable cause, String host, int port) {
        super("ClickHouse exception, code: " + code + ", host: " + host + ", port: " + port + "; "
                + (cause == null ? "" : cause.getMessage()), null, code, cause);
    }

    public ClickHouseException(int code, String message, Throwable cause, String host, int port) {
        super("ClickHouse exception, message: " + message + ", host: " + host + ", port: " + port + "; "
                + (cause == null ? "" : cause.getMessage()), null, code, cause);
    }
}
