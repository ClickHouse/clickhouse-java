package ru.yandex.metrika.clickhouse.except;

import java.sql.SQLException;

/**
 * Created by jkee on 16.03.15.
 */
public class CHException extends SQLException {

    public CHException(int code, Throwable cause, String host, int port) {
        super("ClickHouse exception, code: " + code + ", host: " + host + ", port: " + port + "; " + cause.getMessage(), null, code, cause);
    }

    public CHException(int code, String message, Throwable cause, String host, int port) {
        super("ClickHouse exception, message: " + message + ", host: " + host + ", port: " + port + "; " + cause.getMessage(), null, code, cause);
    }
}
