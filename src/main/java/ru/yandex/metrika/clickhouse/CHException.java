package ru.yandex.metrika.clickhouse;

import java.sql.SQLException;

/**
 * Created by jkee on 16.03.15.
 */
public class CHException extends SQLException {


    public CHException(String reason) {
        super(reason);
    }

    public CHException(String reason, int vendorCode) {
        super(reason, null, vendorCode);
    }

    public CHException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public CHException(Throwable cause, String host, int port) {
        super("ClickHouse exception, host: " + host + ", port: " + port, cause);
    }

    public CHException(int code, Throwable cause, String host, int port) {
        super("ClickHouse exception, code: " + code + ", host: " + host + ", port: " + port, cause);
    }

    public CHException(String message, Throwable cause, String host, int port) {
        super("ClickHouse exception, message: " + message + ", host: " + host + ", port: " + port, cause);
    }
}
