package ru.yandex.metrika.clickhouse.except;

import ru.yandex.metrika.clickhouse.CHException;

/**
 * @author lopashev
 * @since 16.02.15
 */
public class ClickhouseUnhandledException extends CHException {

    public ClickhouseUnhandledException(Throwable cause, String host, int port) {
        super(ClickhouseErrorCode.UNKNOWN_EXCEPTION.code, cause, host, port);
    }


    public ClickhouseUnhandledException(String message, Throwable cause, String host, int port) {
        super(ClickhouseErrorCode.UNKNOWN_EXCEPTION.code, message, cause, host, port);
    }

    public ClickhouseUnhandledException(Integer code, Throwable cause, String host, int port) {
        super(code, cause, host, port);
    }

}
