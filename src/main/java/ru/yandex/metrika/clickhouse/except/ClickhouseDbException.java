package ru.yandex.metrika.clickhouse.except;

import ru.yandex.metrika.clickhouse.CHException;

/**
 * @author lopashev
 * @since 16.02.15
 */
public class ClickhouseDbException extends CHException {

    public ClickhouseDbException(Integer code, Throwable cause, String host, int port) {
        super(code, cause, host, port);
    }
}
