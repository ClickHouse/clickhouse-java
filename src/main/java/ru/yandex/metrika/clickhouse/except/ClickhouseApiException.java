package ru.yandex.metrika.clickhouse.except;

import ru.yandex.metrika.clickhouse.CHException;

/**
 * @author lopashev
 * @since 16.02.15
 */
public class ClickhouseApiException extends CHException {

    public ClickhouseApiException(Integer code, Throwable cause, String host, int port) {
        super(code, cause, host, port);
    }
}
