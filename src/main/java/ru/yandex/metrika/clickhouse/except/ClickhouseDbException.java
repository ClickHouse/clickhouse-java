package ru.yandex.metrika.clickhouse.except;

import ru.yandex.metrika.clickhouse.CHException;

/**
 * @author lopashev
 * @since 16.02.15
 */
public class ClickhouseDbException extends CHException {
    public static final String MESSAGE =
            "Запрос не может быть обработан в данный момент из-за возросшей нагрузки. Пожалуйста, отправьте запрос повторно через несколько минут.";

    public ClickhouseDbException(Integer code, Throwable cause, String host, int port) {
        super(code, cause, host, port);
    }
}
