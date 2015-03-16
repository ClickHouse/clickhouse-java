package ru.yandex.metrika.clickhouse.except;


import ru.yandex.metrika.clickhouse.CHException;

/**
 * @author lopashev
 * @since 16.02.15
 */
public class ClickhouseQueryException extends CHException {
    public static final String MESSAGE =
            "Запрос слишком сложный и не может быть обработан. Пожалуйста, уменьшите интервал дат запроса либо включите/уменьшите семплирование и повторите запрос.";

    public ClickhouseQueryException(Integer code, Throwable cause, String host, int port) {
        super(code, cause, host, port);
    }
}
