package ru.yandex.metrika.clickhouse.except;

import ru.yandex.metrika.clickhouse.CHException;

/**
 * @author lopashev
 * @since 16.02.15
 */
public class ClickhouseApiException extends CHException {
    public static final String MESSAGE =
            "Запрос не может быть обработан из-за внутренней ошибки. Мы проанализируем и постараемся устранить причину как можно быстрее. Пожалуйста, не отправляйте запрос повторно.";

    public ClickhouseApiException(Integer code, Throwable cause, String host, int port) {
        super(code, cause, host, port);
    }
}
