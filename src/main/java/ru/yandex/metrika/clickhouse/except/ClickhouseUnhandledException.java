package ru.yandex.metrika.clickhouse.except;

import ru.yandex.metrika.clickhouse.CHException;

/**
 * @author lopashev
 * @since 16.02.15
 */
public class ClickhouseUnhandledException extends CHException {
    public static final String MESSAGE =
            "Запрос не может быть обработан по неизвестной причине. Мы проанализируем и постараемся устранить причину как можно быстрее. Пожалуйста, отправьте запрос повторно через несколько минут.";

    public ClickhouseUnhandledException(Throwable cause, String host, int port) {
        super(cause, host, port);
    }


    public ClickhouseUnhandledException(String message, Throwable cause, String host, int port) {
        super(message, cause, host, port);
    }

    public ClickhouseUnhandledException(Integer code, Throwable cause, String host, int port) {
        super(code, cause, host, port);
    }

}
