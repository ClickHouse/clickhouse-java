package ru.yandex.metrika.clickhouse.except;

/**
 * @author lopashev
 * @since 16.02.15
 */
public class CHUnknownException extends CHException {

    public CHUnknownException(Throwable cause, String host, int port) {
        super(CHErrorCode.UNKNOWN_EXCEPTION.code, cause, host, port);
    }


    public CHUnknownException(String message, Throwable cause, String host, int port) {
        super(CHErrorCode.UNKNOWN_EXCEPTION.code, message, cause, host, port);
    }

    public CHUnknownException(Integer code, Throwable cause, String host, int port) {
        super(code, cause, host, port);
    }

}
