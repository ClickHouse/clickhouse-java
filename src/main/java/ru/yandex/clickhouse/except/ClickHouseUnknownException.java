package ru.yandex.clickhouse.except;

public class ClickHouseUnknownException extends ClickHouseException {

    public ClickHouseUnknownException(Throwable cause, String host, int port) {
        super(ClickHouseErrorCode.UNKNOWN_EXCEPTION, cause, host, port);
    }


    public ClickHouseUnknownException(String message, Throwable cause, String host, int port) {
        super(ClickHouseErrorCode.UNKNOWN_EXCEPTION, message, cause, host, port);
    }

    public ClickHouseUnknownException(int code, String message, Throwable cause, String host, int port) {
        super(code, message, cause, host, port);
    }

}
