package ru.yandex.clickhouse.except;

public class ClickHouseUnknownException extends ClickHouseException {

    public ClickHouseUnknownException(Throwable cause, String host, int port) {
        super(ClickHouseErrorCode.UNKNOWN_EXCEPTION.code, cause, host, port);
    }


    public ClickHouseUnknownException(String message, Throwable cause, String host, int port) {
        super(ClickHouseErrorCode.UNKNOWN_EXCEPTION.code, message, cause, host, port);
    }

    public ClickHouseUnknownException(Integer code, Throwable cause, String host, int port) {
        super(code, cause, host, port);
    }

}
