package com.clickhouse.client.api;

public class DataTransferException extends ClickHouseException {

    public DataTransferException(String message) {
        super(message);
    }

    public DataTransferException(String message, Throwable cause) {
        super(message, cause);
    }
}
