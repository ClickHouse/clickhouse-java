package com.clickhouse.client.api;

public class ClientException extends ClickHouseException {

    public ClientException(String message) {
        super(message);
    }

    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClientException(String message, Throwable cause, String queryId) {
        super(message, cause, queryId);
    }
}
