package com.clickhouse.client.api;

public class ConnectionInitiationException extends ClickHouseException {

    public ConnectionInitiationException(String message) {
        super(message);
        this.isRetryable = true;
    }

    public ConnectionInitiationException(String message, Throwable cause) {
        super(message, cause);
        this.isRetryable = true;
    }
}
