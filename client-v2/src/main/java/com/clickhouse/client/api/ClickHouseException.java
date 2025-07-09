package com.clickhouse.client.api;

public class ClickHouseException extends RuntimeException {
    protected boolean isRetryable  = false;

    public ClickHouseException(String message) {
        super(message);
    }

    public ClickHouseException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClickHouseException(Throwable cause) {
        super(cause);
    }
    public boolean isRetryable() { return isRetryable; }
}
