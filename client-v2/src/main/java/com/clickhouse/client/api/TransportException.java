package com.clickhouse.client.api;

/**
 * Transport-layer exception that is hard to categorize as connection initiation or data transfer.
 * These exceptions are not retryable by default.
 * Main purpose of this exception is to wrap transport-specific failures (e.g., SSL errors).
 */
public class TransportException extends ClickHouseException {
    public TransportException(String message, Throwable cause, String queryId) {
        super(message, cause, queryId);
        this.isRetryable = false;
    }
}
