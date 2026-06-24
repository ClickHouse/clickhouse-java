package com.clickhouse.client.api;

/**
 * Any exception that happens inside transport logic and hard to categorize as client logic
 * like connection initiation or data transfer. These exceptions are not retriable normally.
 * Main purpose of this exception is to wrap transport specific.
 */
public class TransportException extends ClickHouseException {
    public TransportException(String message, Throwable cause, String queryId) {
        super(message, cause, queryId);
        this.isRetryable = false;
    }
}
