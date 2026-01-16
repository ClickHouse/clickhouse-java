package com.clickhouse.client.api;

public class ClickHouseException extends RuntimeException {
    protected boolean isRetryable  = false;

    protected String queryId;

    public ClickHouseException(String message) {
        super(message);
    }

    public ClickHouseException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClickHouseException(String message, Throwable cause, String queryId) {
        super(message, cause);
        this.queryId = queryId;
    }

    public ClickHouseException(Throwable cause) {
        super(cause);
    }
    public boolean isRetryable() { return isRetryable; }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getQueryId() {
        return queryId;
    }
}
