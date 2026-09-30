package com.clickhouse.client.api;

public enum ClientFaultCause {

    None,

    NoHttpResponse,
    ConnectTimeout,
    ConnectionRequestTimeout,
    SocketTimeout,
    ServerRetryable,

    /**
     * Server error {@code 159} ({@code TIMEOUT_EXCEEDED}). Not covered by {@link #ServerRetryable} and not retried
     * by default.
     */
    ServerTimeoutExceeded,
}
