package com.clickhouse.client.api;

/**
 * Represents errors caused by a client misconfiguration.
 */
public class ClientMisconfigurationException extends ClientException {
    public ClientMisconfigurationException(String message) {
        super(message);
    }

    public ClientMisconfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
