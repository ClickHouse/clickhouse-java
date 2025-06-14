package com.clickhouse.client.api;

import com.clickhouse.client.api.exception.ClientException;

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
