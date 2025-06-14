package com.clickhouse.client.api.exception;

public class ConnectionInitiationException extends ClientException {

    public ConnectionInitiationException(String message) {
        super(message);
    }

    public ConnectionInitiationException(String message, Throwable cause) {
        super(message, cause);
    }
}
