package com.clickhouse.client.api.exception;

public class ClientException extends Throwable{
    public ClientException() {
        super();
    }

    public ClientException(Throwable cause) {
        super(cause);
    }

    public ClientException(String message) {
        super(message);
    }

    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
