package com.clickhouse.client.api.metadata;

import com.clickhouse.client.api.ClientException;

public class NoSuchColumnException extends ClientException {

    public NoSuchColumnException(String message) {
        super(message);
    }
}
