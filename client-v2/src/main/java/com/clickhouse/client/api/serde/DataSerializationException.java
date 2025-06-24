package com.clickhouse.client.api.serde;

import com.clickhouse.client.api.ClientException;

public class DataSerializationException extends ClientException {

    public DataSerializationException(String message) {
        super(message);
    }

    public DataSerializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataSerializationException(Object obj, POJOFieldSerializer serializer, Exception e) {
        super("Failed to serialize data '" + obj + "' with serializer '" + serializer + "'", e);
    }
}
