package com.clickhouse.client.api.serde;

import com.clickhouse.client.api.exception.ClientException;

public class SerializerNotFoundException extends ClientException {

    public SerializerNotFoundException(Class<?> pojoClass) {
        super("Serializer not found for the class '" + pojoClass.getName() + "'");
    }
}
