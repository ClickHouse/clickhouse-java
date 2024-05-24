package com.clickhouse.client.api.insert;

import com.clickhouse.client.api.ClientException;

public class SerializerNotFoundException extends ClientException {

    public SerializerNotFoundException(Class<?> pojoClass) {
        super("Serializer not found for the class '" + pojoClass.getName() + "'");
    }
}
