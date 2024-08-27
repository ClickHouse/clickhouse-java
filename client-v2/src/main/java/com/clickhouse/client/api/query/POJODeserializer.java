package com.clickhouse.client.api.query;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public interface POJODeserializer {

    /**
     * Read data from a inputStream to fill a single object.
     *
     * @param obj - target object to fill with data
     * @param value - value of the field.
     * @throws InvocationTargetException when the underlying method throws an exception.
     * @throws IllegalAccessException - when the underlying method is inaccessible.
     * @throws IOException - when an I/O error occurs.
     */
    void deserialize(Object obj, Object value) throws InvocationTargetException, IllegalAccessException, IOException;
}
