package com.clickhouse.client.api.insert;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;

public interface POJOSerializer {
    void serialize(Object obj, OutputStream outputStream) throws InvocationTargetException, IllegalAccessException, IOException;
}
