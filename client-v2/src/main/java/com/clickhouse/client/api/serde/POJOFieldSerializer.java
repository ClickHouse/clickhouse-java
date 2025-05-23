package com.clickhouse.client.api.serde;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;

public interface POJOFieldSerializer {
    void serialize(Object obj, OutputStream outputStream) throws InvocationTargetException, IllegalAccessException, IOException;
}
