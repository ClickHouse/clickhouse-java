package com.clickhouse.client.api.insert;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public interface POJOSerializer {
    void serialize(Object obj, OutputStream outputStream, List<String> columns) throws InvocationTargetException, IllegalAccessException, IOException;
}
