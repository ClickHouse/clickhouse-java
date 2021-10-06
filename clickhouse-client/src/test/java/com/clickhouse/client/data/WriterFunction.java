package com.clickhouse.client.data;

import java.io.IOException;
import java.io.OutputStream;

@FunctionalInterface
public interface WriterFunction {
    void write(OutputStream o) throws IOException;
}
