package com.clickhouse.client;

import java.io.IOException;
import java.io.OutputStream;

@FunctionalInterface
public interface ClickHouseWriter {
    /**
     * Writes value to output stream.
     *
     * @param output non-null output stream
     * @throws IOException when failed to write data to output stream
     */
    void write(OutputStream output) throws IOException;
}
