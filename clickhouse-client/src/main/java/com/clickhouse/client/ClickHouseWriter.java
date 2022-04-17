package com.clickhouse.client;

import java.io.IOException;

@FunctionalInterface
public interface ClickHouseWriter {
    /**
     * Writes value to output stream.
     *
     * @param output non-null output stream
     * @throws IOException when failed to write data to output stream
     */
    void write(ClickHouseOutputStream output) throws IOException;
}
