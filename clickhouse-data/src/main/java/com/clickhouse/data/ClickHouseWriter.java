package com.clickhouse.data;

import java.io.IOException;

@Deprecated
@FunctionalInterface
public interface ClickHouseWriter {
    static final String TYPE_NAME = "Writer";

    /**
     * Writes data to output stream.
     *
     * @param output non-null output stream
     * @throws IOException when failed to write data to output stream
     */
    void write(ClickHouseOutputStream output) throws IOException;
}
