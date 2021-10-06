package com.clickhouse.client;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Functional interface for serializtion.
 */
@FunctionalInterface
public interface ClickHouseSerializer<T extends ClickHouseValue> {
    /**
     * Writes serialized value to output stream.
     *
     * @param value  non-null value to be serialized
     * @param column non-null type information
     * @param output non-null output stream
     * @throws IOException when failed to write data to output stream
     */
    void serialize(T value, ClickHouseColumn column, OutputStream output) throws IOException;
}
