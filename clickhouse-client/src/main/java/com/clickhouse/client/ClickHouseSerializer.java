package com.clickhouse.client;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Functional interface for serializtion.
 */
@FunctionalInterface
public interface ClickHouseSerializer<T extends ClickHouseValue> {
    /**
     * Default serializer simply does nothing.
     */
    ClickHouseSerializer<ClickHouseValue> DO_NOTHING = (v, f, c, o) -> {
    };

    /**
     * Default deserializer throws IOException to inform caller serialization is
     * not supported.
     */
    ClickHouseSerializer<ClickHouseValue> NOT_SUPPORTED = (v, f, c, o) -> {
        throw new IOException(c.getOriginalTypeName() + " is not supported");
    };

    /**
     * Writes serialized value to output stream.
     *
     * @param value  non-null value to be serialized
     * @param config non-null configuration
     * @param column non-null type information
     * @param output non-null output stream
     * @throws IOException when failed to write data to output stream
     */
    void serialize(T value, ClickHouseConfig config, ClickHouseColumn column, OutputStream output) throws IOException;
}
