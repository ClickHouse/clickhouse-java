package com.clickhouse.client;

import java.io.IOException;
import java.io.InputStream;

/**
 * Functional interface for deserialization.
 */
@FunctionalInterface
public interface ClickHouseDeserializer<T extends ClickHouseValue> {
    /**
     * Deserializes data read from input stream.
     *
     * @param ref    wrapper object can be reused, could be null(always return new
     *               wrapper object)
     * @param config non-null configuration
     * @param column non-null type information
     * @param input  non-null input stream
     * @return deserialized value which might be the same instance as {@code ref}
     * @throws IOException when failed to read data from input stream
     */
    T deserialize(T ref, ClickHouseConfig config, ClickHouseColumn column, InputStream input) throws IOException;
}
