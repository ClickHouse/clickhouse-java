package com.clickhouse.client;

import java.io.IOException;

import com.clickhouse.client.data.ClickHouseEmptyValue;

/**
 * Functional interface for deserialization.
 */
@FunctionalInterface
public interface ClickHouseDeserializer<T extends ClickHouseValue> {
    /**
     * Default deserializer simply returns empty value.
     */
    ClickHouseDeserializer<ClickHouseValue> EMPTY_VALUE = (v, f, c, i) -> ClickHouseEmptyValue.INSTANCE;

    /**
     * Default deserializer throws IOException to inform caller deserialization is
     * not supported.
     */
    ClickHouseDeserializer<ClickHouseValue> NOT_SUPPORTED = (v, f, c, i) -> {
        throw new IOException(c.getOriginalTypeName() + " is not supported");
    };

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
    T deserialize(T ref, ClickHouseConfig config, ClickHouseColumn column, ClickHouseInputStream input)
            throws IOException;
}
