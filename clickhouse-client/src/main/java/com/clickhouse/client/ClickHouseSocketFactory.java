package com.clickhouse.client;

import java.io.IOException;

/**
 * Generic factory interface for creating sockets used by the TCP and Apache
 * Http clients.
 */
@Deprecated
public interface ClickHouseSocketFactory {
    /**
     * Creates a new instance of the provided configuration and class type.
     *
     * @param <T>    type of class to create
     * @param config configuration
     * @param clazz  class instance for the type to instantiate
     * @return non-null new instance of the given class
     * @throws IOException                   when failed to create the instance
     * @throws UnsupportedOperationException when the given class is not supported
     */
    <T> T create(ClickHouseConfig config, Class<T> clazz) throws IOException, UnsupportedOperationException;

    /**
     * Tests whether this factory supports creating instances of the given class
     * type. For example, before calling {@link #create(ClickHouseConfig, Class)},
     * you may want to call this method first to verify the factory can produce the
     * desired type.
     *
     * @param clazz non-null class reflecting the type to check support for
     * @return true if the factory supports creating instances of the given type;
     *         false otherwise
     */
    boolean supports(Class<?> clazz);
}
