package com.clickhouse.client.api.data_formats.internal;

import java.util.Map;

/**
 * Interface for JSON row processors.
 */
public interface JsonParser extends AutoCloseable {
    /**
     * Reads next row from the input stream.
     * @return map of column names to values, or null if no more rows
     * @throws Exception if an error occurs during parsing
     */
    Map<String, Object> nextRow() throws Exception;
}
