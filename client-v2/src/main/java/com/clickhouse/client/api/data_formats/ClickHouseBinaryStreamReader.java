package com.clickhouse.client.api.data_formats;

import java.io.IOException;

public interface ClickHouseBinaryStreamReader {

    /**
     * Reads a single value from the stream.
     *
     * @param <T>
     * @return
     * @throws IOException
     */
    <T> T readValue(int colIndex) throws IOException;

    /**
     * Reads a row to an array of objects.
     *
     * @param colName
     * @param <T>
     * @return
     * @throws IOException
     */
    <T> T readValue(String colName) throws IOException;

    /**
     * Checks if there are more rows to read.
     *
     * @return
     */
    boolean hasNext();

    /**
     * Moves cursor to next row.
     *
     * @return
     */
    boolean next();
}
