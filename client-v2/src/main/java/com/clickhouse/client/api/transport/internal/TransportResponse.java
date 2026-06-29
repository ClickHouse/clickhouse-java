package com.clickhouse.client.api.transport.internal;

import com.clickhouse.data.ClickHouseFormat;

import java.io.Closeable;
import java.io.InputStream;
import java.util.Map;

public interface TransportResponse extends Closeable {

    /**
     * Data format returned by server or calculated other way
     * @return data format
     */
    ClickHouseFormat getDataFormat();

    String getSummaryJson();

    String getQueryId();

    /**
     * Gives access to transport delegate. Used strictly only by transport.
     * @return internal transport response object
     * @param <T> - Type of delegate
     */
    <T> T getDelegate();


    /**
     * Server headers.
     * @return response headers
     */
    Map<String, String> getHeaders();


    /**
     * Creates a new stream to read data. It is applicable only for
     * blocking transports. In real life this should be called once.
     * It is important to mention that each time new input stream is created.
     *
     * @return new data stream
     */
    InputStream createDataInputStream();
}
