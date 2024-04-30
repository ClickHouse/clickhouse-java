package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.RowBinaryStreamReader;
import com.clickhouse.client.api.data_formats.internal.RowBinaryWithNamesAndTypesReader;
import com.clickhouse.client.api.data_formats.internal.RowBinaryWithNamesStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;

import java.io.IOException;
import java.util.Map;

public interface ClickHouseBinaryStreamReader {

    /**
     * Reads a row to a map using column definitions from the stream
     * see {@link TableSchema}
     *
     * @param record data destination
     * @param schema table scheme
     */
    void readToMap(Map<String, Object> record, TableSchema schema) throws IOException;

    /**
     * Resets the reader to the beginning of the stream.
     */
    void reset() throws IOException;

    static ClickHouseBinaryStreamReader create(QueryResponse response, QuerySettings settings) {
        try {
            switch (response.getFormat()) {
                case RowBinary:
                    return new RowBinaryStreamReader(response.getInputStream(), settings);
                case RowBinaryWithNames:
                    return new RowBinaryWithNamesStreamReader(response.getInputStream(), settings);
                case RowBinaryWithNamesAndTypes:
                    return new RowBinaryWithNamesAndTypesReader(response.getInputStream(), settings);
                case Native:
                    throw new UnsupportedOperationException("Native format is not supported yet");
                default:
                    throw new IllegalStateException("Format: " + response.getFormat() + " is not compatible with RowBinaryStreamReader");
            }
        } catch (Exception e) {
            throw new RuntimeException(e); // TODO: handle exception
        }
    }
}
