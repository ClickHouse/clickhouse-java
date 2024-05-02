package com.clickhouse.client.api.data_formats;

import java.io.IOException;
import java.util.Map;

public interface ClickHouseRowBinaryStreamReader {


    /**
     * Read a record from the stream and write it to the given map.
     * @param record
     * @param schema - mapping schema of columns in the record
     * @throws IOException
     */
    void readToMap(Map<String, Object> record) throws IOException;
}
