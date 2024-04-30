package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class RowBinaryWithNamesAndTypesReader extends AbstractRowBinaryReader{
    public RowBinaryWithNamesAndTypesReader(InputStream inputStream, QuerySettings querySettings) {
        super(inputStream, querySettings);
    }


    /**
     * Reads a row to a map using column definitions from the stream
     *
     * @param record
     */
    public void readToMap(Map<String, Object> record) {

    }


    /**
     * Reads a row to a map using column definitions from the schema.
     * If column type mismatch and cannot be converted, an exception will be thrown.
     *
     * @param record data destination
     * @param schema table scheme
     * @throws IOException
     */
    @Override
    public void readToMap(Map<String, Object> record, TableSchema schema) throws IOException {
        super.readToMap(record, schema);
    }
}
