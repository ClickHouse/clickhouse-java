package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.internal.AbstractBinaryFormatReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class RowBinaryFormatReader extends AbstractBinaryFormatReader {

    public RowBinaryFormatReader(InputStream inputStream, TableSchema schema) {
        this(inputStream, null, schema);
    }

    public RowBinaryFormatReader(InputStream inputStream, QuerySettings querySettings, TableSchema schema) {
        super(inputStream, querySettings, schema);
    }

    @Override
    public void readRecord(Map<String, Object> record) throws IOException {
        for (ClickHouseColumn column : getSchema().getColumns()) {
            record.put(column.getColumnName(), binaryStreamReader
                    .readValue(column));
        }
    }
}
