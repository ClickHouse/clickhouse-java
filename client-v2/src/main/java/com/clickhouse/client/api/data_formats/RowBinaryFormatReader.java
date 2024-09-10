package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.AbstractBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class RowBinaryFormatReader extends AbstractBinaryFormatReader {

    public RowBinaryFormatReader(InputStream inputStream, QuerySettings querySettings, TableSchema schema,
                                 BinaryStreamReader.ByteBufferAllocator byteBufferAllocator) {
        super(inputStream, querySettings, schema, byteBufferAllocator);
        readNextRecord();
    }

    @Override
    public boolean readRecord(Map<String, Object> record) throws IOException {
        boolean firstColumn = true;
        for (ClickHouseColumn column : getSchema().getColumns()) {
            try {
                Object val = binaryStreamReader.readValue(column);
                if (val != null) {
                    record.put(column.getColumnName(),val);
                }
                firstColumn = false;
            } catch (EOFException e) {
                if (firstColumn) {
                    endReached();
                    return false;
                }
                throw e;
            }
        }
        return true;
    }
}
