package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.internal.AbstractBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RowBinaryWithNamesAndTypesFormatReader extends AbstractBinaryFormatReader implements Iterator<Map<String, Object>> {

    public RowBinaryWithNamesAndTypesFormatReader(InputStream inputStream, QuerySettings querySettings,
                                                  BinaryStreamReader.ByteBufferAllocator byteBufferAllocator) {
        super(inputStream, querySettings, null, byteBufferAllocator);
        readSchema();
    }

    private void readSchema() {
        try {
            List<String> names = new ArrayList<>();
            int nCol;
            try {
                nCol = BinaryStreamReader.readVarInt(input);
            } catch (EOFException e) {
                endReached();
                return;
            }
            for (int i = 0; i < nCol; i++) {
                names.add(binaryStreamReader.readString());
            }

            List<ClickHouseColumn> columns = new ArrayList<>(nCol);
            for (int i = 0; i < nCol; i++) {
                columns.add(ClickHouseColumn.of(names.get(i), binaryStreamReader.readString()));
            }

            TableSchema schema = new TableSchema(columns);
            setSchema(schema);
        } catch (IOException e) {
            throw new ClientException("Failed to read header", e);
        }
    }
}
