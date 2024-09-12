package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.AbstractBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RowBinaryWithNamesFormatReader extends AbstractBinaryFormatReader {

    private List<String> columns = null;

    public RowBinaryWithNamesFormatReader(InputStream inputStream, QuerySettings querySettings, TableSchema schema,
                                          BinaryStreamReader.ByteBufferAllocator byteBufferAllocator) {
        super(inputStream, querySettings, schema, byteBufferAllocator);
        int nCol = 0;
        try {
            nCol = BinaryStreamReader.readVarInt(input);
        } catch (EOFException e) {
            endReached();
            columns = Collections.emptyList();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read header", e);
        }

        if (nCol > 0) {
            columns = new ArrayList<>(nCol);
            try {
                for (int i = 0; i < nCol; i++) {
                    columns.add(BinaryStreamReader.readString(input));
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read header", e);
            }

            columns = Collections.unmodifiableList(columns);
        }
        readNextRecord();
    }

    public List<String> getColumns() {
        return columns;
    }
}
