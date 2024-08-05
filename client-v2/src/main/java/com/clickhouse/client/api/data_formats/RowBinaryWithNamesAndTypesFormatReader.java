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

    public RowBinaryWithNamesAndTypesFormatReader(InputStream inputStream) {
        this(inputStream, null);
    }

    public RowBinaryWithNamesAndTypesFormatReader(InputStream inputStream, QuerySettings querySettings) {
        super(inputStream, querySettings, null);
        readSchema();
    }

    private void readSchema() {
        try {
            TableSchema headerSchema = new TableSchema();
            List<String> columns = new ArrayList<>();
            int nCol;
            try {
                nCol = BinaryStreamReader.readVarInt(input);
            } catch (EOFException e) {
                endReached();
                return;
            }
            for (int i = 0; i < nCol; i++) {
                columns.add(BinaryStreamReader.readString(input));
            }

            for (int i = 0; i < nCol; i++) {
                headerSchema.addColumn(columns.get(i), BinaryStreamReader.readString(input));
            }

            setSchema(headerSchema);
        } catch (IOException e) {
            throw new ClientException("Failed to read header", e);
        }
    }
}
