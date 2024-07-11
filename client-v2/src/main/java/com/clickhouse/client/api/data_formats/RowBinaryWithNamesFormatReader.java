package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.AbstractBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RowBinaryWithNamesFormatReader extends AbstractBinaryFormatReader {

    private List<String> columns = null;

    public RowBinaryWithNamesFormatReader(InputStream inputStream, TableSchema schema) {
        this(inputStream, null, schema);
    }

    public RowBinaryWithNamesFormatReader(InputStream inputStream, QuerySettings querySettings, TableSchema schema) {
        super(inputStream, querySettings, schema);
    }

    @Override
    public void readRecord(Map<String, Object> record) throws IOException {
        if (columns == null) {
            columns = new ArrayList<>();
            int nCol = BinaryStreamReader.readVarInt(input);
            for (int i = 0; i < nCol; i++) {
                columns.add(BinaryStreamReader.readString(input));
            }

            columns = Collections.unmodifiableList(columns);
        }

        for (ClickHouseColumn column : getSchema().getColumns()) {
            record.put(column.getColumnName(), binaryStreamReader
                    .readValue(column));
        }
    }

    public List<String> getColumns() {
        return columns;
    }
}
