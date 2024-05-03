package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.internal.AbstractBinaryFormatReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RowBinaryWithNamesFormatReader extends AbstractBinaryFormatReader {

    private List<String> columns = null;

    public RowBinaryWithNamesFormatReader(InputStream inputStream, QuerySettings querySettings, TableSchema schema) {
        super(inputStream, querySettings, schema);
    }

    @Override
    public void readRecord(Map<String, Object> record) throws IOException {
        if (columns == null) {
            columns = new ArrayList<>();
            int nCol = chInputStream.readVarInt();
            for (int i = 0; i < nCol; i++) {
                columns.add(chInputStream.readUnicodeString());
            }

            columns = Collections.unmodifiableList(columns);
        }

        for (ClickHouseColumn column : getSchema().getColumns()) {
            record.put(column.getColumnName(), binaryStreamReader.readValue(column.getDataType()));
        }
    }

    public List<String> getColumns() {
        return columns;
    }

    @Override
    public boolean next() {
        try {
            readRecord(currentRecord);
            return true;
        } catch (EOFException e) {
            return false;
        } catch (IOException e) {
            throw new ClientException("Failed to read row", e);
        }
    }
}
