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
import java.util.List;
import java.util.Map;

public class RowBinaryWithNamesAndTypesFormatReader extends AbstractBinaryFormatReader {

    public RowBinaryWithNamesAndTypesFormatReader(InputStream inputStream, QuerySettings querySettings) {
        super(inputStream, querySettings, null);
        setSchema(readSchema());
    }

    private TableSchema readSchema() {
        try {
            TableSchema headerSchema = new TableSchema();
            List<String> columns = new ArrayList<>();
            int nCol = chInputStream.readVarInt();
            for (int i = 0; i < nCol; i++) {
                columns.add(chInputStream.readUnicodeString());
            }

            for (int i = 0; i < nCol; i++) {
                headerSchema.addColumn(columns.get(i), chInputStream.readUnicodeString());
            }

            return headerSchema;
        } catch (IOException e) {
            throw new ClientException("Failed to read header", e);
        }
    }

    /**
     * Reads a row to a map using column definitions from the schema.
     * If column type mismatch and cannot be converted, an exception will be thrown.
     *
     * @param record data destination
     * @throws IOException
     */
    @Override
    public void readRecord(Map<String, Object> record) throws IOException {
        for (ClickHouseColumn column : getSchema().getColumns()) {
            record.put(column.getColumnName(), binaryStreamReader
                    .readValue(column));
        }
    }

    @Override
    public boolean next() {
        if (hasNext) {
            try {
                readRecord(currentRecord);
                return true;
            } catch (EOFException e) {
                hasNext = false;
                return false;
            } catch (IOException e) {
                hasNext = false;
                throw new ClientException("Failed to read row", e);
            }
        }
        return false;
    }
}
