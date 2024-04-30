package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RowBinaryWithNamesAndTypesReader extends AbstractRowBinaryReader {

    private List<String> columns = null;

    private List<String> types = null;

    public RowBinaryWithNamesAndTypesReader(InputStream inputStream, QuerySettings querySettings) {
        super(inputStream, querySettings);
    }


    /**
     * Reads a row to a map using column definitions from the stream
     *
     * @param record
     */
    public void readToMap(Map<String, Object> record) throws IOException {
        if (columns == null) {
            readHeader();
        }

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
        if (columns == null) {
            readHeader();
        }

        super.readToMap(record, schema);
    }

    private void readHeader() throws IOException {
        columns = new ArrayList<>();
        int nCol = chInputStream.readVarInt();
        for (int i = 0; i < nCol; i++) {
            columns.add(chInputStream.readUnicodeString());
        }

        columns = Collections.unmodifiableList(columns);

        types = new ArrayList<>();
        for (int i = 0; i < nCol; i++) {
            types.add(chInputStream.readUnicodeString());
        }

        types = Collections.unmodifiableList(types);
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<String> getTypes() {
        return types;
    }
}
