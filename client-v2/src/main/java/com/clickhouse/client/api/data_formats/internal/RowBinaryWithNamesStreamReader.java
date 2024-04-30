package com.clickhouse.client.api.data_formats.internal;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QuerySettings;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RowBinaryWithNamesStreamReader extends AbstractRowBinaryReader {

    private List<String> columns = null;

    public RowBinaryWithNamesStreamReader(InputStream inputStream, QuerySettings querySettings) {
        super(inputStream, querySettings);
    }

    @Override
    public void readToMap(Map<String, Object> record, TableSchema schema) throws IOException {
        if (columns == null) {
            columns = new ArrayList<>();
            int nCol = chInputStream.readVarInt();
            for (int i = 0; i < nCol; i++) {
                columns.add(chInputStream.readUnicodeString());
            }

            columns = Collections.unmodifiableList(columns);
        }
        super.readToMap(record, schema);
    }

    public List<String> getColumns() {
        return columns;
    }
}
