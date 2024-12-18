package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.metadata.TableSchema;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class AdvancedRowBinaryFormatWriter {

    private final OutputStream out;

    private final TableSchema tableSchema;

    private final RowBinaryFormatWriter binWriter;

    private final Map<String, Object> row;

    public AdvancedRowBinaryFormatWriter(OutputStream out, TableSchema tableSchema) {
        this.out = out;
        this.tableSchema = tableSchema;

        this.row = new HashMap<>();
        this.binWriter = new RowBinaryFormatWriter(out);
    }

    public void setString(String column, String value) {
        row.put(column, value);
    }

    public void flushRow() {

    }

}
