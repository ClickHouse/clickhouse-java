package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;


/**
 * This class is intended to help writing data in row binary format.
 * It handles Nullable and Defaults.
 * It implements record and commit approach row-by-row. It means that data is not written immediately but it is stored
 * until {@link #commitRow()} is called.
 * <p>
 * Experimental API
 */
public class RowBinaryFormatWriter {

    private final OutputStream out;

    private final TableSchema tableSchema;

    private final Object[] row;

    private final boolean defaultSupport;

    public RowBinaryFormatWriter(OutputStream out, TableSchema tableSchema, ClickHouseFormat format) {
        if (format != ClickHouseFormat.RowBinary && format != ClickHouseFormat.RowBinaryWithDefaults) {
            throw new IllegalArgumentException("Only RowBinary and RowBinaryWithDefaults are supported");
        }

        this.out = out;
        this.tableSchema = tableSchema;
        this.row = new Object[tableSchema.getColumns().size()];
        this.defaultSupport = format == ClickHouseFormat.RowBinaryWithDefaults;
    }

    public void setValue(String column, Object value) {
        setValue(tableSchema.nameToColumnIndex(column), value);
    }

    public void setValue(int colIndex, Object value) {
        row[colIndex - 1] = value;
    }

    public void commitRow() throws IOException {
        List<ClickHouseColumn> columnList = tableSchema.getColumns();
        for (int i = 0; i < row.length; i++) {
            ClickHouseColumn column = columnList.get(i);

            if (RowBinaryFormatSerializer.writeValuePreamble(out, defaultSupport, column, row[i])) {
                SerializerUtils.serializeData(out, row[i], column);
            }
        }
    }

    public void setByte(String column, byte value) {
        setValue(column, value);
    }

    public void setByte(int colIndex, byte value) {
        setValue(colIndex, value);
    }

    public void setShort(String column, short value) {
        setValue(column, value);
    }

    public void setShort(int colIndex, short value) {
        setValue(colIndex, value);
    }

    public void setInteger(String column, int value) {
        setValue(column, value);
    }

    public void setInteger(int colIndex, int value) {
        setValue(colIndex, value);
    }

    public void setLong(String column, long value) {
        setValue(column, value);
    }

    public void setLong(int colIndex, long value) {
        setValue(colIndex, value);
    }

    public void setString(String column, String value) {
        setValue(column, value);
    }

    public void setString(int colIndex, String value) {
        setValue(colIndex, value);
    }

    public void setDate(String column, LocalDate value) {
        setValue(column, value);
    }

    public void setDate(int colIndex, LocalDate value) {
        setValue(colIndex, value);
    }

    public void setDateTime(String column, LocalDateTime value) {
        setValue(column, value);
    }

    public void setDateTime(int colIndex, LocalDateTime value) {
        setValue(colIndex, value);
    }

    public void setDateTime(String column, ZonedDateTime value) {
        setValue(column, value);
    }

    public void setDateTime(int colIndex, ZonedDateTime value) {
        setValue(colIndex, value);
    }

    public void setList(String column, List<?> value) {
        setValue(column, value);
    }

    public void setList(int colIndex, List<?> value) {
        setValue(colIndex, value);
    }
}
