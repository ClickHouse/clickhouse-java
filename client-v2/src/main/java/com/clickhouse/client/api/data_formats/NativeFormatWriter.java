package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.format.BinaryStreamUtils;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;

public class NativeFormatWriter implements Closeable {

    private final OutputStream outputStream;

    private final TableSchema tableSchema;

    private final Column[] columns;

    private int currentRowIndx = 0;

    private byte[][] columnHeaders;

    public NativeFormatWriter(OutputStream outputStream, TableSchema schema) {
        this(outputStream, schema, 1000);
    }

    public NativeFormatWriter(OutputStream outputStream, TableSchema schema, int valuesInBlock) {
        this.outputStream = outputStream;
        this.tableSchema = schema;

        this.columns = new Column[tableSchema.getColumns().size()];
        this.columnHeaders = new byte[columns.length][];
        List<ClickHouseColumn> tableColumns = schema.getColumns();
        try {
            for (int i = 0; i < columns.length; i++) {
                columns[i] = new Column(valuesInBlock);
                ByteArrayOutputStream header = new ByteArrayOutputStream();
                BinaryStreamUtils.writeString(header, tableColumns.get(i).getColumnName());
                BinaryStreamUtils.writeString(header, tableColumns.get(i).getOriginalTypeName());

                columnHeaders[i] = header.toByteArray();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error creating NativeFormatWriter", e);
        }
    }

    public void nextRow() throws IOException {
        currentRowIndx++;
    }

    public void commitBlock() throws IOException {

        if (currentRowIndx == 0) {
            return;
        }
        SerializerUtils.writeVarInt(outputStream, columns.length);
        SerializerUtils.writeVarInt(outputStream, currentRowIndx);

        List<ClickHouseColumn> columnList = tableSchema.getColumns();
        for (int i = 0; i < columns.length; i++) {
            ClickHouseColumn column = columnList.get(i);
            outputStream.write(columnHeaders[i], 0, columnHeaders[i].length);

            if (column.isNullable()) {
                // write null bitmap
                outputStream.write(columns[i].getNullableBitmap(), 0, currentRowIndx);
            }

            for (int j = 0; j < currentRowIndx; j++) {
                Object value = columns[i].getValues()[j];
                if (value != null) {
                    SerializerUtils.serializeData(outputStream, value, column);
                } else {
                    // write null string
                    // TODO: placeholder for null values for different types
                    switch (column.getDataType()) {
                        case String:
                        case FixedString:
                            BinaryStreamUtils.writeString(outputStream, "");
                            break;
                        case Int8:
                            BinaryStreamUtils.writeInt8(outputStream, (byte) 0);
                            break;
                        default:
                            BinaryStreamUtils.writeInt8(outputStream, (byte) 0);
                    }
                }
            }
            outputStream.flush();
        }

        currentRowIndx = 0;
    }

    public void setValue(String column, Object value) {
        setValue(tableSchema.nameToColumnIndex(column), value);
    }

    public void setValue(int colIndex, Object value) {
        columns[colIndex - 1].setValue(currentRowIndx, value);
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

    @Override
    public void close() throws IOException {
        commitBlock();
        outputStream.close();
    }

    public static class Column {
        private final Object[] values;

        private final byte[] nullableBitmap;

        public Column(int size) {
            this.values = new Object[size];
            this.nullableBitmap = new byte[size];
        }

        public void setValue(int index, Object value) {
            values[index] = value;
            nullableBitmap[index] = value == null ? (byte) 1 : (byte) 0;
        }

        public Object[] getValues() {
            return values;
        }

        public byte[] getNullableBitmap() {
            return nullableBitmap;
        }
    }
}
