package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.jdbc.ResultSetImpl;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

public class MetadataResultSet extends ResultSetImpl {
    private final Map<String, UnaryOperator<String>> columnTransformers = new HashMap<>();
    private final String[] cachedColumnLabels;

    private final OverridingSchemaAdaptor overridingSchemaAdaptor;

    public MetadataResultSet(ResultSetImpl resultSet) throws SQLException {
        super(resultSet);
        this.overridingSchemaAdaptor = new OverridingSchemaAdaptor(resultSet.getSchema());
        ResultSetMetaData metaData = getMetaData();
        int count = metaData.getColumnCount();
        cachedColumnLabels = new String[count];
        for (int i = 1; i <= count; i++) {
            cachedColumnLabels[i - 1] = metaData.getColumnLabel(i).toUpperCase();
        }
    }

    /**
     * Registers a transformer function for a given column.
     * The transformer takes the original String value and returns a new String.
     *
     * @param columnLabel The name of the column (case-insensitive).
     * @param transformer The function that transforms the value.
     */
    public MetadataResultSet transform(String columnLabel, ClickHouseColumn column, UnaryOperator<String> transformer) {
        if (columnLabel != null && transformer != null) {
            columnTransformers.put(columnLabel.toUpperCase(), transformer);
        }
        overridingSchemaAdaptor.setOverriddenColumn(overridingSchemaAdaptor.nameToIndex(columnLabel), column);
        return this;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        String value = super.getString(columnLabel);
        UnaryOperator<String> transformer = columnTransformers.get(columnLabel.toUpperCase());
        if (transformer != null && value != null) {
            return transformer.apply(value);
        }
        return value;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        String stringValue = getString(columnLabel);
        if (stringValue == null || stringValue.trim().isEmpty()) {
            return 0;
        }
        try {
            return Integer.parseInt(stringValue);
        } catch (NumberFormatException e) {
            throw new SQLException("Value for column '" + columnLabel + "' is not a valid integer: " + stringValue, e);
        }
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > cachedColumnLabels.length) {
            throw new SQLException("Invalid column index: " + columnIndex);
        }
        return getString(cachedColumnLabels[columnIndex - 1]);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > cachedColumnLabels.length) {
            throw new SQLException("Invalid column index: " + columnIndex);
        }
        return getInt(cachedColumnLabels[columnIndex - 1]);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return (byte) getInt(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return (short) getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getInt(columnIndex);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return (byte) getInt(columnLabel);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return (short) getInt(columnLabel);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getInt(columnLabel);
    }

    @Override
    public TableSchema getSchema() {
        return overridingSchemaAdaptor;
    }

    private static class OverridingSchemaAdaptor extends TableSchema {
        private final TableSchema originalSchema;

        private final List<ClickHouseColumn> overriddenColumns;

        public OverridingSchemaAdaptor(TableSchema originalSchema) {
            super(Collections.emptyList());
            this.originalSchema = originalSchema;
            this.overriddenColumns = new ArrayList<>(originalSchema.getColumns());
        }

        public void setOverriddenColumn(int index, ClickHouseColumn column) {
            if (index < 0 || index >= overriddenColumns.size()) {
                throw new IndexOutOfBoundsException("Index " + index + " is out of bounds for overridden columns.");
            }
            overriddenColumns.set(index, column);
        }

        @Override
        public List<ClickHouseColumn> getColumns() {
            return overriddenColumns;
        }

        @Override
        public String getDatabaseName() {
            return originalSchema.getDatabaseName();
        }

        @Override
        public String getTableName() {
            return originalSchema.getTableName();
        }

        @Override
        public boolean hasDefaults() {
            return originalSchema.hasDefaults();
        }

        @Override
        public String getQuery() {
            return originalSchema.getQuery();
        }

        @Override
        public ClickHouseColumn getColumnByName(String name) {
            return overriddenColumns.get(originalSchema.nameToIndex(name));
        }

        @Override
        public ClickHouseColumn getColumnByIndex(int colIndex) {
            return overriddenColumns.get(colIndex - 1);
        }

        @Override
        public String indexToName(int index) {
            return originalSchema.indexToName(index);
        }

        @Override
        public String columnIndexToName(int index) {
            return originalSchema.columnIndexToName(index);
        }

        @Override
        public int nameToColumnIndex(String name) {
            return originalSchema.nameToColumnIndex(name);
        }

        @Override
        public int nameToIndex(String name) {
            return originalSchema.nameToIndex(name);
        }

        @Override
        public String toString() {
            return originalSchema.toString();
        }
    }
}
