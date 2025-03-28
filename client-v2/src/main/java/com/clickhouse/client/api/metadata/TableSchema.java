package com.clickhouse.client.api.metadata;

import com.clickhouse.data.ClickHouseColumn;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableSchema {

    private final String tableName;

    private final String query;

    private final String databaseName;

    private final List<ClickHouseColumn> columns;

    private final Map<String, Integer> colIndex;

    private boolean hasDefaults = false;

    public TableSchema(Collection<ClickHouseColumn> columns) {
        this("", "", "", columns);
    }
    public TableSchema(String tableName, String query, String databaseName, Collection<ClickHouseColumn> columns) {
        this.tableName = tableName;
        this.databaseName = databaseName;
        this.query = query;
        this.columns = ImmutableList.copyOf(columns);
        ImmutableMap.Builder<String, Integer> colIndexMapBuilder = ImmutableMap.builder();
        for (int i = 0; i < this.columns.size(); i++) {
            ClickHouseColumn column= this.columns.get(i);
            if (column.hasDefault()) {
                this.hasDefaults = true;
            }
            colIndexMapBuilder.put(this.columns.get(i).getColumnName(), i);
        }
        this.colIndex = colIndexMapBuilder.build();
    }

    /**
     * Returns unmodifiable collection of columns.
     *
     * @return - collection of columns in the table
     */
    public List<ClickHouseColumn> getColumns() {
        return columns;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean hasDefaults() {
        return hasDefaults;
    }

    public String getQuery() {
        return query;
    }

    public ClickHouseColumn getColumnByName(String name) {
        return columns.get(nameToIndex(name));
    }

    /**
     * Returns column by index. Index starts with 1.
     * @param colIndex - column index;
     * @return
     */
    public ClickHouseColumn getColumnByIndex(int colIndex) {
        return columns.get(colIndex - 1);
    }

    /**
     * Takes absolute index (starting from 0) and returns corresponding column.
     *
     * @param index - column index starting from 0
     * @return - column name
     */
    public String indexToName(int index) {
        try {
            return columns.get(index).getColumnName();
        } catch (IndexOutOfBoundsException e) {
            throw new NoSuchColumnException("Result has no column with index = " + index);
        }
    }

    /**
     * Takes absolute index (starting from 1) and return corresponding column.
     * Equals to {@code indexToName(index - 1}.
     *
     * @param index - column index starting from 1
     * @return - column name.
     */
    public String columnIndexToName(int index) {
        return indexToName(index - 1);
    }

    public int nameToColumnIndex(String name) {
        return nameToIndex(name) + 1;
    }

    public int nameToIndex(String name) {
        Integer index = colIndex.get(name);
        if (index == null) {
            throw new NoSuchColumnException("Result has no column with name '" + name + "'");
        }
        return index;
    }

    @Override
    public String toString() {
        return "TableSchema{" +
                "tableName='" + tableName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", columns=" + columns +
                ", colIndex=" + colIndex +
                ", hasDefaults=" + hasDefaults +
                '}';
    }
}

