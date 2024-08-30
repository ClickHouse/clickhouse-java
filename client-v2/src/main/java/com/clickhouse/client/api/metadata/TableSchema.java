package com.clickhouse.client.api.metadata;

import com.clickhouse.data.ClickHouseColumn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableSchema {

    private String tableName = "";

    private String databaseName = "";

    private List<ClickHouseColumn> columns;

    private List<ClickHouseColumn> columnsView;

    private Map<String, Map<String, Object>> metadata;

    private Map<String, Integer> colIndex;

    private boolean hasDefaults = false;

    public TableSchema() {
        this.metadata = new HashMap<>();
        this.columns = new ArrayList<>();
        this.columnsView = Collections.unmodifiableList(columns);
        this.colIndex = new HashMap<>();
    }

    /**
     * Returns unmodifiable collection of columns.
     *
     * @return - collection of columns in the table
     */
    public List<ClickHouseColumn> getColumns() {
        return columnsView;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public boolean hasDefaults() {
        return hasDefaults;
    }

    public void addColumn(String name, String type) {
        addColumn(name, type, "");
    }
    public void addColumn(String name, String type, String defaultType) {
        ClickHouseColumn column = ClickHouseColumn.of(name, type);
        if (defaultType.toUpperCase().contains("DEFAULT")) {
            hasDefaults = true;
            column.setHasDefault(true);
        }
        columns.add(column);

        Map<String, Object> columnMetadata = metadata.computeIfAbsent(name, k -> new HashMap<>());
            columnMetadata.put("type", type);
        colIndex.put(name, columns.size() - 1);
    }

    public ClickHouseColumn getColumnByName(String name) {
        for (ClickHouseColumn column : columns) {
            if (column.getColumnName().equalsIgnoreCase(name)) {
                return column;//TODO: Try to deep clone the column rather than reference pass
            }
        }

        return null;
    }

    public String indexToName(int index) {
        return columns.get(index).getColumnName();
    }

    public int nameToIndex(String name) {
        return colIndex.get(name).intValue();
    }

    @Override
    public String toString() {
        return "TableSchema{" +
                "tableName='" + tableName + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", columns=" + columns +
                ", metadata=" + metadata +
                ", colIndex=" + colIndex +
                ", hasDefaults=" + hasDefaults +
                '}';
    }
}

