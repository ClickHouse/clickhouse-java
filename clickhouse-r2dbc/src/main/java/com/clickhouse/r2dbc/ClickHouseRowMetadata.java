package com.clickhouse.r2dbc;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

public class ClickHouseRowMetadata implements RowMetadata {

    LinkedHashMap<String, ClickHouseColumnMetadata> columnNameMetadataMap;

    ClickHouseRowMetadata(LinkedHashMap<String, ClickHouseColumnMetadata> columnNameMetadataMap) {
        this.columnNameMetadataMap = columnNameMetadataMap;
    }

    @Override
    public ColumnMetadata getColumnMetadata(int i) {
        if (i > columnNameMetadataMap.size())
            throw new IllegalArgumentException("Given index is greater than size column metadata array.");
        return columnNameMetadataMap.entrySet().stream().skip(i - 1L).findFirst().get().getValue(); // NOSONAR
    }

    @Override
    public ColumnMetadata getColumnMetadata(String columnName) {
        return columnNameMetadataMap.get(columnName);
    }

    @Override
    public List<? extends ColumnMetadata> getColumnMetadatas() {
        return Collections.unmodifiableList(new ArrayList<>(columnNameMetadataMap.values()));
    }

    // deprecated method
    public Collection<String> getColumnNames() {
        return Collections.unmodifiableCollection(new ArrayList<>(columnNameMetadataMap.keySet()));
    }
}
