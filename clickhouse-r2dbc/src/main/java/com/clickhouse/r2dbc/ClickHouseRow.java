package com.clickhouse.r2dbc;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseRecord;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ClickHouseRow implements Row {

    final ClickHouseRecord record;
    final ClickHouseRowMetadata rowMetadata;

    ClickHouseRow(ClickHouseRecord record, List<ClickHouseColumn> columnList) {
        this.record = record;
        this.rowMetadata = new ClickHouseRowMetadata(columnList.stream()
                .map(ClickHouseColumnMetadata::new)
                .collect(Collectors
                        .toMap(ClickHouseColumnMetadata::getName,
                                Function.identity(),
                                (v1,v2) -> v2, // since every key will be unique, won't need to merge so just overwrite with the latest one.
                                LinkedHashMap::new)));
    }

    @Override
    public RowMetadata getMetadata() {
        return rowMetadata;
    }

    @Override
    public <T> T get(int i, Class<T> aClass) {
        return aClass.cast(record.getValue(i).asObject(aClass));
    }

    @Override
    public <T> T get(String name, Class<T> aClass) {
        try {
            return aClass.cast(record.getValue(name).asObject(aClass));
        } catch (IllegalArgumentException e) {
            throw new NoSuchElementException(String.format("Unknown element with a name %s", name));
        }
    }
}
