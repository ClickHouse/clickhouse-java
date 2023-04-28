package com.clickhouse.data.mapper;

import java.util.Iterator;
import java.util.List;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseRecordMapper;

public final class IterableRecordWrapper<T> implements Iterator<T> {
    private final List<ClickHouseColumn> columns;
    private final Iterator<ClickHouseRecord> records;
    private final Class<T> objClass;

    private ClickHouseRecordMapper mapper;

    public IterableRecordWrapper(List<ClickHouseColumn> columns, Iterator<ClickHouseRecord> records,
            Class<T> objClass) {
        if (columns == null || records == null || objClass == null) {
            throw new IllegalArgumentException("Non-null column list, records and object class are required");
        }
        this.columns = columns;
        this.records = records;
        this.objClass = objClass;
    }

    @Override
    public boolean hasNext() {
        return records.hasNext();
    }

    @Override
    public T next() {
        if (mapper == null) {
            mapper = ClickHouseRecordMapper.of(columns, objClass);
        }
        return mapper.mapTo(records.next(), objClass);
    }
}
