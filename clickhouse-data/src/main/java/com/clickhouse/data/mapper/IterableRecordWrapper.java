package com.clickhouse.data.mapper;

import java.util.Iterator;
import java.util.List;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseRecordMapper;

@Deprecated
public final class IterableRecordWrapper<T> implements Iterator<T> {
    private final ClickHouseDataConfig config;
    private final List<ClickHouseColumn> columns;
    private final Iterator<ClickHouseRecord> records;
    private final Class<T> objClass;
    private final T template;

    private ClickHouseRecordMapper mapper;

    public IterableRecordWrapper(ClickHouseDataConfig config, List<ClickHouseColumn> columns,
            Iterator<ClickHouseRecord> records, Class<T> objClass, T template) {
        if (columns == null || records == null || objClass == null) {
            throw new IllegalArgumentException("Non-null column list, records and object class are required");
        }
        this.config = config;
        this.columns = columns;
        this.records = records;
        this.objClass = objClass;
        this.template = template;
    }

    @Override
    public boolean hasNext() {
        return records.hasNext();
    }

    @Override
    public T next() {
        if (mapper == null) {
            mapper = ClickHouseRecordMapper.of(config, columns, objClass);
        }
        return mapper.mapTo(records.next(), objClass, template);
    }
}
