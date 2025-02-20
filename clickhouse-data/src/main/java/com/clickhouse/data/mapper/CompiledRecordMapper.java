package com.clickhouse.data.mapper;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseRecordMapper;
import com.clickhouse.data.ClickHouseValue;

@Deprecated
public class CompiledRecordMapper extends AbstractRecordMapper {
    private final Constructor<?> constructor;
    private final PropertyInfo[] properties;

    private CompiledRecordMapper(Class<?> objClass, Constructor<?> constructor, List<ClickHouseColumn> columns) {
        super(objClass);
        this.constructor = constructor;

        if (columns == null || columns.isEmpty()) {
            this.properties = new PropertyInfo[0];
        } else {
            this.properties = getProperties(objClass, columns);
        }
    }

    protected CompiledRecordMapper(Class<?> objClass) {
        this(objClass, getDefaultConstructor(objClass), Collections.emptyList());
    }

    @Override
    public ClickHouseRecordMapper get(ClickHouseDataConfig config, List<ClickHouseColumn> columns) {
        if (columns == null) {
            columns = Collections.emptyList();
        }
        // TODO use asm to generate mapper class
        // ClickHouseRecordMapper mapper = null;
        // if (config != null && config.isUseCompilation()) {
        // mapper = new CompiledRecordMapper(clazz, constructor, columns);
        // }

        return new DynamicRecordMapper(clazz, columns, constructor);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T mapTo(ClickHouseRecord r, Class<T> objClass, T obj) {
        check(r, objClass);

        if (obj == null) {
            obj = (T) newInstance(constructor);
        }

        for (int i = 0, len = properties.length; i < len; i++) {
            PropertyInfo p = properties[i];
            ClickHouseValue v = r.getValue(p.index);
        }
        return obj;
    }
}