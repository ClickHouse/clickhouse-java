package com.clickhouse.data;

import java.util.Iterator;
import java.util.List;

import com.clickhouse.data.mapper.IterableRecordWrapper;
import com.clickhouse.data.mapper.RecordMapperFactory;

/**
 * Functional interface for mapping {@link ClickHouseRecord} to customized
 * object.
 */
@Deprecated
@FunctionalInterface
public interface ClickHouseRecordMapper {
    /**
     * Wraps iterable records as mapped objects.
     *
     * @param <T>      type of mapped object
     * @param config   non-null configuration
     * @param columns  non-null list of columns
     * @param records  non-null iterable records
     * @param objClass non-null class of mapped object
     * @param template optional template object to reuse
     * @return non-null iterable objects
     */
    static <T> Iterator<T> wrap(ClickHouseDataConfig config, List<ClickHouseColumn> columns,
            Iterator<ClickHouseRecord> records, Class<T> objClass, T template) {
        return new IterableRecordWrapper<>(config, columns, records, objClass, template);
    }

    /**
     * Gets mapper to turn {@link ClickHouseRecord} into user-defined object.
     *
     * @param config   non-null configuration
     * @param columns  non-null list of columns
     * @param objClass non-null class of the user-defined object
     * @return non-null user-defined object
     */
    static ClickHouseRecordMapper of(ClickHouseDataConfig config, List<ClickHouseColumn> columns, Class<?> objClass) {
        return RecordMapperFactory.of(config, columns, objClass);
    }

    /**
     * Maps a record to a user-defined object. By default, it's same as
     * {@code mapTo(r, objClass, null)}.
     *
     * @param <T>      type of the user-defined object
     * @param r        non-null record
     * @param objClass non-null class of the user-defined object
     * @return non-null mapped object
     */
    default <T> T mapTo(ClickHouseRecord r, Class<T> objClass) {
        return mapTo(r, objClass, null);
    }

    /**
     * Maps a record to a user-defined object.
     *
     * @param <T>      type of the user-defined object
     * @param r        non-null record
     * @param objClass non-null class of the user-defined object
     * @param obj      reusable object instance to set values
     * @return non-null user-defined object, either new instance of
     *         {@code objClass}, or same as the given {@code obj} when it's not null
     */
    <T> T mapTo(ClickHouseRecord r, Class<T> objClass, T obj);
}