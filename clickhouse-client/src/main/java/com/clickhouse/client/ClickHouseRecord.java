package com.clickhouse.client;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/**
 * This defines a record returned from ClickHouse server. Usually it's a row but
 * sometimes it could a (nested) column, a (semi-)structured object, or even the
 * whole data set.
 */
public interface ClickHouseRecord extends Iterable<ClickHouseValue>, Serializable {
    /**
     * Gets size of the record.
     *
     * @return size of the record
     */
    int size();

    /**
     * Gets deserialized value wrapped in an object using column index. Please avoid
     * to cache the wrapper object, as it's reused among records for memory
     * efficiency when {@link ClickHouseConfig#isReuseValueWrapper()} returns
     * {@code true}, which is the default value. So instead of
     * {@code map.put("my_value", record.getValue(0))}, try something like
     * {@code map.put("my_value", record.getValue(0).asString())}.
     * 
     * @param index index of the column
     * @return non-null wrapped value
     * @throws IOException when failed get data from the input stream
     */
    ClickHouseValue getValue(int index) throws IOException;

    /**
     * Gets deserialized value wrapped in an object using column name, which usually
     * is slower than {@link #getValue(int)}. Please avoid to cache the wrapper
     * object, as it's reused among records for memory efficiency when
     * {@link ClickHouseConfig#isReuseValueWrapper()} returns {@code true}, which is
     * the default value. So instead of
     * {@code map.put("my_value", record.getValue("my_column"))}, try something like
     * {@code map.put("my_value", record.getValue("my_column").asString())}.
     * 
     * @param name name of the column
     * @return non-null wrapped value
     * @throws IOException when failed get data from the input stream
     */
    ClickHouseValue getValue(String name) throws IOException;

    @Override
    default Iterator<ClickHouseValue> iterator() {
        return new Iterator<ClickHouseValue>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index < size();
            }

            @Override
            public ClickHouseValue next() {
                try {
                    return getValue(index++);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        };
    }
}
