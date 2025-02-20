package com.clickhouse.data;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This defines a record returned from ClickHouse server. Usually it's a row but
 * sometimes it could a (nested) column, a (semi-)structured object, or even the
 * whole data set.
 */
@Deprecated
public interface ClickHouseRecord extends Iterable<ClickHouseValue>, Serializable {
    /**
     * Empty record.
     */
    static final ClickHouseRecord EMPTY = new ClickHouseRecord() {
        @Override
        public ClickHouseRecord copy() {
            return this;
        }

        @Override
        public ClickHouseValue getValue(int index) {
            throw new ArrayIndexOutOfBoundsException();
        }

        @Override
        public ClickHouseValue getValue(String name) {
            throw new ArrayIndexOutOfBoundsException();
        }

        @Override
        public int size() {
            return 0;
        }
    };

    /**
     * Creates a new record by copying values from current one.
     *
     * @return a new record
     */
    ClickHouseRecord copy();

    /**
     * Gets deserialized value wrapped in an object using column index. Please avoid
     * to cache the wrapper object, as it's reused among records for memory
     * efficiency when {@link ClickHouseDataConfig#isReuseValueWrapper()} returns
     * {@code true}, which is the default value. So instead of
     * {@code map.put("my_value", record.getValue(0))}, try something like
     * {@code map.put("my_value", record.getValue(0).asString())}.
     * 
     * @param index zero-based index of the column
     * @return non-null wrapped value
     */
    ClickHouseValue getValue(int index);

    /**
     * Gets deserialized value wrapped in an object using case-insensitive column
     * name, which usually is slower than {@link #getValue(int)}. Please avoid to
     * cache the wrapper object, as it's reused among records for memory efficiency
     * when {@link ClickHouseDataConfig#isReuseValueWrapper()} returns {@code true},
     * which is the default value. So instead of
     * {@code map.put("my_value", record.getValue("my_column"))}, try something like
     * {@code map.put("my_value", record.getValue("my_column").asString())}.
     * 
     * @param name case-insensitive name of the column
     * @return non-null wrapped value
     */
    ClickHouseValue getValue(String name);

    @Override
    default Iterator<ClickHouseValue> iterator() {
        return new Iterator<ClickHouseValue>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < size();
            }

            @Override
            public ClickHouseValue next() {
                try {
                    return getValue(index++);
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new NoSuchElementException(e.getMessage());
                }
            }
        };
    }

    /**
     * Gets size of the record.
     *
     * @return size of the record
     */
    int size();
}
