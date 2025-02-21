package com.clickhouse.data;

import java.util.Collections;
import java.util.Map;

/**
 * Default implementation of {@link com.clickhouse.data.ClickHouseRecord},
 * which is simply a combination of list of columns and array of values.
 */
@Deprecated
public class ClickHouseSimpleRecord implements ClickHouseRecord {
    public static final ClickHouseSimpleRecord EMPTY = new ClickHouseSimpleRecord(Collections.emptyMap(),
            new ClickHouseValue[0]);

    private final Map<String, Integer> columnsIndex;
    private ClickHouseValue[] values;

    /**
     * Creates a record object to wrap given values.
     *
     * @param columnsIndex index of columns ord numbers
     * @param values  non-null array of values
     * @return record
     */
    public static ClickHouseRecord of(Map<String, Integer> columnsIndex, ClickHouseValue[] values) {
        if (columnsIndex == null || values == null) {
            throw new IllegalArgumentException("Non-null columns and values are required");
        } else if (columnsIndex.size() != values.length) {
            throw new IllegalArgumentException(ClickHouseUtils.format(
                    "Mismatched count: we have %d columns but we got %d values", columnsIndex.size(), values.length));
        } else if (values.length == 0) {
            return EMPTY;
        }

        return new ClickHouseSimpleRecord(columnsIndex, values);
    }

    protected ClickHouseSimpleRecord(Map<String, Integer> columnsIndex, ClickHouseValue[] values) {
        this.columnsIndex = columnsIndex;
        this.values = values;
    }

    protected ClickHouseValue[] getValues() {
        return values;
    }

    protected void update(ClickHouseValue[] values) {
        this.values = values;
    }

    protected void update(Object[] values) {
        int len = values != null ? values.length : 0;
        for (int i = 0, size = this.values.length; i < size; i++) {
            if (i < len) {
                this.values[i].update(values[i]); // NOSONAR
            } else {
                this.values[i].resetToNullOrEmpty();
            }
        }
    }

    @Override
    public ClickHouseRecord copy() {
        if (this == EMPTY) {
            return EMPTY;
        }

        int len = values.length;
        ClickHouseValue[] vals = new ClickHouseValue[len];
        for (int i = 0; i < len; i++) {
            vals[i] = values[i].copy();
        }
        return new ClickHouseSimpleRecord(columnsIndex, vals);
    }

    @Override
    public ClickHouseValue getValue(int index) {
        return values[index];
    }

    @Override
    public ClickHouseValue getValue(String name) {
        Integer index = columnsIndex.get(name);
        if (index == null) {
            throw new IllegalArgumentException(ClickHouseUtils.format("Unknown column name: %s", name));
        }
        return getValue(index);
    }

    @Override
    public int size() {
        return values.length;
    }
}
