package com.clickhouse.data;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default implementation of {@link com.clickhouse.data.ClickHouseRecord},
 * which is simply a combination of list of columns and array of values.
 */
public class ClickHouseSimpleRecord implements ClickHouseRecord {
    public static final ClickHouseSimpleRecord EMPTY = new ClickHouseSimpleRecord(Collections.emptyList(),
            new ClickHouseValue[0]);

    private final List<ClickHouseColumn> columns;
    private ClickHouseValue[] values;
    private Map<String, Integer> columnsIndexes = null;

    /**
     * Creates a record object to wrap given values.
     *
     * @param columns non-null list of columns
     * @param values  non-null array of values
     * @return record
     */
    public static ClickHouseRecord of(List<ClickHouseColumn> columns, ClickHouseValue[] values) {
        if (columns == null || values == null) {
            throw new IllegalArgumentException("Non-null columns and values are required");
        } else if (columns.size() != values.length) {
            throw new IllegalArgumentException(ClickHouseUtils.format(
                    "Mismatched count: we have %d columns but we got %d values", columns.size(), values.length));
        } else if (values.length == 0) {
            return EMPTY;
        }

        return new ClickHouseSimpleRecord(columns, values);
    }

    protected ClickHouseSimpleRecord(List<ClickHouseColumn> columns, ClickHouseValue[] values) {
        this.columns = columns;
        this.values = values;
    }

    protected List<ClickHouseColumn> getColumns() {
        return columns;
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
        return new ClickHouseSimpleRecord(columns, vals);
    }

    @Override
    public ClickHouseValue getValue(int index) {
        return values[index];
    }

    @Override
    public ClickHouseValue getValue(String name) {
        if(columnsIndexes == null)
            columnsIndexes = new HashMap<>(columns.size());

        return getValue(columnsIndexes.computeIfAbsent(name, this::computeColumnIndex));
    }

    @Override
    public int size() {
        return values.length;
    }

    private int computeColumnIndex(String name) {
        int index = 0;
        for (ClickHouseColumn c : columns) {
            if (c.getColumnName().equalsIgnoreCase(name)) {
                return index;
            }
            index++;
        }
        throw new IllegalArgumentException(ClickHouseUtils.format("Unable to find column [%s]", name));
    }
}
