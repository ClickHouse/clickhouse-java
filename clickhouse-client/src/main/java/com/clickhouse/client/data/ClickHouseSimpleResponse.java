package com.clickhouse.client.data;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;

/**
 * A simple response built on top of two lists: columns and values.
 */
public class ClickHouseSimpleResponse implements ClickHouseResponse {
    private final List<ClickHouseColumn> columns;
    private final List<ClickHouseRecord> records;

    /**
     * Creates a response object using columns definition and raw values.
     *
     * @param structure column definition
     * @param values    non-null raw values
     * @return response object
     */
    public static ClickHouseResponse of(String structure, Object[][] values) {
        List<ClickHouseColumn> columns = ClickHouseColumn.parse(structure);
        int size = columns.size();
        int len = values != null ? values.length : 0;

        ClickHouseValue[][] wrappedValues = new ClickHouseValue[len][];
        if (len > 0) {
            ClickHouseValue[] templates = new ClickHouseValue[size];
            for (int i = 0; i < size; i++) {
                templates[i] = ClickHouseValues.newValue(columns.get(i));
            }

            for (int i = 0; i < len; i++) {
                Object[] input = values[i];
                int count = input != null ? input.length : 0;
                ClickHouseValue[] v = new ClickHouseValue[size];
                for (int j = 0; j < size; j++) {
                    v[j] = templates[j].copy().update(j < count ? input[j] : null);
                }
                wrappedValues[i] = v;
            }
        }

        return new ClickHouseSimpleResponse(columns, wrappedValues);
    }

    protected ClickHouseSimpleResponse(List<ClickHouseColumn> columns, ClickHouseValue[][] values) {
        this.columns = columns;

        int len = values.length;
        List<ClickHouseRecord> list = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            list.add(new ClickHouseSimpleRecord(columns, values[i]));
        }
        this.records = Collections.unmodifiableList(list);
    }

    @Override
    public List<ClickHouseColumn> getColumns() {
        return columns;
    }

    @Override
    public ClickHouseResponseSummary getSummary() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InputStream getInputStream() {
        throw new UnsupportedOperationException("An in-memory response does not have input stream");
    }

    @Override
    public Iterable<ClickHouseRecord> records() {
        return records;
    }

    @Override
    public void close() {
        // nothing to close
    }
}
