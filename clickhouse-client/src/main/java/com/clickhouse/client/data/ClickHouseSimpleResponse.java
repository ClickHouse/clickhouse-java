package com.clickhouse.client.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;

/**
 * A simple response built on top of two lists: columns and records.
 */
public class ClickHouseSimpleResponse implements ClickHouseResponse {
    /**
     * Empty response.
     *
     * @deprecated will be removed in v0.3.3, please use
     *             {@link ClickHouseResponse#EMPTY} instead
     */
    @Deprecated
    public static final ClickHouseSimpleResponse EMPTY = new ClickHouseSimpleResponse(Collections.emptyList(),
            new ClickHouseValue[0][], ClickHouseResponseSummary.EMPTY);

    /**
     * Creates a response object using columns definition and raw values.
     *
     * @param config  non-null config
     * @param columns list of columns
     * @param values  raw values, which may or may not be null
     * @return response object
     */
    public static ClickHouseResponse of(ClickHouseConfig config, List<ClickHouseColumn> columns, Object[][] values) {
        return of(config, columns, values, null);
    }

    /**
     * Creates a response object using columns definition and raw values.
     *
     * @param config  non-null config
     * @param columns list of columns
     * @param values  raw values, which may or may not be null
     * @param summary response summary
     * @return response object
     */
    public static ClickHouseResponse of(ClickHouseConfig config, List<ClickHouseColumn> columns, Object[][] values,
            ClickHouseResponseSummary summary) {
        if (columns == null || columns.isEmpty()) {
            return EMPTY;
        }

        int size = columns.size();
        int len = values != null ? values.length : 0;

        ClickHouseValue[][] wrappedValues = new ClickHouseValue[len][];
        if (len > 0) {
            ClickHouseValue[] templates = new ClickHouseValue[size];
            for (int i = 0; i < size; i++) {
                templates[i] = ClickHouseValues.newValue(config, columns.get(i));
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

        return new ClickHouseSimpleResponse(columns, wrappedValues, summary);
    }

    /**
     * Creates a response object by copying columns and values from the given one.
     * Same as {@code of(response, null)}.
     *
     * @param response response to copy
     * @return new response object
     */
    public static ClickHouseResponse of(ClickHouseResponse response) {
        return of(response, null);
    }

    /**
     * Creates a response object by copying columns and values from the given one.
     * You should never use this method against a large response, because it will
     * load everything into memory. Worse than that, when {@code func} is not null,
     * it will be applied to every single row, which is going to be slow when
     * original response contains many records.
     *
     * @param response response to copy
     * @param func     optinal function to update value by column index
     * @return new response object
     */
    public static ClickHouseResponse of(ClickHouseResponse response, ClickHouseRecordTransformer func) {
        if (response == null) {
            return EMPTY;
        } else if (response instanceof ClickHouseSimpleResponse) {
            return response;
        }

        List<ClickHouseColumn> columns = response.getColumns();
        int size = columns.size();
        List<ClickHouseRecord> records = new LinkedList<>();
        int rowIndex = 0;
        for (ClickHouseRecord r : response.records()) {
            ClickHouseValue[] values = new ClickHouseValue[size];
            for (int i = 0; i < size; i++) {
                values[i] = r.getValue(i).copy();
            }

            ClickHouseRecord rec = ClickHouseSimpleRecord.of(columns, values);
            if (func != null) {
                func.update(rowIndex, rec);
            }
            records.add(rec);
        }

        return new ClickHouseSimpleResponse(response.getColumns(), records, response.getSummary());
    }

    private final List<ClickHouseColumn> columns;
    // better to use simple ClickHouseRecord as template along with raw values
    private final List<ClickHouseRecord> records;
    private final ClickHouseResponseSummary summary;

    private boolean isClosed;

    protected ClickHouseSimpleResponse(List<ClickHouseColumn> columns, List<ClickHouseRecord> records,
            ClickHouseResponseSummary summary) {
        this.columns = columns;
        this.records = Collections.unmodifiableList(records);
        this.summary = summary != null ? summary : ClickHouseResponseSummary.EMPTY;
    }

    protected ClickHouseSimpleResponse(List<ClickHouseColumn> columns, ClickHouseValue[][] values,
            ClickHouseResponseSummary summary) {
        this.columns = columns;

        int len = values.length;
        List<ClickHouseRecord> list = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            list.add(new ClickHouseSimpleRecord(columns, values[i]));
        }

        this.records = Collections.unmodifiableList(list);

        this.summary = summary != null ? summary : ClickHouseResponseSummary.EMPTY;
    }

    @Override
    public List<ClickHouseColumn> getColumns() {
        return columns;
    }

    @Override
    public ClickHouseResponseSummary getSummary() {
        return summary;
    }

    @Override
    public ClickHouseInputStream getInputStream() {
        throw new UnsupportedOperationException("An in-memory response does not have input stream");
    }

    @Override
    public Iterable<ClickHouseRecord> records() {
        return records;
    }

    @Override
    public void close() {
        // nothing to close
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}
