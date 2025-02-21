package com.clickhouse.client;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseRecordMapper;
import com.clickhouse.data.ClickHouseRecordTransformer;
import com.clickhouse.data.ClickHouseSimpleRecord;
import com.clickhouse.data.ClickHouseValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A simple response built on top of two lists: columns and records.
 */
@Deprecated
public class ClickHouseSimpleResponse implements ClickHouseResponse {
    private static final long serialVersionUID = 6883452584393840649L;

    private final TimeZone timeZone;
    /**
     * Creates a response object using columns definition and raw values.
     *
     * @param config  non-null config
     * @param columns list of columns
     * @param values  raw values, which may or may not be null
     * @return response object
     */
    public static ClickHouseResponse of(ClickHouseConfig config, List<ClickHouseColumn> columns, Object[][] values) {
        return of(config, columns, values, null, null);
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
            ClickHouseResponseSummary summary, TimeZone timeZone) {
        if (columns == null) {
            columns = Collections.emptyList();
        }
        if (columns.isEmpty() && (summary == null || summary == ClickHouseResponseSummary.EMPTY)) {
            return ClickHouseResponse.EMPTY;
        }

        int size = columns.size();
        int len = values != null ? values.length : 0;
        ClickHouseValue[][] wrappedValues = new ClickHouseValue[len][];
        if (len > 0) {
            ClickHouseValue[] templates = new ClickHouseValue[size];
            for (int i = 0; i < size; i++) {
                templates[i] = columns.get(i).newValue(config);
            }

            for (int i = 0; i < len; i++) {
                Object[] input = values[i];
                int count = input != null ? input.length : 0;
                ClickHouseValue[] v = new ClickHouseValue[size];
                for (int j = 0; j < size; j++) {
                    v[j] = templates[j].copy().update(j < count ? input[j] : null); // NOSONAR
                }
                wrappedValues[i] = v;
            }
        }

        return new ClickHouseSimpleResponse(columns, wrappedValues, summary, timeZone);
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
        Map<String, Integer> columnIndex = IntStream.range(0, columns.size()).boxed().
                collect(Collectors.toMap(i->columns.get(i).getColumnName() , i -> i));

        int size = columns.size();
        List<ClickHouseRecord> records = new LinkedList<>();
        int rowIndex = 0;
        for (ClickHouseRecord r : response.records()) {
            ClickHouseValue[] values = new ClickHouseValue[size];
            for (int i = 0; i < size; i++) {
                values[i] = r.getValue(i).copy();
            }

            ClickHouseRecord rec = ClickHouseSimpleRecord.of(columnIndex, values);
            if (func != null) {
                func.update(rowIndex, rec);
            }
            records.add(rec);
        }

        return new ClickHouseSimpleResponse(response.getColumns(), records, response.getSummary(), response.getTimeZone());
    }

    private final List<ClickHouseColumn> columns;
    // better to use simple ClickHouseRecord as template along with raw values
    private final List<ClickHouseRecord> records;
    private final ClickHouseResponseSummary summary;

    private volatile boolean closed;

    protected ClickHouseSimpleResponse(List<ClickHouseColumn> columns, List<ClickHouseRecord> records,
                                       ClickHouseResponseSummary summary, TimeZone timeZone) {
        this.columns = columns;
        this.records = Collections.unmodifiableList(records);
        this.summary = summary != null ? summary : ClickHouseResponseSummary.EMPTY;
        this.timeZone = timeZone;
    }

    protected ClickHouseSimpleResponse(List<ClickHouseColumn> columns, ClickHouseValue[][] values,
            ClickHouseResponseSummary summary, TimeZone timeZone) {
        this.columns = columns;
        Map<String, Integer> columnIndex = IntStream.range(0, columns.size()).boxed().
                collect(Collectors.toMap(i->columns.get(i).getColumnName() , i -> i));
        int len = values.length;
        List<ClickHouseRecord> list = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            list.add(ClickHouseSimpleRecord.of(columnIndex, values[i]));
        }

        this.records = Collections.unmodifiableList(list);

        this.summary = summary != null ? summary : ClickHouseResponseSummary.EMPTY;
        this.timeZone = timeZone;
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
    public TimeZone getTimeZone() {
        return timeZone;
    }

    @Override
    public Iterable<ClickHouseRecord> records() {
        return records;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Iterable<T> records(Class<T> objClass) {
        if (objClass == null || objClass == ClickHouseRecord.class) {
            return (Iterable<T>) records();
        }

        return () -> ClickHouseRecordMapper.wrap(null, columns, records().iterator(), objClass, null);
    }

    @Override
    public void close() {
        // nothing to close
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
