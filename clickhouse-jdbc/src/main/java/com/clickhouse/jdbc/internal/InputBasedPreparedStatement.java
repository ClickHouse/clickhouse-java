package com.clickhouse.jdbc.internal;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
import com.clickhouse.jdbc.ClickHousePreparedStatement;
import com.clickhouse.jdbc.SqlExceptionUtils;

@Deprecated
public class InputBasedPreparedStatement extends AbstractPreparedStatement implements ClickHousePreparedStatement {
    private static final Logger log = LoggerFactory.getLogger(InputBasedPreparedStatement.class);

    private final Calendar defaultCalendar;
    private final ZoneId timeZoneForDate;
    private final ZoneId timeZoneForTs;

    private final ClickHouseColumn[] columns;
    private final ClickHouseValue[] values;
    private final ClickHouseParameterMetaData paramMetaData;
    private final boolean[] flags;

    private int counter;
    private ClickHousePipedOutputStream stream;

    protected InputBasedPreparedStatement(ClickHouseConnectionImpl connection, ClickHouseRequest<?> request,
            List<ClickHouseColumn> columns, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        super(connection, request, resultSetType, resultSetConcurrency, resultSetHoldability);

        if (columns == null) {
            throw SqlExceptionUtils.clientError("Non-null column list is required");
        }

        ClickHouseConfig config = getConfig();
        defaultCalendar = connection.getDefaultCalendar();
        timeZoneForTs = config.getUseTimeZone().toZoneId();
        timeZoneForDate = config.isUseServerTimeZoneForDates() ? timeZoneForTs : null;

        int size = columns.size();
        this.columns = new ClickHouseColumn[size];
        this.values = new ClickHouseValue[size];
        List<ClickHouseColumn> list = new ArrayList<>(size);
        int i = 0;
        for (ClickHouseColumn col : columns) {
            this.columns[i] = col;
            this.values[i] = col.newValue(config);
            list.add(col);
            i++;
        }
        paramMetaData = new ClickHouseParameterMetaData(Collections.unmodifiableList(list), mapper,
                connection.getTypeMap());
        flags = new boolean[size];

        counter = 0;
        // it's important to make sure the queue has unlimited length
        stream = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(config.getWriteBufferSize(), 0,
                config.getSocketTimeout());
    }

    protected void ensureParams() throws SQLException {
        List<String> list = new ArrayList<>();
        for (int i = 0, len = values.length; i < len; i++) {
            if (!flags[i]) {
                list.add(String.valueOf(i + 1));
            }
        }

        if (!list.isEmpty()) {
            throw SqlExceptionUtils.clientError(ClickHouseUtils.format("Missing parameter(s): %s", list));
        }
    }

    @Override
    protected long[] executeAny(boolean asBatch) throws SQLException {
        ensureOpen();
        boolean continueOnError = false;
        if (asBatch) {
            if (counter < 1) {
                return ClickHouseValues.EMPTY_LONG_ARRAY;
            }
            continueOnError = getConnection().getJdbcConfig().isContinueBatchOnError();
        } else {
            try {
                if (counter != 0) {
                    throw SqlExceptionUtils.undeterminedExecutionError();
                }
                addBatch();
            } catch (SQLException e) {
                clearBatch();
                throw e;
            }
        }

        long[] results = new long[counter];
        long rows = 0;
        try {
            stream.close();
            rows = executeInsert(getRequest().getStatements(false).get(0), stream.getInputStream());
            if (asBatch && getResultSet() != null) {
                throw SqlExceptionUtils.queryInBatchError(results);
            }

            if (counter == 1) {
                results[0] = rows;
            } else {
                // FIXME grpc and tcp by default can provides accurate result
                Arrays.fill(results, 1);
            }
        } catch (Exception e) {
            if (!asBatch) {
                throw SqlExceptionUtils.handle(e);
            }

            // just a wild guess...
            if (rows < 1) {
                results[0] = EXECUTE_FAILED;
            } else {
                if (rows >= counter) {
                    rows = counter;
                }
                for (int i = 0, len = (int) rows - 1; i < len; i++) {
                    results[i] = 1;
                }
                results[(int) rows] = EXECUTE_FAILED;
            }

            if (!continueOnError) {
                throw SqlExceptionUtils.batchUpdateError(e, results);
            }
            log.error("Failed to execute batch insert of %d records", counter + 1, e);
        } finally {
            clearBatch();
        }

        return results;
    }

    @Override
    protected int getMaxParameterIndex() {
        return values.length;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        ensureParams();
        try {
            executeAny(false);
        } catch (SQLException e) {
            if (e.getSQLState() != null) {
                throw e;
            } else {
                throw new SQLException("Query failed", SqlExceptionUtils.SQL_STATE_SQL_ERROR, e.getCause());
            }
        }

        ResultSet rs = getResultSet();
        if (rs != null) { // should not happen
            try {
                rs.close();
            } catch (Exception e) {
                // ignore
            }
        }
        return newEmptyResultSet();
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        ensureParams();
        try {
            executeAny(false);
        } catch (SQLException e) {
            if (e.getSQLState() != null) {
                throw e;
            } else {
                throw new SQLException("Update failed", SqlExceptionUtils.SQL_STATE_SQL_ERROR, e.getCause());
            }
        }
        long row = getLargeUpdateCount();
        return row > 0L ? row : 0L;
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public void clearParameters() throws SQLException {
        ensureOpen();

        for (int i = 0, len = values.length; i < len; i++) {
            flags[i] = false;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }

    @Override
    public boolean execute() throws SQLException {
        ensureParams();
        try {
            executeAny(false);
        } catch (SQLException e) {
            if (e.getSQLState() != null) {
                throw e;
            } else {
                throw new SQLException("Execution failed", SqlExceptionUtils.SQL_STATE_SQL_ERROR, e.getCause());
            }
        }
        return false;
    }

    @Override
    public void addBatch() throws SQLException {
        ensureOpen();

        ClickHouseDataProcessor processor = getDataProcessor(stream, null, columns);
        int nullAsDefault = getNullAsDefault();
        // validate the values before the actual write into the processor
        for (int i = 0, len = values.length; i < len; i++) {
            if (!flags[i]) {
                throw SqlExceptionUtils
                        .clientError(ClickHouseUtils.format("Missing value for parameter #%d [%s]", i + 1, columns[i]));
            }
            ClickHouseColumn col = columns[i];
            ClickHouseValue val = values[i];
            if (!col.isNestedType() && !col.isNullable() && (val == null || val.isNullOrEmpty())) {
                if (nullAsDefault > 1 && val != null) {
                    val.resetToDefault();
                } else if (nullAsDefault < 1) {
                    throw SqlExceptionUtils.clientError(ClickHouseUtils.format(
                            "Cannot set null to non-nullable column #%d [%s]", i + 1, col));
                }
            }
        }
        // the actual write to the processor
        for (int i = 0, len = values.length; i < len; i++) {
            ClickHouseValue val = values[i];
            try {
                processor.write(val);
            } catch (IOException e) {
                // should not happen
                throw SqlExceptionUtils.handle(e);
            }
        }
        counter++;
        clearParameters();
    }

    @Override
    public void clearBatch() throws SQLException {
        ensureOpen();

        // just in case
        try {
            stream.close();
        } catch (Exception e) {
            // ignore
        }
        counter = 0;

        ClickHouseConfig config = getConfig();
        stream = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(config.getWriteBufferSize(), 0,
                config.getSocketTimeout());
        resetDataProcessor();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        Object array = x != null ? x.getArray() : x;
        values[idx].update(array);
        flags[idx] = true;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        if (x != null) {
            LocalDate d;
            if (cal == null) {
                cal = defaultCalendar;
            }
            ZoneId tz = cal.getTimeZone().toZoneId();
            if (timeZoneForDate == null || tz.equals(timeZoneForDate)) {
                d = x.toLocalDate();
            } else {
                Calendar c = (Calendar) cal.clone();
                c.setTime(x);
                d = c.toInstant().atZone(tz).withZoneSameInstant(timeZoneForDate).toLocalDate();
            }
            values[idx].update(d);
        } else {
            values[idx].resetToNullOrEmpty();
        }
        flags[idx] = true;
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        if (x != null) {
            LocalTime t;
            if (cal == null) {
                cal = defaultCalendar;
            }
            ZoneId tz = cal.getTimeZone().toZoneId();
            if (tz.equals(timeZoneForTs)) {
                t = x.toLocalTime();
            } else {
                Calendar c = (Calendar) cal.clone();
                c.setTime(x);
                t = c.toInstant().atZone(tz).withZoneSameInstant(timeZoneForTs).toLocalTime();
            }
            values[idx].update(t);
        } else {
            values[idx].resetToNullOrEmpty();
        }
        flags[idx] = true;
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        if (x != null) {
            LocalDateTime dt;
            if (cal == null) {
                cal = defaultCalendar;
            }
            ZoneId tz = cal.getTimeZone().toZoneId();
            if (tz.equals(timeZoneForTs)) {
                dt = x.toLocalDateTime();
            } else {
                Calendar c = (Calendar) cal.clone();
                c.setTime(x);
                dt = c.toInstant().atZone(tz).withNano(x.getNanos()).withZoneSameInstant(timeZoneForTs)
                        .toLocalDateTime();
            }
            values[idx].update(dt);
        } else {
            values[idx].resetToNullOrEmpty();
        }
        flags[idx] = true;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].resetToNullOrEmpty();
        flags[idx] = true;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return paramMetaData;
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }
}
