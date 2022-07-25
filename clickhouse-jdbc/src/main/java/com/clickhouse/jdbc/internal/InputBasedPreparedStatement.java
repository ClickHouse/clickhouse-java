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
import java.util.List;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;
import com.clickhouse.client.data.ClickHousePipedStream;
import com.clickhouse.client.data.ClickHouseRowBinaryProcessor;
import com.clickhouse.client.data.ClickHouseRowBinaryProcessor.MappedFunctions;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;
import com.clickhouse.jdbc.ClickHousePreparedStatement;
import com.clickhouse.jdbc.SqlExceptionUtils;

public class InputBasedPreparedStatement extends AbstractPreparedStatement implements ClickHousePreparedStatement {
    private static final Logger log = LoggerFactory.getLogger(InputBasedPreparedStatement.class);

    private final Calendar defaultCalendar;
    private final ZoneId timeZoneForDate;
    private final ZoneId timeZoneForTs;

    private final List<ClickHouseColumn> columns;
    private final ClickHouseValue[] values;
    private final boolean[] flags;

    private int counter;
    private ClickHousePipedStream stream;

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

        this.columns = columns;
        int size = columns.size();
        int i = 0;
        values = new ClickHouseValue[size];
        for (ClickHouseColumn col : columns) {
            values[i++] = ClickHouseValues.newValue(config, col);
        }
        flags = new boolean[size];

        counter = 0;
        // it's important to make sure the queue has unlimited length
        stream = new ClickHousePipedStream(config.getMaxBufferSize(), 0, config.getSocketTimeout());
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
                throw SqlExceptionUtils.emptyBatchError();
            }
            continueOnError = getConnection().getJdbcConfig().isContinueBatchOnError();
        } else {
            if (counter != 0) {
                throw SqlExceptionUtils.undeterminedExecutionError();
            }
            addBatch();
        }

        long[] results = new long[counter];
        long rows = 0;
        try {
            stream.close();
            rows = executeInsert(getRequest().getStatements(false).get(0), stream.getInput());
            if (asBatch && getResultSet() != null) {
                throw SqlExceptionUtils.queryInBatchError(results);
            }
            // FIXME grpc and tcp by default can provides accurate result
            Arrays.fill(results, 1);
        } catch (Exception e) {
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
        // log.warn("Input function can be only used for insertion not query");
        if (executeAny(false)[0] == EXECUTE_FAILED) {
            throw new SQLException("Query failed", SqlExceptionUtils.SQL_STATE_SQL_ERROR);
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

        if (executeAny(false)[0] == EXECUTE_FAILED) {
            throw new SQLException("Update failed", SqlExceptionUtils.SQL_STATE_SQL_ERROR);
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

        if (executeAny(false)[0] == EXECUTE_FAILED) {
            throw new SQLException("Execution failed", SqlExceptionUtils.SQL_STATE_SQL_ERROR);
        }
        return false;
    }

    @Override
    public void addBatch() throws SQLException {
        ensureOpen();

        ClickHouseConfig config = getConfig();
        MappedFunctions functions = ClickHouseRowBinaryProcessor.getMappedFunctions();
        for (int i = 0, len = values.length; i < len; i++) {
            if (!flags[i]) {
                throw SqlExceptionUtils.clientError(ClickHouseUtils.format("Missing value for parameter #%d for column name %s", i + 1, columns.get(i)));
            }
            try {
                functions.serialize(values[i], config, columns.get(i), stream);
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

        counter = 0;
        ClickHouseConfig config = getConfig();
        stream = new ClickHousePipedStream(config.getMaxBufferSize(), 0, config.getSocketTimeout());
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
                dt = c.toInstant().atZone(tz).withZoneSameInstant(timeZoneForTs).toLocalDateTime();
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        values[idx].update(x);
        flags[idx] = true;
    }
}
