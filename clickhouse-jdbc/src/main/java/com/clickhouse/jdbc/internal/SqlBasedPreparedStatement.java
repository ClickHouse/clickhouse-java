package com.clickhouse.jdbc.internal;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
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
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseParameterizedQuery;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.data.value.ClickHouseDateTimeValue;
import com.clickhouse.data.value.ClickHouseDateValue;
import com.clickhouse.data.value.ClickHouseStringValue;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHousePreparedStatement;
import com.clickhouse.jdbc.ClickHouseResultSetMetaData;
import com.clickhouse.jdbc.JdbcParameterizedQuery;
import com.clickhouse.jdbc.SqlExceptionUtils;
import com.clickhouse.jdbc.parser.ClickHouseSqlStatement;

@Deprecated
public class SqlBasedPreparedStatement extends AbstractPreparedStatement implements ClickHousePreparedStatement {
    private static final Logger log = LoggerFactory.getLogger(SqlBasedPreparedStatement.class);

    private final Calendar defaultCalendar;
    private final TimeZone preferredTimeZone;
    private final ZoneId timeZoneForDate;
    private final ZoneId timeZoneForTs;

    private final ClickHouseSqlStatement parsedStmt;
    private final String insertValuesQuery;
    private final ClickHouseParameterizedQuery preparedQuery;
    private final ClickHouseValue[] templates;
    private final String[] values;
    private final ClickHouseParameterMetaData paramMetaData;
    private final List<String[]> batch;
    private final StringBuilder builder;

    private int counter;

    protected SqlBasedPreparedStatement(ClickHouseConnectionImpl connection, ClickHouseRequest<?> request,
            ClickHouseSqlStatement parsedStmt, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        super(connection, request, resultSetType, resultSetConcurrency, resultSetHoldability);

        ClickHouseConfig config = getConfig();
        defaultCalendar = connection.getDefaultCalendar();
        preferredTimeZone = config.getUseTimeZone();
        timeZoneForTs = preferredTimeZone.toZoneId();
        timeZoneForDate = config.isUseServerTimeZoneForDates() ? timeZoneForTs : null;

        this.parsedStmt = parsedStmt;
        String valuesExpr = null;
        ClickHouseParameterizedQuery parsedValuesExpr = null;
        String prefix = null;
        if (parsedStmt.hasValues()) { // consolidate multiple inserts into one
            valuesExpr = parsedStmt.getContentBetweenKeywords(ClickHouseSqlStatement.KEYWORD_VALUES_START,
                    ClickHouseSqlStatement.KEYWORD_VALUES_END);
            if (ClickHouseChecker.isNullOrBlank(valuesExpr)) {
                log.warn(
                        "Please consider to use one and only one values expression, for example: use 'values(?)' instead of 'values(?),(?)'.");
            } else {
                valuesExpr += ")";
                prefix = parsedStmt.getSQL().substring(0,
                        parsedStmt.getPositions().get(ClickHouseSqlStatement.KEYWORD_VALUES_START));
                if (connection.getJdbcConfig().useNamedParameter()) {
                    parsedValuesExpr = ClickHouseParameterizedQuery.of(config, valuesExpr);
                } else {
                    parsedValuesExpr = JdbcParameterizedQuery.of(config, valuesExpr);
                }
            }
        }

        preparedQuery = parsedValuesExpr == null ? request.getPreparedQuery() : parsedValuesExpr;

        templates = preparedQuery.getParameterTemplates();

        int tlen = templates.length;
        values = new String[tlen];
        List<ClickHouseColumn> list = new ArrayList<>(tlen);
        for (int i = 1; i <= tlen; i++) {
            list.add(ClickHouseColumn.of("parameter" + i, ClickHouseDataType.JSON, true));
        }
        paramMetaData = new ClickHouseParameterMetaData(Collections.unmodifiableList(list), mapper,
                connection.getTypeMap());
        batch = new LinkedList<>();
        builder = new StringBuilder();
        if ((insertValuesQuery = prefix) != null) {
            builder.append(insertValuesQuery);
        }

        counter = 0;
    }

    protected void ensureParams() throws SQLException {
        List<String> columns = new ArrayList<>();
        for (int i = 0, len = values.length; i < len; i++) {
            if (values[i] == null) {
                columns.add(String.valueOf(i + 1));
            }
        }

        if (!columns.isEmpty()) {
            throw SqlExceptionUtils.clientError(ClickHouseUtils.format("Missing parameter(s): %s", columns));
        }
    }

    private ClickHouseResponse executeStatement(String sql, boolean reparse) throws SQLException {
        ClickHouseResponse r = null;
        if (reparse) {
            // parse the query for the third time...
            ClickHouseSqlStatement[] stmts = getConnection().parse(sql, getConfig(), null);
            if (stmts.length == 1 && stmts[0].hasFile()) {
                r = executeStatement(stmts[0], null, null, null);
            }
        }
        return r != null ? r : executeStatement(builder.toString(), null, null, null);
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
            if (counter != 0) {
                throw SqlExceptionUtils.undeterminedExecutionError();
            }
            addBatch();
        }

        long[] results = new long[counter];
        ClickHouseResponse r = null;
        boolean reparse = getConnection().getJdbcConfig().useLocalFile() && this.parsedStmt.hasFile();
        if (builder.length() > 0) { // insert ... values
            long rows = 0L;
            try {
                r = executeStatement(builder.toString(), reparse);
                if (updateResult(parsedStmt, r) != null && asBatch && parsedStmt.isQuery()) {
                    throw SqlExceptionUtils.queryInBatchError(results);
                }
                rows = r.getSummary().getWrittenRows();
                // no effective rows for update and delete, and the number for insertion is not
                // accurate as well
                // if (rows > 0L && rows != counter) {
                // log.warn("Expect %d rows being inserted but only got %d", counter, rows);
                // }
                // FIXME needs to enhance http client before getting back to this
                Arrays.fill(results, 1);
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
                log.error("Failed to execute batch insertion of %d records", counter, e);
            } finally {
                if (asBatch && r != null) {
                    r.close();
                }
                clearBatch();
            }
        } else {
            int index = 0;
            try {
                for (String[] params : batch) {
                    builder.setLength(0);
                    preparedQuery.apply(builder, params);
                    try {
                        r = executeStatement(builder.toString(), reparse);
                        if (updateResult(parsedStmt, r) != null && asBatch && parsedStmt.isQuery()) {
                            throw SqlExceptionUtils.queryInBatchError(results);
                        }
                        int count = getUpdateCount();
                        results[index] = count > 0 ? count : 0;
                    } catch (Exception e) {
                        results[index] = EXECUTE_FAILED;
                        if (!continueOnError) {
                            throw SqlExceptionUtils.batchUpdateError(e, results);
                        }
                        log.error("Failed to execute batch insert at %d of %d", index + 1, counter, e);
                    } finally {
                        index++;
                        if (asBatch && r != null) {
                            r.close();
                        }
                    }
                }
            } finally {
                clearBatch();
            }
        }

        return results;
    }

    @Override
    protected int getMaxParameterIndex() {
        return templates.length;
    }

    @Override
    public ResultSetMetaData describeQueryResult() throws SQLException {
        // No metadata unless query has been recognized as SELECT
        if (!parsedStmt.isRecognized() || !parsedStmt.isQuery()) {
            return null;
        }

        final String[] vals;
        if (batch.isEmpty()) {
            vals = new String[values.length];
            System.arraycopy(this.values, 0, vals, 0, values.length);
        } else {
            vals = batch.get(0);
        }
        for (int i = 0; i < values.length; i++) {
            if (vals[i] == null) {
                vals[i] = ClickHouseValues.NULL_EXPR;
            }
        }

        StringBuilder sb = new StringBuilder("desc (");
        preparedQuery.apply(sb, vals);
        sb.append(')');

        List<ClickHouseColumn> columns = new LinkedList<>();
        ClickHouseConnection conn = getConnection();
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sb.toString())) {
            while (rs.next()) {
                columns.add(ClickHouseColumn.of(rs.getString(1), rs.getString(2)));
            }
        }

        return ClickHouseResultSetMetaData.of(conn.getJdbcConfig(), conn.getCurrentDatabase(), "",
            Collections.unmodifiableList(new ArrayList<>(columns)), mapper, conn.getTypeMap());
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
        return rs == null ? newEmptyResultSet() : rs;
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
        return getLargeUpdateCount();
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ClickHouseValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = String.valueOf(x);
        }
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ClickHouseValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = String.valueOf(x);
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ClickHouseValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = String.valueOf(x);
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ClickHouseValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = String.valueOf(x);
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ClickHouseValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = String.valueOf(x);
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ClickHouseValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = String.valueOf(x);
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ClickHouseValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = String.valueOf(x);
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ClickHouseValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = ClickHouseValues.convertToQuotedString(x);
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ClickHouseValue value = templates[idx];
        if (value == null) {
            templates[idx] = value = ClickHouseStringValue.ofNull();
        }
        values[idx] = value.update(x).toSqlExpression();
    }

    @Override
    public void clearParameters() throws SQLException {
        ensureOpen();

        for (int i = 0, len = values.length; i < len; i++) {
            values[i] = null;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ClickHouseValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            if (x instanceof ClickHouseValue) {
                value = (ClickHouseValue) x;
                templates[idx] = value;
                values[idx] = value.toSqlExpression();
            } else {
                values[idx] = ClickHouseValues.convertToSqlExpression(x);
            }
        }
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
        return getResultSet() != null;
    }

    @Override
    public void addBatch() throws SQLException {
        ensureOpen();

        if (builder.length() > 0) {
            int index = 1;
            for (String v : values) {
                if (v == null) {
                    throw SqlExceptionUtils
                            .clientError(ClickHouseUtils.format("Missing value for parameter #%d", index));
                }
                index++;
            }
            preparedQuery.apply(builder, values);
        } else {
            int len = values.length;
            String[] newValues = new String[len];
            for (int i = 0; i < len; i++) {
                String v = values[i];
                if (v == null) {
                    throw SqlExceptionUtils
                            .clientError(ClickHouseUtils.format("Missing value for parameter #%d", i + 1));
                } else {
                    newValues[i] = v;
                }
            }
            batch.add(newValues);
        }
        counter++;
        clearParameters();
    }

    @Override
    public void clearBatch() throws SQLException {
        ensureOpen();

        this.batch.clear();
        this.builder.setLength(0);
        if (insertValuesQuery != null) {
            this.builder.append(insertValuesQuery);
        }

        this.counter = 0;
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        Object array = x != null ? x.getArray() : x;
        values[idx] = array != null ? ClickHouseValues.convertToSqlExpression(array)
                : ClickHouseValues.EMPTY_ARRAY_EXPR;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        if (x == null) {
            values[idx] = ClickHouseValues.NULL_EXPR;
            return;
        }

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

        ClickHouseValue value = templates[idx];
        if (value == null) {
            value = ClickHouseDateValue.ofNull();
        }
        values[idx] = value.update(d).toSqlExpression();
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        if (x == null) {
            values[idx] = ClickHouseValues.NULL_EXPR;
            return;
        }

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

        ClickHouseValue value = templates[idx];
        if (value == null) {
            value = ClickHouseDateValue.ofNull();
        }
        values[idx] = value.update(t).toSqlExpression();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        if (x == null) {
            values[idx] = ClickHouseValues.NULL_EXPR;
            return;
        }

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
            dt = c.toInstant().atZone(tz).withNano(x.getNanos()).withZoneSameInstant(timeZoneForTs).toLocalDateTime();
        }

        ClickHouseValue value = templates[idx];
        if (value == null) {
            value = ClickHouseDateTimeValue.ofNull(dt.getNano() > 0 ? 9 : 0, preferredTimeZone);
        }
        values[idx] = value.update(dt).toSqlExpression();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ClickHouseValue value = templates[idx];
        if (value != null) {
            value.resetToNullOrEmpty();
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = ClickHouseValues.NULL_EXPR;
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return paramMetaData;
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ClickHouseValue value = templates[idx];
        if (value == null) {
            value = mapper.toColumn(targetSqlType, scaleOrLength).newValue(getConfig());
            templates[idx] = value;
        }

        value.update(x);
        values[idx] = value.toSqlExpression();
    }
}
