package com.clickhouse.jdbc.internal;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
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
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Function;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseParameterizedQuery;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.ClickHouseValues;
import com.clickhouse.client.data.ClickHouseDateTimeValue;
import com.clickhouse.client.data.ClickHouseDateValue;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;
import com.clickhouse.jdbc.ClickHousePreparedStatement;
import com.clickhouse.jdbc.JdbcParameterizedQuery;
import com.clickhouse.jdbc.JdbcTypeMapping;
import com.clickhouse.jdbc.SqlExceptionUtils;
import com.clickhouse.jdbc.parser.ClickHouseSqlStatement;

public class SqlBasedPreparedStatement extends ClickHouseStatementImpl implements ClickHousePreparedStatement {
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
    private final List<String[]> batch;

    protected SqlBasedPreparedStatement(ClickHouseConnectionImpl connection, ClickHouseRequest<?> request,
            ClickHouseSqlStatement parsedStmt, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        super(connection, request, resultSetType, resultSetConcurrency, resultSetHoldability);

        ClickHouseConfig config = getConfig();
        defaultCalendar = connection.getDefaultCalendar();
        preferredTimeZone = config.getUseTimeZone();
        timeZoneForDate = (config.isUseServerTimeZoneForDate() ? connection.getServerTimeZone()
                : config.getTimeZoneForDate()).toZoneId();
        timeZoneForTs = preferredTimeZone.toZoneId();

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

        insertValuesQuery = prefix;
        preparedQuery = parsedValuesExpr == null ? request.getPreparedQuery() : parsedValuesExpr;

        templates = preparedQuery.getParameterTemplates();

        values = new String[templates.length];
        batch = new LinkedList<>();
    }

    protected String buildInsertValuesSql(String[] params) {
        return new StringBuilder().append(insertValuesQuery).append(preparedQuery.apply(params)).toString();
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

    protected int toArrayIndex(int parameterIndex) throws SQLException {
        if (parameterIndex < 1 || parameterIndex > templates.length) {
            throw SqlExceptionUtils.clientError(ClickHouseUtils
                    .format("Parameter index must between 1 and %d but we got %d", templates.length, parameterIndex));
        }

        return parameterIndex - 1;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        ensureParams();

        // FIXME ResultSet should never be null
        return executeQuery(preparedQuery.apply(values));
    }

    @Override
    public int executeUpdate() throws SQLException {
        ensureParams();

        return executeUpdate(preparedQuery.apply(values));
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
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = new String(x, StandardCharsets.UTF_8);
        }
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

        return execute(preparedQuery.apply(values));
    }

    @Override
    public void addBatch() throws SQLException {
        ensureOpen();

        int len = values.length;
        String[] newValues = new String[len];
        for (int i = 0; i < len; i++) {
            String v = values[i];
            if (v == null) {
                throw SqlExceptionUtils.clientError(ClickHouseUtils.format("Missing value for parameter #%d", i + 1));
            } else {
                newValues[i] = v;
            }
        }
        batch.add(newValues);
        clearParameters();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        ensureOpen();

        int len = batch.size();
        int[] results = new int[len];
        if (insertValuesQuery != null) {
            StringBuilder builder = new StringBuilder().append(insertValuesQuery);
            for (String[] params : batch) {
                builder.append(preparedQuery.apply(params));
            }
            try (ClickHouseResponse r = executeStatement(builder.toString(), null, null, null)) {
                // nothing to read
            } catch (Exception e) {
                // actually we don't know which ones failed
                for (int i = 0; i < len; i++) {
                    results[i] = EXECUTE_FAILED;
                }
                log.error("Failed to execute batch insertion of %d records", len, e);
            }
        } else {
            int counter = 0;
            for (String[] params : batch) {
                try (ClickHouseResponse r = executeStatement(preparedQuery.apply(params), null, null, null)) {
                    results[counter] = (int) r.getSummary().getWrittenRows();
                } catch (Exception e) {
                    results[counter] = EXECUTE_FAILED;
                    log.error("Failed to execute task %d of %d", counter + 1, len, e);
                }
                counter++;
            }
        }

        clearBatch();

        return results;
    }

    @Override
    public void clearBatch() throws SQLException {
        ensureOpen();

        this.batch.clear();
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
        if (tz.equals(timeZoneForDate)) {
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
            dt = c.toInstant().atZone(tz).withZoneSameInstant(timeZoneForTs).toLocalDateTime();
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ClickHouseValue value = templates[idx];
        if (value == null) {
            value = ClickHouseValues.newValue(getConfig(), JdbcTypeMapping.fromJdbcType(targetSqlType, scaleOrLength));
            templates[idx] = value;
        }

        value.update(x);
        values[idx] = value.toSqlExpression();
    }
}
