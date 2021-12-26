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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

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
import com.clickhouse.jdbc.JdbcTypeMapping;
import com.clickhouse.jdbc.SqlExceptionUtils;
import com.clickhouse.jdbc.parser.ClickHouseSqlStatement;

public class SqlBasedPreparedStatement extends ClickHouseStatementImpl implements ClickHousePreparedStatement {
    private static final Logger log = LoggerFactory.getLogger(SqlBasedPreparedStatement.class);

    private final Calendar defaultCalendar;
    private final ZoneId jvmZoneId;
    private final TimeZone serverTimeZone;

    private final ClickHouseSqlStatement parsedStmt;
    private final ClickHouseParameterizedQuery preparedQuery;
    private final ClickHouseValue[] templates;
    private final String[] values;
    private final List<String[]> batch;

    protected SqlBasedPreparedStatement(ClickHouseConnectionImpl connection, ClickHouseRequest<?> request,
            ClickHouseSqlStatement parsedStmt, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        super(connection, request, resultSetType, resultSetConcurrency, resultSetHoldability);

        defaultCalendar = connection.getDefaultCalendar();
        jvmZoneId = connection.getJvmTimeZone().toZoneId();
        serverTimeZone = connection.getServerTimeZone();

        this.parsedStmt = parsedStmt;

        preparedQuery = request.getPreparedQuery();

        templates = preparedQuery.getParameterTemplates();

        values = new String[templates.length];
        batch = new LinkedList<>();
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

        LocalDate d = null;
        if (cal != null) {
            d = x.toLocalDate().atStartOfDay(jvmZoneId)
                    .withZoneSameInstant(cal.getTimeZone().toZoneId()).toLocalDate();
        } else {
            d = x.toLocalDate();
        }

        ClickHouseValue value = templates[idx];
        if (value == null) {
            value = ClickHouseDateValue.ofNull();
        }
        values[idx] = value.update(d).toSqlExpression();
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw SqlExceptionUtils.clientError("setTime not implemented");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        if (x == null) {
            values[idx] = ClickHouseValues.NULL_EXPR;
            return;
        }

        LocalDateTime dt = null;
        if (cal != null) {
            dt = x.toLocalDateTime().atZone(jvmZoneId)
                    .withZoneSameInstant(cal.getTimeZone().toZoneId()).toLocalDateTime();
        } else {
            dt = x.toLocalDateTime();
        }

        ClickHouseValue value = templates[idx];
        if (value == null) {
            value = ClickHouseDateTimeValue.ofNull(dt.getNano() > 0 ? 9 : 0, serverTimeZone);
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
