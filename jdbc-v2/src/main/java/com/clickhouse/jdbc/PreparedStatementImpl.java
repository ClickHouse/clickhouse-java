package com.clickhouse.jdbc;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.Tuple;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.clickhouse.jdbc.internal.JdbcUtils;
import com.clickhouse.jdbc.internal.ParsedPreparedStatement;
import com.clickhouse.jdbc.metadata.ParameterMetaDataImpl;
import com.clickhouse.jdbc.metadata.ResultSetMetaDataImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PreparedStatementImpl extends StatementImpl implements PreparedStatement, JdbcV2Wrapper {
    private static final Logger LOG = LoggerFactory.getLogger(PreparedStatementImpl.class);

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder().appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
    public static final DateTimeFormatter DATETIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();

    private final Calendar defaultCalendar;

    private final String originalSql;
    private final String[] values; // temp value holder (set can be called > once)
    private final List<StringBuilder> batchValues; // composed value statements
    private final ParsedPreparedStatement parsedPreparedStatement;
    private final boolean insertStmtWithValues;
    private final String valueListTmpl;
    private final int[] paramPositionsInDataClause;

    private final int argCount;

    private final ParameterMetaData parameterMetaData;
    private ResultSetMetaData resultSetMetaData = null;

    public PreparedStatementImpl(ConnectionImpl connection, String sql, ParsedPreparedStatement parsedStatement) throws SQLException {
        super(connection);
        this.isPoolable = true; // PreparedStatement is poolable by default
        this.originalSql = sql;
        this.parsedPreparedStatement = parsedStatement;
        this.argCount = parsedStatement.getArgCount();

        this.defaultCalendar = connection.defaultCalendar;
        this.values = new String[argCount];
        this.parameterMetaData = new ParameterMetaDataImpl(this.values.length);

        int valueListStartPos = parsedStatement.getAssignValuesListStartPosition();
        int valueListStopPos = parsedStatement.getAssignValuesListStopPosition();
        if (parsedStatement.getAssignValuesGroups() == 1 && valueListStartPos > -1 && valueListStopPos > -1) {
            int[] positions = parsedStatement.getParamPositions();
            paramPositionsInDataClause = new int[argCount];
            for (int i = 0; i < argCount; i++) {
                int p = positions[i] - valueListStartPos;
                paramPositionsInDataClause[i] = p;
            }

            valueListTmpl = originalSql.substring(valueListStartPos, valueListStopPos + 1);
            insertStmtWithValues = true;
            batchValues = new ArrayList<>();
        } else {
            paramPositionsInDataClause = new int[0];
            batchValues = Collections.emptyList();
            valueListTmpl = "";
            insertStmtWithValues = false;
        }
    }

    private String buildSQL() {
        StringBuilder compiledSql = new StringBuilder(originalSql);
        int posOffset = 0;
        int[] positions = parsedPreparedStatement.getParamPositions();
        for (int i = 0; i < argCount; i++) {
            int p = positions[i] + posOffset;
            String val = values[i];
            compiledSql.replace(p, p+1, val == null ? "NULL" : val);
            posOffset += val == null ? 0 : val.length() - 1;
        }
        return compiledSql.toString();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        ensureOpen();
        return super.executeQueryImpl(buildSQL(), localSettings);
    }

    @Override
    public int executeUpdate() throws SQLException {
        ensureOpen();
        return (int) super.executeUpdateImpl(buildSQL(), localSettings);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        ensureOpen();
        setNull(parameterIndex, sqlType, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setDate(parameterIndex, x, null);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setTime(parameterIndex, x, null);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setTimestamp(parameterIndex, x, null);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void clearParameters() throws SQLException {
        ensureOpen();
        Arrays.fill(this.values, null);
    }

    int getParametersCount() {
        return values.length;
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        ensureOpen();
        setObject(parameterIndex, x, targetSqlType, 0);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        ensureOpen();
        setObject(parameterIndex, x, Types.OTHER);
    }

    @Override
    public boolean execute() throws SQLException {
        ensureOpen();
        if (parsedPreparedStatement.isHasResultSet()) {
            currentResultSet = super.executeQueryImpl(buildSQL(), localSettings);
            return true;
        } else {
            currentUpdateCount = super.executeUpdateImpl(buildSQL(), localSettings);
            return false;
        }
    }

    @Override
    public void addBatch() throws SQLException {
        ensureOpen();

        if (insertStmtWithValues) {
            StringBuilder valuesClause = new StringBuilder(valueListTmpl);
            int posOffset = 0;
            for (int i = 0; i < argCount; i++) {
                int p = paramPositionsInDataClause[i] + posOffset;
                valuesClause.replace(p, p + 1, values[i]);
                posOffset += values[i].length() - 1;
            }
            batchValues.add(valuesClause);
        } else {
            super.addBatch(buildSQL());
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        ensureOpen();

        if (insertStmtWithValues) {
            // run executeBatch
            return executeInsertBatch().stream().mapToInt(Integer::intValue).toArray();
        } else {
            List<Integer> results = new ArrayList<>();
            for (String sql : batch) {
                results.add((int) executeUpdateImpl(sql, localSettings));
            }
            return results.stream().mapToInt(Integer::intValue).toArray();
        }
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        ensureOpen();

        if (insertStmtWithValues) {
            return executeInsertBatch().stream().mapToLong(Integer::longValue).toArray();
        } else {
            List<Integer> results = new ArrayList<>();
            for (String sql : batch) {
                results.add((int) executeUpdateImpl(sql, localSettings));
            }
            return results.stream().mapToLong(Integer::longValue).toArray();
        }
    }

    private List<Integer> executeInsertBatch() throws SQLException {
        StringBuilder insertSql = new StringBuilder(originalSql.substring(0,
                parsedPreparedStatement.getAssignValuesListStartPosition()));

        for (StringBuilder valuesList : batchValues) {
            insertSql.append(valuesList).append(',');
        }
        insertSql.setLength(insertSql.length() - 1);

        int updateCount = (int) super.executeUpdateImpl(insertSql.toString(), localSettings);
        if (updateCount == batchValues.size()) {
            return Collections.nCopies(batchValues.size(), 1);
        } else {
            return Collections.nCopies(batchValues.size(), Statement.SUCCESS_NO_INFO);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x, int length) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        ensureOpen();
        if (!connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Ref is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ensureOpen();

        if (resultSetMetaData == null && currentResultSet == null) {
            // before execution
            if (parsedPreparedStatement.isHasResultSet()) {
                try {
                    // Replace '?' with NULL to make SQL valid for DESCRIBE
                    String sql = replaceQuestionMarks(originalSql, NULL_LITERAL);
                    TableSchema tSchema = connection.getClient().getTableSchemaFromQuery(sql);
                    resultSetMetaData = new ResultSetMetaDataImpl(tSchema.getColumns(),
                            connection.getSchema(), connection.getCatalog(),
                            tSchema.getTableName(), JdbcUtils.DATA_TYPE_CLASS_MAP);
                } catch (Exception e) {
                    LOG.warn("Failed to get schema for statement '{}'", originalSql);
                }
            }

            if (resultSetMetaData == null) {
                resultSetMetaData = new ResultSetMetaDataImpl(Collections.emptyList(),
                        connection.getSchema(), connection.getCatalog(),
                        "", JdbcUtils.DATA_TYPE_CLASS_MAP);
            }
        } else if (currentResultSet != null) {
            resultSetMetaData = currentResultSet.getMetaData();
        }

        return resultSetMetaData;
    }

    public static final String NULL_LITERAL = "NULL";

    private static final Pattern REPLACE_Q_MARK_PATTERN = Pattern.compile("(\"[^\"]*\"|`[^`]*`|'[^']*')|(\\?)");

    public static String replaceQuestionMarks(String sql, final String replacement) {
        Matcher matcher = REPLACE_Q_MARK_PATTERN.matcher(sql);

        StringBuilder result = new StringBuilder();

        int lastPos = 0;
        while (matcher.find()) {
            String text;
            if ((text = matcher.group(1)) != null) {
                // Quoted string — keep as-is
                String str = Matcher.quoteReplacement(text);
                result.append(sql, lastPos, matcher.start()).append(str);
                lastPos = matcher.end();
            } else if (matcher.group(2) != null) {
                // Question mark outside quotes — replace it
                String str = Matcher.quoteReplacement(replacement);
                result.append(sql, lastPos, matcher.start()).append(str);
                lastPos = matcher.end();
            }
        }

        // Add rest of the `sql`
        result.append(sql, lastPos, sql.length());
        return result.toString();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(sqlDateToInstant(x, cal));
    }

    protected Instant sqlDateToInstant(Date x, Calendar cal) {
        LocalDate d = x.toLocalDate();
        Calendar c = (Calendar) (cal != null ? cal : defaultCalendar).clone();
        c.clear();
        c.set(d.getYear(), d.getMonthValue() - 1, d.getDayOfMonth(), 0, 0, 0);
        return c.toInstant();
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(sqlTimeToInstant(x, cal));
    }

    protected Instant sqlTimeToInstant(Time x, Calendar cal) {
        LocalTime t = x.toLocalTime();
        Calendar c = (Calendar) (cal != null ? cal : defaultCalendar).clone();
        c.clear();
        c.set(1970, Calendar.JANUARY, 1, t.getHour(), t.getMinute(), t.getSecond());
        return c.toInstant();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(sqlTimestampToZDT(x, cal));
    }

    protected ZonedDateTime sqlTimestampToZDT(Timestamp x, Calendar cal) {
        LocalDateTime ldt = x.toLocalDateTime();
        Calendar c = (Calendar) (cal != null ? cal : defaultCalendar).clone();
        c.clear();
        c.set(ldt.getYear(), ldt.getMonthValue() - 1, ldt.getDayOfMonth(), ldt.getHour(), ldt.getMinute(), ldt.getSecond());
        return c.toInstant().atZone(ZoneId.of("UTC")).withNano(x.getNanos());
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(null);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    /**
     * Returned metadata has only minimal information about parameters. Currently only their count.
     * Current implementation do not parse SQL to detect type of each parameter.
     *
     * @see ParameterMetaDataImpl
     * @return {@link ParameterMetaDataImpl}
     * @throws SQLException if the statement is close
     */
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        ensureOpen();
        return parameterMetaData;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        ensureOpen();
        throw new SQLException("ROWID type is not supported by ClickHouse.",
                ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public void setNString(int parameterIndex, String x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setNClob(int parameterIndex, NClob x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setClob(int parameterIndex, Reader x, long length) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream x, long length) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setNClob(int parameterIndex, Reader x, long length) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        ensureOpen();
        setObject(parameterIndex, x, JDBCType.valueOf(targetSqlType), scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setClob(int parameterIndex, Reader x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setNClob(int parameterIndex, Reader x) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        ensureOpen();
        values[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        ensureOpen();
        setObject(parameterIndex, x, targetSqlType, 0);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        return executeUpdate();
    }

    @Override
    public final void addBatch(String sql) throws SQLException {
        ensureOpen();
        throw new SQLException(
                        "addBatch(String) cannot be called in PreparedStatement or CallableStatement!",
                ExceptionUtils.SQL_STATE_WRONG_OBJECT_TYPE);
    }

    @Override
    public final boolean execute(String sql) throws SQLException {
        ensureOpen();
        throw new SQLException(
                        "execute(String) cannot be called in PreparedStatement or CallableStatement!",
                ExceptionUtils.SQL_STATE_WRONG_OBJECT_TYPE);
    }

    @Override
    public final boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        ensureOpen();
        throw new SQLException(
                        "execute(String, int) cannot be called in PreparedStatement or CallableStatement!",
                ExceptionUtils.SQL_STATE_WRONG_OBJECT_TYPE);
    }

    @Override
    public final boolean execute(String sql, int[] columnIndexes) throws SQLException {
        ensureOpen();
        throw new SQLException(
                        "execute(String, int[]) cannot be called in PreparedStatement or CallableStatement!",
                ExceptionUtils.SQL_STATE_WRONG_OBJECT_TYPE);
    }

    @Override
    public final boolean execute(String sql, String[] columnNames) throws SQLException {
        ensureOpen();
        throw new SQLException(
                        "execute(String, String[]) cannot be called in PreparedStatement or CallableStatement!",
                ExceptionUtils.SQL_STATE_WRONG_OBJECT_TYPE);
    }

    @Override
    public final long executeLargeUpdate(String sql) throws SQLException {
        ensureOpen();
        throw new SQLException(
                        "executeLargeUpdate(String) cannot be called in PreparedStatement or CallableStatement!",
                ExceptionUtils.SQL_STATE_WRONG_OBJECT_TYPE);
    }

    @Override
    public final long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        ensureOpen();
        throw new SQLException(
                        "executeLargeUpdate(String, int) cannot be called in PreparedStatement or CallableStatement!",
                ExceptionUtils.SQL_STATE_WRONG_OBJECT_TYPE);
    }

    @Override
    public final long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        ensureOpen();
        throw new SQLException(
                        "executeLargeUpdate(String, int[]) cannot be called in PreparedStatement or CallableStatement!",
                ExceptionUtils.SQL_STATE_WRONG_OBJECT_TYPE);
    }

    @Override
    public final long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        ensureOpen();
        throw new SQLException(
                        "executeLargeUpdate(String, String[]) cannot be called in PreparedStatement or CallableStatement!",
                ExceptionUtils.SQL_STATE_WRONG_OBJECT_TYPE);
    }

    @Override
    public final ResultSet executeQuery(String sql) throws SQLException {
        ensureOpen();
        throw new SQLException(
                        "executeQuery(String) cannot be called in PreparedStatement or CallableStatement!",
                ExceptionUtils.SQL_STATE_WRONG_OBJECT_TYPE);
    }

    @Override
    public final int executeUpdate(String sql) throws SQLException {
        ensureOpen();
        throw new SQLException(
                        "executeUpdate(String) cannot be called in PreparedStatement or CallableStatement!",
                ExceptionUtils.SQL_STATE_WRONG_OBJECT_TYPE);
    }

    @Override
    public final int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        ensureOpen();
        throw new SQLException(
                        "executeUpdate(String, int) cannot be called in PreparedStatement or CallableStatement!",
                ExceptionUtils.SQL_STATE_WRONG_OBJECT_TYPE);
    }

    @Override
    public final int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        ensureOpen();
        throw new SQLException(
                        "executeUpdate(String, int[]) cannot be called in PreparedStatement or CallableStatement!",
                ExceptionUtils.SQL_STATE_WRONG_OBJECT_TYPE);
    }

    @Override
    public final int executeUpdate(String sql, String[] columnNames) throws SQLException {
        ensureOpen();
        throw new SQLException(
                        "executeUpdate(String, String[]) cannot be called in PreparedStatement or CallableStatement!",
                ExceptionUtils.SQL_STATE_WRONG_OBJECT_TYPE);
    }

    private static String encodeObject(Object x) throws SQLException {
        LOG.trace("Encoding object: {}", x);

        try {
            if (x == null) {
                return "NULL";
            } else if (x instanceof String) {
                return "'" + escapeString((String) x) + "'";
            } else if (x instanceof Boolean) {
                return (Boolean) x ? "1" : "0";
            } else if (x instanceof Date) {
                return "'" + DATE_FORMATTER.format(((Date) x).toLocalDate()) + "'";
            } else if (x instanceof LocalDate) {
                return "'" + DATE_FORMATTER.format((LocalDate) x) + "'";
            } else if (x instanceof Time) {
                return "'" + TIME_FORMATTER.format(((Time) x).toLocalTime()) + "'";
            } else if (x instanceof LocalTime) {
                return "'" + TIME_FORMATTER.format((LocalTime) x) + "'";
            } else if (x instanceof Timestamp) {
                return "'" + DATETIME_FORMATTER.format(((Timestamp) x).toLocalDateTime()) + "'";
            } else if (x instanceof LocalDateTime) {
                return "'" + DATETIME_FORMATTER.format((LocalDateTime) x) + "'";
            } else if (x instanceof OffsetDateTime) {
                return encodeObject(((OffsetDateTime) x).toInstant());
            } else if (x instanceof ZonedDateTime) {
                return encodeObject(((ZonedDateTime) x).toInstant());
            } else if (x instanceof Instant) {
                return "fromUnixTimestamp64Nano(" + (((Instant) x).getEpochSecond() * 1_000_000_000L + ((Instant) x).getNano()) + ")";
            } else if (x instanceof InetAddress) {
                return "'" + ((InetAddress) x).getHostAddress() + "'";
            } else if (x instanceof Array) {
                StringBuilder listString = new StringBuilder();
                listString.append("[");
                int i = 0;
                for (Object item : (Object[]) ((Array) x).getArray()) {
                    if (i > 0) {
                        listString.append(", ");
                    }
                    listString.append(encodeObject(item));
                    i++;
                }
                listString.append("]");

                return listString.toString();
            } else if (x.getClass().isArray()) {
                StringBuilder listString = new StringBuilder();
                listString.append("[");


                if (x.getClass().getComponentType().isPrimitive()) {
                    int len = java.lang.reflect.Array.getLength(x);
                    for (int  i = 0; i < len; i++) {
                        if (i > 0) {
                            listString.append(", ");
                        }
                        listString.append(encodeObject(java.lang.reflect.Array.get(x, i)));
                    }
                } else {
                    int i = 0;
                    for (Object item : (Object[]) x) {
                        if (i > 0) {
                            listString.append(", ");
                        }
                        listString.append(encodeObject(item));
                        i++;
                    }
                }
                listString.append("]");

                return listString.toString();
            } else if (x instanceof Collection) {
                StringBuilder listString = new StringBuilder();
                listString.append("[");
                for (Object item : (Collection<?>) x) {
                    listString.append(encodeObject(item)).append(", ");
                }
                if (listString.length() > 1) {
                    listString.delete(listString.length() - 2, listString.length());
                }
                listString.append("]");

                return listString.toString();
            } else if (x instanceof Map) {
                Map<?, ?> tmpMap = (Map<?, ?>) x;
                StringBuilder mapString = new StringBuilder();
                mapString.append("{");
                for (Object key : tmpMap.keySet()) {
                    mapString.append(encodeObject(key)).append(": ").append(encodeObject(tmpMap.get(key))).append(", ");
                }
                if (!tmpMap.isEmpty())
                    mapString.delete(mapString.length() - 2, mapString.length());
                mapString.append("}");

                return mapString.toString();
            } else if (x instanceof Reader) {
                StringBuilder sb = new StringBuilder();
                Reader reader = (Reader) x;
                char[] buffer = new char[1024];
                int len;
                while ((len = reader.read(buffer)) != -1) {
                    sb.append(buffer, 0, len);
                }
                return "'" + escapeString(sb.toString()) + "'";
            } else if (x instanceof InputStream) {
                StringBuilder sb = new StringBuilder();
                InputStream is = (InputStream) x;
                byte[] buffer = new byte[1024];
                int len;
                while ((len = is.read(buffer)) != -1) {
                    sb.append(new String(buffer, 0, len));
                }
                return "'" + escapeString(sb.toString()) + "'";
            } else if (x instanceof Object[]) {
                StringBuilder arrayString = new StringBuilder();
                arrayString.append("[");
                int i = 0;
                for (Object item : (Object[]) x) {
                    if (i > 0) {
                        arrayString.append(", ");
                    }
                    arrayString.append(encodeObject(item));
                    i++;
                }
                arrayString.append("]");

                return arrayString.toString();
            } else if (x instanceof Tuple) {
                StringBuilder tupleString = new StringBuilder();
                tupleString.append("(");
                Tuple t = (Tuple) x;
                Object [] values = t.getValues();
                int i = 0;
                for (Object item : values) {
                    if (i > 0) {
                        tupleString.append(", ");
                    }
                    tupleString.append(encodeObject(item));
                    i++;
                }
                tupleString.append(")");
                return tupleString.toString();
            } else if (x instanceof UUID) {
                return "'" + escapeString(((UUID) x).toString()) + "'";
            }

            return escapeString(x.toString());//Escape single quotes
        } catch (Exception e) {
            LOG.error("Error encoding object", e);
            throw new SQLException("Error encoding object", ExceptionUtils.SQL_STATE_SQL_ERROR, e);
        }
    }

    private static String escapeString(String x) {
        return x.replace("\\", "\\\\").replace("'", "\\'");//Escape single quotes
    }
}
