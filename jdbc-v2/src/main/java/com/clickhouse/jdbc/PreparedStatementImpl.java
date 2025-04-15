package com.clickhouse.jdbc;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.Tuple;
import com.clickhouse.jdbc.internal.ExceptionUtils;
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
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PreparedStatementImpl extends StatementImpl implements PreparedStatement, JdbcV2Wrapper {
    private static final Logger LOG = LoggerFactory.getLogger(PreparedStatementImpl.class);

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder().appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
    public static final DateTimeFormatter DATETIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();

    private final Calendar defaultCalendar;

    String originalSql;
    String [] sqlSegments;
    String [] valueSegments;
    Object [] parameters;
    String insertIntoSQL;

    StatementType statementType;

    public PreparedStatementImpl(ConnectionImpl connection, String sql) throws SQLException {
        super(connection);
        this.originalSql = sql.trim();
        //Split the sql string into an array of strings around question mark tokens
        this.sqlSegments = splitStatement(originalSql);
        this.statementType = parseStatementType(originalSql);

        if (statementType == StatementType.INSERT) {
            insertIntoSQL = originalSql.substring(0, originalSql.indexOf("VALUES") + 6);
            valueSegments = originalSql.substring(originalSql.indexOf("VALUES") + 6).split("\\?");
        }

        //Create an array of objects to store the parameters
        this.parameters = new Object[sqlSegments.length - 1];
        this.defaultCalendar = connection.defaultCalendar;
    }

    private String compileSql(String [] segments) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < segments.length; i++) {
            sb.append(segments[i]);
            if (i < parameters.length) {
                sb.append(parameters[i]);
            }
        }
        LOG.trace("Compiled SQL: {}", sb);
        return sb.toString();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        return executeQuery(compileSql(sqlSegments));
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkClosed();
        return executeUpdate(compileSql(sqlSegments));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkClosed();
        setNull(parameterIndex, sqlType, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
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
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        if (originalSql.contains("?")) {
            this.parameters = new Object[sqlSegments.length];
        } else {
            this.parameters = new Object[0];
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        checkClosed();
        setObject(parameterIndex, x, targetSqlType, 0);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkClosed();
        setObject(parameterIndex, x, Types.OTHER);
    }

    @Override
    public boolean execute() throws SQLException {
        checkClosed();
        return execute(compileSql(sqlSegments));
    }

    @Override
    public void addBatch() throws SQLException {
        checkClosed();
        if (statementType == StatementType.INSERT) {
            addBatch(compileSql(valueSegments));
        } else {
            addBatch(compileSql(sqlSegments));
        }

    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        if (statementType == StatementType.INSERT && !batch.isEmpty()) {
            List<Integer> results = new ArrayList<>();
            // write insert into as batch to avoid multiple requests
            StringBuilder sb = new StringBuilder();
            sb.append(insertIntoSQL).append(" ");
            for (String sql : batch) {
                sb.append(sql).append(",");
            }
            sb.setCharAt(sb.length() - 1, ';');
            results.add(executeUpdate(sb.toString()));
            // clear batch and re-add insert into
            batch.clear();
            return results.stream().mapToInt(i -> i).toArray();
        } else {
            // run executeBatch
            return super.executeBatch();
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x, int length) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        checkClosed();
        if (!connection.config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Ref is not supported.", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        checkClosed();
        LocalDate d = x.toLocalDate();
        Calendar c = (Calendar) (cal != null ? cal : defaultCalendar).clone();
        c.clear();
        c.set(d.getYear(), d.getMonthValue() - 1, d.getDayOfMonth(), 0, 0, 0);
        parameters[parameterIndex - 1] = encodeObject(c.toInstant());
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        checkClosed();

        LocalTime t = x.toLocalTime();
        Calendar c = (Calendar) (cal != null ? cal : defaultCalendar).clone();
        c.clear();
        c.set(1970, Calendar.JANUARY, 1, t.getHour(), t.getMinute(), t.getSecond());
        parameters[parameterIndex - 1] = encodeObject(c.toInstant());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        checkClosed();

        LocalDateTime ldt = x.toLocalDateTime();
        Calendar c = (Calendar) (cal != null ? cal : defaultCalendar).clone();
        c.clear();
        c.set(ldt.getYear(), ldt.getMonthValue() - 1, ldt.getDayOfMonth(), ldt.getHour(), ldt.getMinute(), ldt.getSecond());
        parameters[parameterIndex - 1] = encodeObject(c.toInstant().atZone(ZoneId.of("UTC")).withNano(x.getNanos()));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(null);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setNString(int parameterIndex, String x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setNClob(int parameterIndex, NClob x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setClob(int parameterIndex, Reader x, long length) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream x, long length) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setNClob(int parameterIndex, Reader x, long length) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        checkClosed();
        setObject(parameterIndex, x, JDBCType.valueOf(targetSqlType), scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setClob(int parameterIndex, Reader x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setNClob(int parameterIndex, Reader x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = encodeObject(x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        checkClosed();
        setObject(parameterIndex, x, targetSqlType, 0);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        checkClosed();
        return PreparedStatement.super.executeLargeUpdate();
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
            } else if (x instanceof Collection) {
                StringBuilder listString = new StringBuilder();
                listString.append("[");
                for (Object item : (Collection<?>) x) {
                    listString.append(encodeObject(item)).append(", ");
                }
                listString.delete(listString.length() - 2, listString.length());
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

    private static String [] splitStatement(String sql) {
        List<String> segments = new ArrayList<>();
        char[] chars = sql.toCharArray();
        int segmentStart = 0;
        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];
            if (c == '\'' || c == '"' || c == '`') {
                // string literal or identifier
                i = skip(chars, i + 1, c, true);
            } else if (c == '/' && lookahead(chars, i) == '*') {
                // block comment
                int end = sql.indexOf("*/", i);
                if (end == -1) {
                    // missing comment end
                    break;
                }
                i = end + 1;
            } else if (c == '#' || (c == '-' && lookahead(chars, i) == '-')) {
                // line comment
                i = skip(chars, i + 1, '\n', false);
            } else if (c == '?') {
                // question mark
                segments.add(sql.substring(segmentStart, i));
                segmentStart = i + 1;
            }
        }
        if (segmentStart < chars.length) {
            segments.add(sql.substring(segmentStart));
        } else {
            // add empty segment in case question mark was last char of sql
            segments.add("");
        }
        return segments.toArray(new String[0]);
    }

    private static int skip(char[] chars, int from, char until, boolean escape) {
        for (int i = from; i < chars.length; i++) {
            char curr = chars[i];
            if (escape) {
                char next = lookahead(chars, i);
                if ((curr == '\\' && (next == '\\' || next == until)) || (curr == until && next == until)) {
                    // should skip:
                    // 1) double \\ (backslash escaped with backslash)
                    // 2) \[until] ([until] char, escaped with backslash)
                    // 3) [until][until] ([until] char, escaped with [until])
                    i++;
                    continue;
                }
            }

            if (curr == until) {
                return i;
            }
        }
        return chars.length;
    }

    private static char lookahead(char[] chars, int pos) {
        pos = pos + 1;
        if (pos >= chars.length) {
            return '\0';
        }
        return chars[pos];
    }

}
