package com.clickhouse.jdbc;

import com.clickhouse.jdbc.internal.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
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
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.Map;

public class PreparedStatementImpl extends StatementImpl implements PreparedStatement, JdbcV2Wrapper {
    private static final Logger LOG = LoggerFactory.getLogger(PreparedStatementImpl.class);

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder().appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
    public static final DateTimeFormatter DATETIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();

    String originalSql;
    String [] sqlSegments;
    Object [] parameters;
    public PreparedStatementImpl(ConnectionImpl connection, String sql) throws SQLException {
        super(connection);
        this.originalSql = sql;
        //Split the sql string into an array of strings around question mark tokens
        this.sqlSegments = sql.split("\\?");

        //Create an array of objects to store the parameters
        if (originalSql.contains("?")) {
            int count = originalSql.length() - originalSql.replace("?", "").length();
            this.parameters = new Object[count];
        } else {
            this.parameters = new Object[0];
        }
    }

    private String compileSql() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < sqlSegments.length; i++) {
            sb.append(sqlSegments[i]);
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
        return executeQuery(compileSql());
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkClosed();
        return executeUpdate(compileSql());
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
        checkClosed();
        setDate(parameterIndex, x, null);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkClosed();
        setTime(parameterIndex, x, null);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        checkClosed();
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
        return execute(compileSql());
    }

    @Override
    public void addBatch() throws SQLException {
        checkClosed();
        addBatch(compileSql());
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
        if (cal == null) {
            cal = new GregorianCalendar();
            cal.setTime(x);
        }

        ZoneId tz = cal.getTimeZone().toZoneId();
        Calendar c = (Calendar) cal.clone();
        c.setTime(x);
        parameters[parameterIndex - 1] = encodeObject(c.toInstant().atZone(tz).toLocalDate());
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        checkClosed();
        if (cal == null) {
            cal = new GregorianCalendar();
            cal.setTime(x);
        }

        ZoneId tz = cal.getTimeZone().toZoneId();
        Calendar c = (Calendar) cal.clone();
        c.setTime(x);
        parameters[parameterIndex - 1] = encodeObject(c.toInstant().atZone(tz).toLocalTime());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        checkClosed();
        if (cal == null) {
            cal = new GregorianCalendar();
            cal.setTime(x);
        }

        ZoneId tz = cal.getTimeZone().toZoneId();
        Calendar c = (Calendar) cal.clone();
        c.setTime(x);
        parameters[parameterIndex - 1] = encodeObject(c.toInstant().atZone(tz).withNano(x.getNanos()).toLocalDateTime());
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
            } else if (x instanceof Array) {
                StringBuilder listString = new StringBuilder();
                listString.append("[");
                int i = 0;
                for (Object item : (Object[])((Array) x).getArray()) {
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
            } else if (x instanceof Map<?, ?> tmpMap) {
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
            }

            return escapeString(x.toString());//Escape single quotes
        } catch (Exception e) {
            LOG.error("Error encoding object", e);
            throw new SQLException("Error encoding object", ExceptionUtils.SQL_STATE_SQL_ERROR, e);
        }
    }

    private static String escapeString(String x) {
        return x.replace("'", "''");//Escape single quotes
    }
}
