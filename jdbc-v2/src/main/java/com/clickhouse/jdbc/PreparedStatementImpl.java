package com.clickhouse.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalField;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class PreparedStatementImpl extends StatementImpl implements PreparedStatement, JdbcWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(PreparedStatementImpl.class);

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder().appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
    public static final DateTimeFormatter DATETIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();

    String originalSql;
    String [] sqlSegments;
    Object [] parameters;
    public PreparedStatementImpl(ConnectionImpl connection, String sql) {
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
        parameters[parameterIndex - 1] = null;
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = x;
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = x;
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = x;
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = x;
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = x;
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = x;
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = x;
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = x;
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = "'" + x + "'";
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = new String(x, StandardCharsets.UTF_8);
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
        throw new SQLFeatureNotSupportedException("AsciiStream is not supported.");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("UnicodeStream is not supported.");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("BinaryStream is not supported.");
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
        throw new SQLFeatureNotSupportedException("Object is not supported.");
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
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("CharacterStream is not supported.");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Ref is not supported.");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Blob is not supported.");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Clob is not supported.");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        checkClosed();
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
        parameters[parameterIndex - 1] = String.format("'%s'", DATE_FORMATTER.format(c.toInstant().atZone(tz).toLocalDate()));
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
        System.out.println(String.format("'%s'", TIME_FORMATTER.format(c.toInstant().atZone(tz).toLocalTime())));
        parameters[parameterIndex - 1] = String.format("'%s'", TIME_FORMATTER.format(c.toInstant().atZone(tz).toLocalTime()));
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
        parameters[parameterIndex - 1] = String.format("'%s'", DATETIME_FORMATTER.format(c.toInstant().atZone(tz).withNano(x.getNanos()).toLocalDateTime()));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = null;
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        checkClosed();
        parameters[parameterIndex - 1] = x;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("RowId is not supported.");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("NString is not supported.");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("NCharacterStream is not supported.");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("NClob is not supported.");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Clob is not supported.");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Blob is not supported.");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("NClob is not supported.");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("SQLXML is not supported.");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Object is not supported.");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("AsciiStream is not supported.");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("BinaryStream is not supported.");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("CharacterStream is not supported.");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("AsciiStream is not supported.");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("BinaryStream is not supported.");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("CharacterStream is not supported.");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("NCharacterStream is not supported.");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Clob is not supported.");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Blob is not supported.");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("NClob is not supported.");
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        checkClosed();
        PreparedStatement.super.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        checkClosed();
        PreparedStatement.super.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        checkClosed();
        return PreparedStatement.super.executeLargeUpdate();
    }
}
