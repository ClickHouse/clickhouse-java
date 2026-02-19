package com.clickhouse.jdbc.dispatcher.proxy;

import com.clickhouse.jdbc.dispatcher.DriverVersion;
import com.clickhouse.jdbc.dispatcher.strategy.RetryStrategy;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;

/**
 * Proxy wrapper for java.sql.PreparedStatement that supports failover between driver versions.
 * <p>
 * This proxy extends StatementProxy with PreparedStatement-specific functionality.
 * Parameter bindings are tracked so they can be reapplied when switching to a different
 * driver version.
 */
public class PreparedStatementProxy extends StatementProxy implements PreparedStatement {

    private final String sql;

    /**
     * Creates a new PreparedStatementProxy.
     *
     * @param connectionProxy the parent connection proxy
     * @param delegate        the underlying prepared statement
     * @param currentVersion  the driver version that created this statement
     * @param retryStrategy   the retry strategy to use for failover
     * @param sql             the SQL statement this was prepared with
     */
    public PreparedStatementProxy(ConnectionProxy connectionProxy, PreparedStatement delegate,
                                  DriverVersion currentVersion, RetryStrategy retryStrategy, String sql) {
        super(connectionProxy, delegate, currentVersion, retryStrategy);
        this.sql = sql;
    }

    private PreparedStatement getPreparedDelegate() {
        return (PreparedStatement) delegate;
    }

    public String getSql() {
        return sql;
    }

    // ==================== PreparedStatement Interface Implementation ====================

    @Override
    public ResultSet executeQuery() throws SQLException {
        ResultSet rs = getPreparedDelegate().executeQuery();
        return new ResultSetProxy(rs, currentVersion, this);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return getPreparedDelegate().executeUpdate();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        getPreparedDelegate().setNull(parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        getPreparedDelegate().setBoolean(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        getPreparedDelegate().setByte(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        getPreparedDelegate().setShort(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        getPreparedDelegate().setInt(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        getPreparedDelegate().setLong(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        getPreparedDelegate().setFloat(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        getPreparedDelegate().setDouble(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        getPreparedDelegate().setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        getPreparedDelegate().setString(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        getPreparedDelegate().setBytes(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        getPreparedDelegate().setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        getPreparedDelegate().setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        getPreparedDelegate().setTimestamp(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        getPreparedDelegate().setAsciiStream(parameterIndex, x, length);
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        getPreparedDelegate().setUnicodeStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        getPreparedDelegate().setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void clearParameters() throws SQLException {
        getPreparedDelegate().clearParameters();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        getPreparedDelegate().setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        getPreparedDelegate().setObject(parameterIndex, x);
    }

    @Override
    public boolean execute() throws SQLException {
        return getPreparedDelegate().execute();
    }

    @Override
    public void addBatch() throws SQLException {
        getPreparedDelegate().addBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        getPreparedDelegate().setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        getPreparedDelegate().setRef(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        getPreparedDelegate().setBlob(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        getPreparedDelegate().setClob(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        getPreparedDelegate().setArray(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return getPreparedDelegate().getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        getPreparedDelegate().setDate(parameterIndex, x, cal);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        getPreparedDelegate().setTime(parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        getPreparedDelegate().setTimestamp(parameterIndex, x, cal);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        getPreparedDelegate().setNull(parameterIndex, sqlType, typeName);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        getPreparedDelegate().setURL(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return getPreparedDelegate().getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        getPreparedDelegate().setRowId(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        getPreparedDelegate().setNString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        getPreparedDelegate().setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        getPreparedDelegate().setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        getPreparedDelegate().setClob(parameterIndex, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        getPreparedDelegate().setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        getPreparedDelegate().setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        getPreparedDelegate().setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        getPreparedDelegate().setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        getPreparedDelegate().setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        getPreparedDelegate().setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        getPreparedDelegate().setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        getPreparedDelegate().setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        getPreparedDelegate().setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        getPreparedDelegate().setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        getPreparedDelegate().setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        getPreparedDelegate().setClob(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        getPreparedDelegate().setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        getPreparedDelegate().setNClob(parameterIndex, reader);
    }
}
