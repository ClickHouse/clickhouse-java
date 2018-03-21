package ru.yandex.clickhouse;

import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import ru.yandex.clickhouse.response.ClickHouseResponse;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import ru.yandex.clickhouse.util.ClickHouseArrayUtil;
import ru.yandex.clickhouse.util.guava.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class ClickHousePreparedAbstractStatementImpl extends ClickHouseStatementImpl implements ClickHousePreparedStatement {
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public ClickHousePreparedAbstractStatementImpl(CloseableHttpClient client,
                                                   ClickHouseConnection connection,
                                                   ClickHouseProperties properties,
                                                   TimeZone timezone) throws SQLException {

        super(client, connection, properties);
        initTimeZone(timezone);
    }

    private void initTimeZone(TimeZone timeZone) {
        dateTimeFormat.setTimeZone(timeZone);
        if (properties.isUseServerTimeZoneForDates())
            dateFormat.setTimeZone(timeZone);
    }


    protected abstract void setParameter(int index, String value, boolean isQuote);

    protected abstract String getSqlQuery() throws SQLException;

    protected static void checkParameters(String[] parameters) throws SQLException {
        int index = 0;
        for (String parameter : parameters) {
            ++index;
            if (parameter == null)
                throw new SQLException("Not all parameters binded (placeholder " + index + " is undefined)");
        }
    }

    @Override
    public ClickHouseResponse executeQueryClickhouseResponse() throws SQLException {
        return super.executeQueryClickhouseResponse(getSqlQuery());
    }

    @Override
    public ClickHouseResponse executeQueryClickhouseResponse(Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return super.executeQueryClickhouseResponse(getSqlQuery(), additionalDBParams);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(getSqlQuery());
    }

    @Override
    public ResultSet executeQuery(Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return super.executeQuery(getSqlQuery(), additionalDBParams);
    }

    @Override
    public ResultSet executeQuery(Map<ClickHouseQueryParam, String> additionalDBParams, List<ClickHouseExternalData> externalData) throws SQLException {
        return super.executeQuery(getSqlQuery(), additionalDBParams, externalData);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(getSqlQuery());
    }

    private void setParameter(int parameterIndex, String bind) {
        setParameter(parameterIndex, bind, false);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParameter(parameterIndex, "\\N");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParameter(parameterIndex, x ? "1" : "0");
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParameter(parameterIndex, Byte.toString(x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParameter(parameterIndex, Short.toString(x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParameter(parameterIndex, Integer.toString(x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParameter(parameterIndex, Long.toString(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParameter(parameterIndex, Float.toString(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParameter(parameterIndex, Double.toString(x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParameter(parameterIndex, x.toPlainString());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setParameter(parameterIndex, ClickHouseUtil.escape(x), x != null);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setParameter(parameterIndex, new String(x, StreamUtils.UTF_8));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setParameter(parameterIndex, dateFormat.format(x), true);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
        //        setParameter(parameterIndex, "toDateTime('" + dateTimeFormat.format(x) + "')");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setParameter(parameterIndex, dateTimeFormat.format(x), true);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Collection collection) throws SQLException {
        setParameter(parameterIndex, ClickHouseArrayUtil.toString(collection));
    }

    @Override
    public void setArray(int parameterIndex, Object[] array) throws SQLException {
        setParameter(parameterIndex, ClickHouseArrayUtil.toString(array));
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.OTHER);
            return;
        }


        if (x instanceof Byte)
            setInt(parameterIndex, ((Byte) x).intValue());
        else if (x instanceof String)
            setString(parameterIndex, (String) x);
        else if (x instanceof BigDecimal)
            setBigDecimal(parameterIndex, (BigDecimal) x);
        else if (x instanceof Short)
            setShort(parameterIndex, ((Short) x).shortValue());
        else if (x instanceof Integer)
            setInt(parameterIndex, ((Integer) x).intValue());
        else if (x instanceof Long)
            setLong(parameterIndex, ((Long) x).longValue());
        else if (x instanceof Float)
            setFloat(parameterIndex, ((Float) x).floatValue());
        else if (x instanceof Double)
            setDouble(parameterIndex, ((Double) x).doubleValue());
        else if (x instanceof byte[])
            setBytes(parameterIndex, (byte[]) x);
        else if (x instanceof Date)
            setDate(parameterIndex, (Date) x);
        else if (x instanceof Time)
            setTime(parameterIndex, (Time) x);
        else if (x instanceof Timestamp)
            setTimestamp(parameterIndex, (Timestamp) x);
        else if (x instanceof Boolean)
            setBoolean(parameterIndex, ((Boolean) x).booleanValue());
        else if (x instanceof InputStream)
            setBinaryStream(parameterIndex, (InputStream) x, -1);
        else if (x instanceof Blob)
            setBlob(parameterIndex, (Blob) x);
        else if (x instanceof Clob)
            setClob(parameterIndex, (Clob) x);
        else if (x instanceof BigInteger)
            setParameter(parameterIndex, x.toString());
        else if (x instanceof Collection)
            setArray(parameterIndex, (Collection) x);
        else if (x.getClass().isArray())
            setArray(parameterIndex, (Object[]) x);
        else
            throw new SQLDataException("Can't bind object of class " + x.getClass().getCanonicalName());

    }

    protected static class BatchHttpEntity extends AbstractHttpEntity {
        private final List<byte[]> rows;

        public BatchHttpEntity(List<byte[]> rows) {
            this.rows = rows;
        }

        @Override
        public boolean isRepeatable() {
            return true;
        }

        @Override
        public long getContentLength() {
            return -1;
        }

        @Override
        public InputStream getContent() throws IOException, IllegalStateException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(OutputStream outputStream) throws IOException {
            for (byte[] row : rows)
                outputStream.write(row);
        }

        @Override
        public boolean isStreaming() {
            return false;
        }
    }


    @Override
    public boolean execute() throws SQLException {
        return super.execute(getSqlQuery());
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        setParameter(parameterIndex, ClickHouseArrayUtil.arrayToString(x.getArray()));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
