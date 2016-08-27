package ru.yandex.clickhouse;

import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;


public class ClickHousePreparedStatementImpl extends ClickHouseStatementImpl implements ClickHousePreparedStatement {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseStatementImpl.class);

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final String sql;
    private final List<String> sqlParts;
    private List<String> binds;
    private List<byte[]> batchRows = new ArrayList<byte[]>();

    public ClickHousePreparedStatementImpl(CloseableHttpClient client, ClickHouseDataSource source,
                                           ClickHouseConnection connection, ClickHouseProperties properties,
                                           String sql) throws SQLException {
        super(client, source, connection, properties);
        this.sql = sql;
        this.sqlParts = parseSql(sql);
        createBinds();
    }

    private void createBinds() {
        this.binds = new ArrayList<String>(this.sqlParts.size() - 1);
        for (int i = 0; i < this.sqlParts.size() - 1; i++) {
            this.binds.add(null);
        }
    }

    protected static List<String> parseSql(String sql) throws SQLException {
        if (sql == null) {
            throw new SQLException("sql statement can't be null");
        }

        List<String> parts = new ArrayList<String>();

        boolean afterBackSlash = false, inQuotes = false, inBackQuotes = false;
        int partStart = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (afterBackSlash) {
                afterBackSlash = false;
            } else if (c == '\\') {
                afterBackSlash = true;
            } else if (c == '\'') {
                inQuotes = !inQuotes;
            } else if (c == '`') {
                inBackQuotes = !inBackQuotes;
            } else if (c == '?' && !inQuotes && !inBackQuotes) {
                parts.add(sql.substring(partStart, i));
                partStart = i + 1;
            }
        }
        parts.add(sql.substring(partStart, sql.length()));

        return parts;
    }

    protected String buildSql() throws SQLException {
        if (sqlParts.size() == 1) {
            return sqlParts.get(0);
        }
        checkBinded(binds);

        StringBuilder sb = new StringBuilder(sqlParts.get(0));
        for (int i = 1; i < sqlParts.size(); i++) {
            sb.append(binds.get(i - 1));
            sb.append(sqlParts.get(i));
        }
        String sql = sb.toString();

        return sql;
    }

    private static void checkBinded(List<String> binds) throws SQLException {
        for (String b : binds) {
            if (b == null) {
                throw new SQLException("Not all parameters binded");
            }
        }
    }

    private byte[] buildBinds() throws SQLException {
        checkBinded(binds);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < binds.size(); i++) {
            sb.append(unquoteIfNeeded(binds.get(i)));
            sb.append(i < binds.size() - 1 ? '\t' : '\n');
        }
        return sb.toString().getBytes();
    }

    //TODO Think of more efficient solution
    private String unquoteIfNeeded(String value) {
        if (value.isEmpty()) {
            return value;
        }
        if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }


    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(buildSql());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(buildSql());
    }

    private void setBind(int parameterIndex, String bind) {
        binds.set(parameterIndex - 1, bind);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setBind(parameterIndex, "NULL");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setBind(parameterIndex, x ? "1" : "0");
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setBind(parameterIndex, Byte.toString(x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setBind(parameterIndex, Short.toString(x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setBind(parameterIndex, Integer.toString(x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setBind(parameterIndex, Long.toString(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setBind(parameterIndex, Float.toString(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setBind(parameterIndex, Double.toString(x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setBind(parameterIndex, x.toPlainString());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setBind(parameterIndex, ClickHouseUtil.quote(x));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setBind(parameterIndex, new String(x));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setBind(parameterIndex, "'" + dateFormat.format(x) + "'");
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
        //        setBind(parameterIndex, "toDateTime('" + dateTimeFormat.format(x) + "')");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setBind(parameterIndex, "'" + dateTimeFormat.format(x) + "'");
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
    public void clearParameters() throws SQLException {
        for (int i = 0; i < binds.size() - 1; i++) {
            binds.set(i, null);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);

    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.OTHER);
        } else {
            if (x instanceof Byte) {
                setInt(parameterIndex, ((Byte) x).intValue());
            } else if (x instanceof String) {
                setString(parameterIndex, (String) x);
            } else if (x instanceof BigDecimal) {
                setBigDecimal(parameterIndex, (BigDecimal) x);
            } else if (x instanceof Short) {
                setShort(parameterIndex, ((Short) x).shortValue());
            } else if (x instanceof Integer) {
                setInt(parameterIndex, ((Integer) x).intValue());
            } else if (x instanceof Long) {
                setLong(parameterIndex, ((Long) x).longValue());
            } else if (x instanceof Float) {
                setFloat(parameterIndex, ((Float) x).floatValue());
            } else if (x instanceof Double) {
                setDouble(parameterIndex, ((Double) x).doubleValue());
            } else if (x instanceof byte[]) {
                setBytes(parameterIndex, (byte[]) x);
            } else if (x instanceof Date) {
                setDate(parameterIndex, (Date) x);
            } else if (x instanceof Time) {
                setTime(parameterIndex, (Time) x);
            } else if (x instanceof Timestamp) {
                setTimestamp(parameterIndex, (Timestamp) x);
            } else if (x instanceof Boolean) {
                setBoolean(parameterIndex, ((Boolean) x).booleanValue());
            } else if (x instanceof InputStream) {
                setBinaryStream(parameterIndex, (InputStream) x, -1);
            } else if (x instanceof Blob) {
                setBlob(parameterIndex, (Blob) x);
            } else if (x instanceof Clob) {
                setClob(parameterIndex, (Clob) x);
            } else if (x instanceof BigInteger) {
                setString(parameterIndex, x.toString());
            } else {
                throw new SQLDataException("Can't bind object of class " + x.getClass().getCanonicalName());
            }
        }
    }

    @Override
    public boolean execute() throws SQLException {
        return super.execute(buildSql());
    }

    @Override
    public void addBatch() throws SQLException {
        batchRows.add(buildBinds());
        createBinds();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        int valuePosition = sql.toUpperCase().indexOf("VALUES");
        if (valuePosition == -1) {
            throw new SQLSyntaxErrorException(
                "Query must be like 'INSERT INTO [db.]table [(c1, c2, c3)] VALUES (?, ?, ?)'. " +
                    "Got: " + sql
            );
        }
        String insertSql = sql.substring(0, valuePosition);
        BatchHttpEntity entity = new BatchHttpEntity(batchRows);
        sendStream(entity, insertSql);

        int[] result = new int[batchRows.size()];
        Arrays.fill(result, 1);
        batchRows = new ArrayList<byte[]>();
        return result;
    }

    private static class BatchHttpEntity extends AbstractHttpEntity {
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
            for (byte[] row : rows) {
                outputStream.write(row);
            }
        }

        @Override
        public boolean isStreaming() {
            return false;
        }
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
        throw new SQLFeatureNotSupportedException();

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
