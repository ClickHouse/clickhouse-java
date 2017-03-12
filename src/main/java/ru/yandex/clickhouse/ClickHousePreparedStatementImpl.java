package ru.yandex.clickhouse;

import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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


public class ClickHousePreparedStatementImpl extends ClickHouseStatementImpl implements ClickHousePreparedStatement {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseStatementImpl.class);

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final String sql;
    private final List<String> sqlParts;
    private String[] binds;
    private boolean[] valuesQuote;
    private List<byte[]> batchRows = new ArrayList<byte[]>();

    public ClickHousePreparedStatementImpl(CloseableHttpClient client, ClickHouseConnection connection,
             ClickHouseProperties properties, String sql, TimeZone timezone) throws SQLException {
        super(client, connection, properties);
        this.sql = sql;
        this.sqlParts = parseSql(sql);
        createBinds();
        initTimeZone(timezone);
    }

    private void createBinds() {
        this.binds = new String[this.sqlParts.size() - 1];
        this.valuesQuote = new boolean[this.sqlParts.size() - 1];
    }

    private void initTimeZone(TimeZone timeZone) {
        dateTimeFormat.setTimeZone(timeZone);
        dateFormat.setTimeZone(timeZone);
    }

    @Override
    public void clearParameters() {
        Arrays.fill(binds, null);
        Arrays.fill(valuesQuote, false);
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
            appendBoundValue(sb, i - 1);
            sb.append(sqlParts.get(i));
        }
        String sql = sb.toString();

        return sql;
    }

    private void appendBoundValue(StringBuilder sb, int num) {
        if (valuesQuote[num]) {
            sb.append("'");
        }
        sb.append(binds[num]);
        if (valuesQuote[num]) {
            sb.append("'");
        }
    }

    private static void checkBinded(String[] binds) throws SQLException {
        for (String b : binds) {
            if (b == null) {
                throw new SQLException("Not all parameters binded");
            }
        }
    }

    private byte[] buildBinds() throws SQLException {
        checkBinded(binds);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < binds.length; i++) {
            sb.append(binds[i]);
            sb.append(i < binds.length - 1 ? '\t' : '\n');
        }
        return sb.toString().getBytes(StreamUtils.UTF_8);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(buildSql());
    }

    @Override
    public ResultSet executeQuery(Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return super.executeQuery(buildSql(), additionalDBParams);
    }

    @Override
    public ResultSet executeQuery(Map<ClickHouseQueryParam, String> additionalDBParams, List<ClickHouseExternalData> externalData) throws SQLException {
        return super.executeQuery(buildSql(), additionalDBParams, externalData);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(buildSql());
    }

    private void setBind(int parameterIndex, String bind) {
        setBind(parameterIndex, bind, false);
    }

    private void setBind(int parameterIndex, String bind, boolean quote) {
        binds[parameterIndex - 1] = bind;
        valuesQuote[parameterIndex - 1] = quote;
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
        setBind(parameterIndex, ClickHouseUtil.escape(x), true);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setBind(parameterIndex, new String(x, StreamUtils.UTF_8));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setBind(parameterIndex, dateFormat.format(x), true);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
        //        setBind(parameterIndex, "toDateTime('" + dateTimeFormat.format(x) + "')");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setBind(parameterIndex, dateTimeFormat.format(x), true);
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
        setBind(parameterIndex, ClickHouseArrayUtil.toString(collection));
    }

    @Override
    public void setArray(int parameterIndex, Object[] array) throws SQLException {
        setBind(parameterIndex, ClickHouseArrayUtil.toString(array));
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
                setBind(parameterIndex, x.toString());
            } else if (x instanceof Collection) {
                setBind(parameterIndex, ClickHouseArrayUtil.toString((Collection) x));
            } else if (x.getClass().isArray()) {
                setBind(parameterIndex, ClickHouseArrayUtil.arrayToString(x));
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
        setBind(parameterIndex, ClickHouseArrayUtil.arrayToString(x.getArray()));
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
