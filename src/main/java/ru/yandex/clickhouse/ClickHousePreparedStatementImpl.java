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
import java.sql.Date;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ClickHousePreparedStatementImpl extends ClickHouseStatementImpl implements ClickHousePreparedStatement {

    static final String PARAM_MARKER = "?";

    private static final Pattern VALUES = Pattern.compile("(?i)VALUES[\\s]*\\(");

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final String sql;
    private final List<String> sqlParts;
    private final String[] binds;
    private final List<List<String>> parameterList;
    private final boolean insertBatchMode;
    private final boolean[] valuesQuote;
    private List<byte[]> batchRows = new ArrayList<byte[]>();


    public ClickHousePreparedStatementImpl(CloseableHttpClient client, ClickHouseConnection connection,
                                           ClickHouseProperties properties, String sql, TimeZone timezone, int resultSetType) throws SQLException {
        super(client, connection, properties, resultSetType);
        this.sql = sql;
        PreparedStatementParser parser = PreparedStatementParser.parse(sql);
        this.parameterList = parser.getParameters();
        this.insertBatchMode = parser.isValuesMode();
        this.sqlParts = parser.getParts();
        int numParams = countNonConstantParams();
        this.binds = new String[numParams];
        this.valuesQuote = new boolean[numParams];
        initTimeZone(timezone);
    }

    private void initTimeZone(TimeZone timeZone) {
        dateTimeFormat.setTimeZone(timeZone);
        if (properties.isUseServerTimeZoneForDates()) {
            dateFormat.setTimeZone(timeZone);
        }
    }

    @Override
    public void clearParameters() {
        Arrays.fill(binds, null);
        Arrays.fill(valuesQuote, false);
    }

    @Override
    public ClickHouseResponse executeQueryClickhouseResponse() throws SQLException {
        return super.executeQueryClickhouseResponse(buildSql());
    }

    @Override
    public ClickHouseResponse executeQueryClickhouseResponse(Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return super.executeQueryClickhouseResponse(buildSql(), additionalDBParams);
    }

    private String buildSql() throws SQLException {
        if (sqlParts.size() == 1) {
            return sqlParts.get(0);
        }

        checkBinded();
        StringBuilder sb = new StringBuilder(sqlParts.get(0));
        for (int i = 1, p = 0; i < sqlParts.size(); i++) {
            String pValue = getParameter(i - 1);
            if (PARAM_MARKER.equals(pValue)) {
                appendBoundValue(sb, p++);
            } else {
                sb.append(pValue);
            }
            sb.append(sqlParts.get(i));
        }
        String mySql = sb.toString();
        return mySql;
    }

    private void appendBoundValue(StringBuilder sb, int num) {
        if (valuesQuote[num]) {
            sb.append("'").append(binds[num]).append("'");
        } else if (binds[num].equals("\\N")) {
            sb.append("null");
        } else {
            sb.append(binds[num]);
        }
    }

    private void checkBinded() throws SQLException {
        int i = 0;
        for (String b : binds) {
            ++i;
            if (b == null) {
                throw new SQLException("Not all parameters binded (placeholder " + i + " is undefined)");
            }
        }
    }

    @Override
    public boolean execute() throws SQLException {
        return super.execute(buildSql());
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
        setBind(parameterIndex, "\\N");
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
        setBind(parameterIndex, ClickHouseUtil.escape(x), x != null);
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
        setBind(parameterIndex, dateTimeFormat.format(x), true);
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
            } else if (x instanceof UUID) {
                setString(parameterIndex, x.toString());
            } else if (x instanceof Collection) {
                setArray(parameterIndex, (Collection) x);
            } else if (x.getClass().isArray()) {
                setArray(parameterIndex, (Object[]) x);
            } else {
                throw new SQLDataException("Can't bind object of class " + x.getClass().getCanonicalName());
            }
        }
    }

    @Override
    public void addBatch() throws SQLException {
        batchRows.addAll(buildBatch());
    }

    private List<byte[]> buildBatch() throws SQLException {
        checkBinded();
        List<byte[]> newBatches = new ArrayList<byte[]>(parameterList.size());
        StringBuilder sb = new StringBuilder();
        for (int i = 0, p = 0; i < parameterList.size(); i++) {
            List<String> pList = parameterList.get(i);
            for (int j = 0; j < pList.size(); j++) {
                String pValue = pList.get(j);
                if (PARAM_MARKER.equals(pValue)) {
                    if (insertBatchMode) {
                        sb.append(binds[p++]);
                    } else {
                        appendBoundValue(sb, p++);
                    }
                } else {
                    sb.append(pValue);
                }
                sb.append(j < pList.size() - 1 ? "\t" : "\n");
            }
            newBatches.add(sb.toString().getBytes(StreamUtils.UTF_8));
            sb = new StringBuilder();
        }
        return newBatches;
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return executeBatch(null);
    }

    @Override
    public int[] executeBatch(Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        Matcher matcher = VALUES.matcher(sql);
        if (!matcher.find()) {
            throw new SQLSyntaxErrorException(
                    "Query must be like 'INSERT INTO [db.]table [(c1, c2, c3)] VALUES (?, ?, ?)'. " +
                            "Got: " + sql
            );
        }
        int valuePosition = matcher.start();
        String insertSql = sql.substring(0, valuePosition);
        BatchHttpEntity entity = new BatchHttpEntity(batchRows);
        sendStream(entity, insertSql, additionalDBParams);
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
        setBind(parameterIndex, ClickHouseArrayUtil.arrayToString(x.getArray(), x.getBaseType() != Types.BINARY));
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

    private int countNonConstantParams() {
        int count = 0;
        for (int i = 0; i < parameterList.size(); i++) {
            List<String> pList = parameterList.get(i);
            for (int j = 0; j < pList.size(); j++) {
                if (PARAM_MARKER.equals(pList.get(j))) {
                    count += 1;
                }
            }
        }
        return count;
    }

    private String getParameter(int paramIndex) {
        for (int i = 0, count = paramIndex; i < parameterList.size(); i++) {
            List<String> pList = parameterList.get(i);
            count -= pList.size();
            if (count < 0) {
                return pList.get(pList.size() + count);
            }
        }
        return null;
    }

}
