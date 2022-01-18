package ru.yandex.clickhouse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.clickhouse.jdbc.parser.ClickHouseSqlStatement;
import ru.yandex.clickhouse.jdbc.parser.StatementType;
import ru.yandex.clickhouse.response.ClickHouseResponse;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import ru.yandex.clickhouse.util.ClickHouseArrayUtil;
import ru.yandex.clickhouse.util.ClickHouseValueFormatter;

public class ClickHousePreparedStatementImpl extends ClickHouseStatementImpl implements ClickHousePreparedStatement {
    private static final Logger log = LoggerFactory.getLogger(ClickHousePreparedStatementImpl.class);

    static final String PARAM_MARKER = "?";
    static final String NULL_MARKER = "\\N";
    static final int FIRST_PARAM_INDEX = 0;
    private final TimeZone dateTimeZone;
    private final TimeZone dateTimeTimeZone;
    private final ClickHouseSqlStatement parsedStmt;
    private final List<String> sqlParts;
    private final ClickHousePreparedStatementParameter[] binds;
    private final List<List<String>> parameterList;
    private final boolean insertBatchMode;
    private List<byte[]> batchRows = new ArrayList<>();

    public ClickHousePreparedStatementImpl(CloseableHttpClient client,
        ClickHouseConnection connection, ClickHouseProperties properties, String sql,
        TimeZone serverTimeZone, int resultSetType) throws SQLException
    {
        super(client, connection, properties, resultSetType);
        parseSqlStatements(sql);

        if (parsedStmts.length != 1) {
            throw new IllegalArgumentException("Only single statement is supported");
        }

        parsedStmt = parsedStmts[0];

        PreparedStatementParser parser = PreparedStatementParser.parse(sql,
            parsedStmt.getEndPosition(ClickHouseSqlStatement.KEYWORD_VALUES));
        this.parameterList = parser.getParameters();
        this.insertBatchMode = parser.isValuesMode();
        this.sqlParts = parser.getParts();
        int numParams = countNonConstantParams();
        this.binds = new ClickHousePreparedStatementParameter[numParams];
        dateTimeTimeZone = serverTimeZone;
        if (properties.isUseServerTimeZoneForDates()) {
            dateTimeZone = serverTimeZone;
        } else {
            dateTimeZone = TimeZone.getDefault();
        }
    }

    @Override
    public void clearParameters() {
        Arrays.fill(binds, null);
    }

    @Override
    public ClickHouseResponse executeQueryClickhouseResponse() throws SQLException {
        return executeQueryClickhouseResponse(buildJdbcSql(), null, buildRequestParams());
    }

    @Override
    public ClickHouseResponse executeQueryClickhouseResponse(Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return executeQueryClickhouseResponse(buildJdbcSql(), additionalDBParams, buildRequestParams());
    }

    private ClickHouseSqlStatement buildSql() throws SQLException {
        if (sqlParts.size() == 1) {
            return new ClickHouseSqlStatement(sqlParts.get(0), parsedStmt.getStatementType());
        }

        checkBinded();
        StringBuilder sb = new StringBuilder(sqlParts.get(0));
        for (int i = 1, p = 0; i < sqlParts.size(); i++) {
            String pValue = getParameter(i - 1);
            if (PARAM_MARKER.equals(pValue)) {
                sb.append(binds[p++].getRegularValue());
            } else if (NULL_MARKER.equals(pValue)) {
                sb.append("NULL");
            } else {
                sb.append(pValue);
            }
            sb.append(sqlParts.get(i));
        }
        return new ClickHouseSqlStatement(sb.toString(), parsedStmt.getStatementType());
    }

    private ClickHouseSqlStatement buildJdbcSql() throws SQLException {
        if (sqlParts.size() == 1) {
            return new ClickHouseSqlStatement(sqlParts.get(0), parsedStmt.getStatementType());
        }

        checkBinded();
        StringBuilder sb = new StringBuilder(sqlParts.get(0));
        for (int i = 1, p = 0, paramIndex = FIRST_PARAM_INDEX; i < sqlParts.size(); i++) {
            String pValue = getParameter(i - 1);
            if (PARAM_MARKER.equals(pValue)) {
                sb.append(binds[p++].getRegularParam(paramIndex));
                paramIndex++;
            } else if (NULL_MARKER.equals(pValue)) {
                sb.append("NULL");
            } else {
                sb.append(pValue);
            }
            sb.append(sqlParts.get(i));
        }
        return new ClickHouseSqlStatement(sb.toString(), parsedStmt.getStatementType());
    }

    private Map<String, String> buildRequestParams() {
        Map<String, String> additionalRequestParams = null;
        if (binds.length > 0) {
            additionalRequestParams = new HashMap<>();
            for (int paramIndex = FIRST_PARAM_INDEX; paramIndex < binds.length; paramIndex++) {
                additionalRequestParams.put("param_param" + paramIndex, binds[paramIndex].getBatchValue());
            }
        }
        return additionalRequestParams;
    }

    private void checkBinded() throws SQLException {
        int i = 0;
        for (Object b : binds) {
            ++i;
            if (b == null) {
                throw new SQLException("Not all parameters binded (placeholder " + i + " is undefined)");
            }
        }
    }

    @Override
    public boolean execute() throws SQLException {
        return executeQueryStatement(buildJdbcSql(), null, null, buildRequestParams()) != null;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQueryStatement(buildJdbcSql(), null, null, buildRequestParams());
    }

    @Override
    public void clearBatch() throws SQLException {
        super.clearBatch();

        batchRows = new ArrayList<>();
    }

    @Override
    public ResultSet executeQuery(Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return executeQuery(additionalDBParams, null);
    }

    @Override
    public ResultSet executeQuery(Map<ClickHouseQueryParam, String> additionalDBParams, List<ClickHouseExternalData> externalData) throws SQLException {
        return executeQueryStatement(buildJdbcSql(), additionalDBParams, externalData, buildRequestParams());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return executeStatement(buildJdbcSql(), null, null, buildRequestParams());
    }

    private void setBind(int parameterIndex, String bind, boolean quote) {
        binds[parameterIndex - 1] = new ClickHousePreparedStatementParameter(bind, quote);
    }

    private void setBind(int parameterIndex, ClickHousePreparedStatementParameter parameter) {
        binds[parameterIndex -1] = parameter;
    }

    private void setNull(int parameterIndex) {
        setBind(parameterIndex, ClickHousePreparedStatementParameter.nullParameter());
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setNull(parameterIndex);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setBind(parameterIndex, ClickHousePreparedStatementParameter.boolParameter(x));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatByte(x), false);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatShort(x), false);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatInt(x), false);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatLong(x), false);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatFloat(x), false);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatDouble(x), false);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatBigDecimal(x), false);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatString(x), x != null);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setBind(parameterIndex, ClickHouseValueFormatter.formatBytes(x), true);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (x != null) {
            setBind(
                parameterIndex,
                ClickHouseValueFormatter.formatDate(x, dateTimeZone),
                true);
        } else {
            setNull(parameterIndex);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (x != null) {
            setBind(
                parameterIndex,
                ClickHouseValueFormatter.formatTime(x, dateTimeTimeZone),
                true);
        } else {
            setNull(parameterIndex);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (x != null) {
            setBind(
                parameterIndex,
                ClickHouseValueFormatter.formatTimestamp(x, dateTimeTimeZone),
                true);
        } else {
            setNull(parameterIndex);
        }
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
        setBind(parameterIndex, ClickHouseArrayUtil.toString(collection, dateTimeZone, dateTimeTimeZone),
            false);
    }

    @Override
    public void setArray(int parameterIndex, Object[] array) throws SQLException {
        setBind(parameterIndex, ClickHouseArrayUtil.toString(array, dateTimeZone, dateTimeTimeZone), false);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        setBind(parameterIndex, ClickHouseArrayUtil.arrayToString(x.getArray(), dateTimeZone, dateTimeTimeZone),
            false);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x != null) {
            setBind(
                parameterIndex,
                ClickHousePreparedStatementParameter.fromObject(
                    x, dateTimeZone, dateTimeTimeZone));
        } else {
            setNull(parameterIndex);
        }
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLException("addBatch(String) cannot be called in PreparedStatement or CallableStatement!");
    }

    @Override
    public void addBatch() throws SQLException {
        if (parsedStmt.getStatementType() == StatementType.INSERT && parsedStmt.hasValues()) {
            batchRows.addAll(buildBatch());
        } else {
            batchStmts.add(buildSql());
        }
    }

    private List<byte[]> buildBatch() throws SQLException {
        checkBinded();
        List<byte[]> newBatches = new ArrayList<>(parameterList.size());
        StringBuilder sb = new StringBuilder();
        for (int i = 0, p = 0; i < parameterList.size(); i++) {
            List<String> pList = parameterList.get(i);
            for (int j = 0; j < pList.size(); j++) {
                String pValue = pList.get(j);
                if (PARAM_MARKER.equals(pValue)) {
                    if (insertBatchMode) {
                        sb.append(binds[p++].getBatchValue());
                    } else {
                        sb.append(binds[p++].getRegularValue());
                    }
                } else {
                    sb.append(pValue);
                }
                sb.append(j < pList.size() - 1 ? "\t" : "\n");
            }
            newBatches.add(sb.toString().getBytes(StandardCharsets.UTF_8));
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
        int valuePosition = -1;
        String sql = parsedStmt.getSQL();
        StatementType type = parsedStmt.getStatementType();
        if (type == StatementType.INSERT && parsedStmt.hasValues()) {
            valuePosition = parsedStmt.getStartPosition(ClickHouseSqlStatement.KEYWORD_VALUES);
        }

        int[] result = new int[batchRows.size()];
        Arrays.fill(result, 1);
        if (valuePosition > 0) { // insert
            String insertSql = sql.substring(0, valuePosition);
            BatchHttpEntity entity = new BatchHttpEntity(batchRows);
            sendStream(entity, insertSql, additionalDBParams);        
        } else { // others
            if (type == StatementType.ALTER_DELETE || type == StatementType.ALTER_UPDATE) {
                log.warn("UPDATE and DELETE should be used with caution, as they are expensive operations and not supposed to be used frequently.");
            } else {
                log.warn("PreparedStatement will slow down non-INSERT queries, so please consider to use Statement instead.");
            }
            result = super.executeBatch();
        }

        clearBatch();
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
    public ResultSetMetaData getMetaData() throws SQLException {
        ResultSet currentResult = getResultSet();
        if (currentResult != null) {
            return currentResult.getMetaData();
        }
        
        if (!parsedStmt.isQuery()) {
            return null;
        }
        ResultSet myRs = executeQuery(Collections.singletonMap(
            ClickHouseQueryParam.MAX_RESULT_ROWS, "0"));
        return myRs != null ? myRs.getMetaData() : null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        if (x != null && cal != null && cal.getTimeZone() != null) {
            setBind(
                parameterIndex,
                ClickHouseValueFormatter.formatDate(x, cal.getTimeZone()),
                true);
        } else {
            setDate(parameterIndex, x);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        if (x != null && cal != null && cal.getTimeZone() != null) {
            setBind(
                parameterIndex,
                ClickHouseValueFormatter.formatTime(x, cal.getTimeZone()),
                true);
        } else {
            setTime(parameterIndex, x);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        if (x != null && cal != null && cal.getTimeZone() != null) {
            setBind(
                parameterIndex,
                ClickHouseValueFormatter.formatTimestamp(x, cal.getTimeZone()),
                true);
        } else {
            setTimestamp(parameterIndex, x);
        }
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

    @Override
    public String asSql() {
        try {
            return buildSql().getSQL();
        } catch (SQLException e) {
            return parsedStmt.getSQL();
        }
    }
}
