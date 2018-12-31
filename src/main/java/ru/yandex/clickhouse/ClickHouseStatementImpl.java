package ru.yandex.clickhouse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.except.ClickHouseExceptionSpecifier;
import ru.yandex.clickhouse.response.ClickHouseLZ4Stream;
import ru.yandex.clickhouse.response.ClickHouseResponse;
import ru.yandex.clickhouse.response.ClickHouseResultSet;
import ru.yandex.clickhouse.response.ClickHouseScrollableResultSet;
import ru.yandex.clickhouse.response.FastByteArrayOutputStream;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import ru.yandex.clickhouse.util.ClickHouseFormat;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryInputStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;
import ru.yandex.clickhouse.util.ClickHouseStreamHttpEntity;
import ru.yandex.clickhouse.util.Patterns;
import ru.yandex.clickhouse.util.Utils;
import ru.yandex.clickhouse.util.guava.StreamUtils;


public class ClickHouseStatementImpl implements ClickHouseStatement {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseStatementImpl.class);

    private final CloseableHttpClient client;

    protected ClickHouseProperties properties = new ClickHouseProperties();

    private ClickHouseConnection connection;

    private ClickHouseResultSet currentResult;

    private ClickHouseRowBinaryInputStream currentRowBinaryResult;

    private int currentUpdateCount = -1;

    private int queryTimeout;

    private int maxRows;

    private boolean closeOnCompletion;
    
    private final boolean isResultSetScrollable;

    /**
     * Current database name may be changed by {@link java.sql.Connection#setCatalog(String)}
     * between creation of this object and query execution, but javadoc does not allow
     * {@code setCatalog} influence on already created statements.
     */
    private final String initialDatabase;

    private static final String[] selectKeywords = new String[]{"SELECT", "WITH", "SHOW", "DESC", "EXISTS"};
    private static final String databaseKeyword = "CREATE DATABASE";

    public ClickHouseStatementImpl(CloseableHttpClient client, ClickHouseConnection connection,
                                   ClickHouseProperties properties, int resultSetType) {
        this.client = client;
        this.connection = connection;
        this.properties = properties;
        this.initialDatabase = properties.getDatabase();
        this.isResultSetScrollable = (resultSetType != ResultSet.TYPE_FORWARD_ONLY);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return executeQuery(sql, null);
    }

    @Override
    public ResultSet executeQuery(String sql, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return executeQuery(sql, additionalDBParams, null);
    }

    @Override
    public ResultSet executeQuery(String sql, Map<ClickHouseQueryParam, String> additionalDBParams, List<ClickHouseExternalData> externalData) throws SQLException {
        return executeQuery(sql, additionalDBParams, externalData, null);
    }

    @Override
    public ResultSet executeQuery(String sql,
                                  Map<ClickHouseQueryParam, String> additionalDBParams,
                                  List<ClickHouseExternalData> externalData,
                                  Map<String, String> additionalRequestParams) throws SQLException {

        // forcibly disable extremes for ResultSet queries
        if (additionalDBParams == null) {
            additionalDBParams = new HashMap<ClickHouseQueryParam, String>();
        } else {
            additionalDBParams = new HashMap<ClickHouseQueryParam, String>(additionalDBParams);
        }
        additionalDBParams.put(ClickHouseQueryParam.EXTREMES, "0");

        InputStream is = getInputStream(sql, additionalDBParams, externalData, additionalRequestParams);

        try {
            if (isSelect(sql)) {
                currentUpdateCount = -1;
                currentResult = createResultSet(properties.isCompress()
                    ? new ClickHouseLZ4Stream(is) : is, properties.getBufferSize(),
                    extractDBName(sql),
                    extractTableName(sql),
                    extractWithTotals(sql),
                    this,
                    getConnection().getTimeZone(),
                    properties
                );
                currentResult.setMaxRows(maxRows);
                return currentResult;
            } else {
                currentUpdateCount = 0;
                StreamUtils.close(is);
                return null;
            }
        } catch (Exception e) {
            StreamUtils.close(is);
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public ClickHouseResponse executeQueryClickhouseResponse(String sql) throws SQLException {
        return executeQueryClickhouseResponse(sql, null);
    }

    @Override
    public ClickHouseResponse executeQueryClickhouseResponse(String sql, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return executeQueryClickhouseResponse(sql, additionalDBParams, null);
    }

    @Override
    public ClickHouseResponse executeQueryClickhouseResponse(String sql,
                                                             Map<ClickHouseQueryParam, String> additionalDBParams,
                                                             Map<String, String> additionalRequestParams) throws SQLException {
        InputStream is = getInputStream(
                addFormatIfAbsent(sql, "JSONCompact"),
                additionalDBParams,
                null,
                additionalRequestParams
        );
        try {
            if (properties.isCompress()) {
                is = new ClickHouseLZ4Stream(is);
            }
            return Jackson.getObjectMapper().readValue(is, ClickHouseResponse.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            StreamUtils.close(is);
        }
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(String sql) throws SQLException {
        return executeQueryClickhouseRowBinaryStream(sql, null);
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(String sql, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        return executeQueryClickhouseRowBinaryStream(sql, additionalDBParams, null);
    }

    @Override
    public ClickHouseRowBinaryInputStream executeQueryClickhouseRowBinaryStream(String sql, Map<ClickHouseQueryParam, String> additionalDBParams, Map<String, String> additionalRequestParams) throws SQLException {
        InputStream is = getInputStream(
                addFormatIfAbsent(sql, "RowBinary"),
                additionalDBParams,
                null,
                additionalRequestParams
        );
        try {
            if (isSelect(sql)) {
                currentUpdateCount = -1;
                currentRowBinaryResult = new ClickHouseRowBinaryInputStream(properties.isCompress()
                        ? new ClickHouseLZ4Stream(is) : is, getConnection().getTimeZone(), properties);
                return currentRowBinaryResult;
            } else {
                currentUpdateCount = 0;
                StreamUtils.close(is);
                return null;
            }
        } catch (Exception e) {
            StreamUtils.close(is);
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        InputStream is = null;
        try {
            is = getInputStream(sql, null, null, null);
            //noinspection StatementWithEmptyBody
        } finally {
            StreamUtils.close(is);
        }
        return 1;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        // currentResult is stored here. InputString and currentResult will be closed on this.close()
        executeQuery(sql);
        return isSelect(sql);
    }

    @Override
    public void close() throws SQLException {
        if (currentResult != null) {
            currentResult.close();
        }

        if (currentRowBinaryResult != null) {
            StreamUtils.close(currentRowBinaryResult);
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw new SQLException(String.format("Illegal maxRows value: %d", max));
        }
        maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return currentResult;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return currentUpdateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if (currentResult != null) {
            currentResult.close();
            currentResult = null;
        }
        currentUpdateCount = -1;
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public ClickHouseConnection getConnection() throws ClickHouseException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    static String clickhousifySql(String sql) {

        return addFormatIfAbsent(sql, "TabSeparatedWithNamesAndTypes");
    }

    /**
     * Adding  FORMAT TabSeparatedWithNamesAndTypes if not added
     * adds format only to select queries
     */
    private static String addFormatIfAbsent(String sql, String format) {
        sql = sql.trim();
        String woSemicolon = Patterns.SEMICOLON.matcher(sql).replaceAll("").trim();
        if (isSelect(sql)
            && !woSemicolon.endsWith(" TabSeparatedWithNamesAndTypes")
            && !woSemicolon.endsWith(" TabSeparated")
            && !woSemicolon.endsWith(" JSONCompact")
            && !woSemicolon.endsWith(" RowBinary")) {
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1);
            }
            sql += " FORMAT " + format + ';';
        }
        return sql;
    }

    static boolean isSelect(String sql) {
        for (int i = 0; i < sql.length(); i++) {
            String nextTwo = sql.substring(i, Math.min(i + 2, sql.length()));
            if ("--".equals(nextTwo)) {
                i = Math.max(i, sql.indexOf("\n", i));
            } else if ("/*".equals(nextTwo)) {
                i = Math.max(i, sql.indexOf("*/", i));
            } else if (Character.isLetter(sql.charAt(i))) {
                String trimmed = sql.substring(i, Math.min(sql.length(), Math.max(i, sql.indexOf(" ", i))));
                for (String keyword : selectKeywords){
                    if (trimmed.regionMatches(true, 0, keyword, 0, keyword.length())) {
                        return true;
                    }
                }
                return false;
            }
        }
        return false;
    }

    private String extractTableName(String sql) {
        String s = extractDBAndTableName(sql);
        if (s.contains(".")) {
            return s.substring(s.indexOf(".") + 1);
        } else {
            return s;
        }
    }

    private String extractDBName(String sql) {
        String s = extractDBAndTableName(sql);
        if (s.contains(".")) {
            return s.substring(0, s.indexOf("."));
        } else {
            return properties.getDatabase();
        }
    }

    private String extractDBAndTableName(String sql) {
        if (Utils.startsWithIgnoreCase(sql, "select")) {
            String withoutStrings = Utils.retainUnquoted(sql, '\'');
            int fromIndex = withoutStrings.indexOf("from");
            if (fromIndex == -1) {
                fromIndex = withoutStrings.indexOf("FROM");
            }
            if (fromIndex != -1) {
                String fromFrom = withoutStrings.substring(fromIndex);
                String fromTable = fromFrom.substring("from".length()).trim();
                return fromTable.split(" ")[0];
            }
        }
        if (Utils.startsWithIgnoreCase(sql, "desc")) {
            return "system.columns";
        }
        if (Utils.startsWithIgnoreCase(sql, "show")) {
            return "system.tables";
        }
        return "system.unknown";
    }

    private boolean extractWithTotals(String sql) {
        if (Utils.startsWithIgnoreCase(sql, "select")) {
            String withoutStrings = Utils.retainUnquoted(sql, '\'');
            return withoutStrings.toLowerCase().contains(" with totals");
        }
        return false;
    }

    private InputStream getInputStream(
        String sql,
        Map<ClickHouseQueryParam, String> additionalClickHouseDBParams,
        List<ClickHouseExternalData> externalData,
        Map<String, String> additionalRequestParams
    ) throws ClickHouseException {
        sql = clickhousifySql(sql);
        log.debug("Executing SQL: " + sql);

        boolean ignoreDatabase = sql.trim().regionMatches(true, 0, databaseKeyword, 0, databaseKeyword.length());
        URI uri;
        if (externalData == null || externalData.isEmpty()) {
            uri = buildRequestUri(
                    null,
                    null,
                    additionalClickHouseDBParams,
                    additionalRequestParams,
                    ignoreDatabase
            );
        } else {
            // write sql in query params when there is external data
            // as it is impossible to pass both external data and sql in body
            // TODO move sql to request body when it is supported in clickhouse
            uri = buildRequestUri(
                    sql,
                    externalData,
                    additionalClickHouseDBParams,
                    additionalRequestParams,
                    ignoreDatabase
            );
        }
        log.debug("Request url: " + uri);

        HttpPost post = new HttpPost(uri);

        HttpEntity requestEntity;
        if (externalData == null || externalData.isEmpty()) {
            requestEntity = new StringEntity(sql, StreamUtils.UTF_8);
        } else {
            MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();

            try {
                for (ClickHouseExternalData externalDataItem : externalData) {
                    // clickhouse may return 400 (bad request) when chunked encoding is used with multipart request
                    // so read content to byte array to avoid chunked encoding
                    // TODO do not read stream into memory when this issue is fixed in clickhouse
                    entityBuilder.addBinaryBody(
                        externalDataItem.getName(),
                        StreamUtils.toByteArray(externalDataItem.getContent()),
                        ContentType.APPLICATION_OCTET_STREAM,
                        externalDataItem.getName()
                    );
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            requestEntity = entityBuilder.build();
        }

        if (properties.isDecompress()) {
            requestEntity = new LZ4EntityWrapper(requestEntity, properties.getMaxCompressBufferSize());
        }

        post.setEntity(requestEntity);

        HttpEntity entity = null;
        try {
            HttpResponse response = client.execute(post);
            entity = response.getEntity();
            checkForErrorAndThrow(entity, response);

            InputStream is;
            if (entity.isStreaming()) {
                is = entity.getContent();
            } else {
                FastByteArrayOutputStream baos = new FastByteArrayOutputStream();
                entity.writeTo(baos);
                is = baos.convertToInputStream();
            }
            return is;
        } catch (ClickHouseException e) {
            throw e;
        } catch (Exception e) {
            log.info("Error during connection to " + properties + ", reporting failure to data source, message: " + e.getMessage());
            EntityUtils.consumeQuietly(entity);
            log.info("Error sql: " + sql);
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    URI buildRequestUri(
        String sql,
        List<ClickHouseExternalData> externalData,
        Map<ClickHouseQueryParam, String> additionalClickHouseDBParams,
        Map<String, String> additionalRequestParams,
        boolean ignoreDatabase
    ) {
        try {
            List<NameValuePair> queryParams = getUrlQueryParams(
                sql,
                externalData,
                additionalClickHouseDBParams,
                additionalRequestParams,
                ignoreDatabase
            );

            return new URIBuilder()
                .setScheme(properties.getSsl() ? "https" : "http")
                .setHost(properties.getHost())
                .setPort(properties.getPort())
                .setPath("/")
                .setParameters(queryParams)
                .build();
        } catch (URISyntaxException e) {
            log.error("Mailformed URL: " + e.getMessage());
            throw new IllegalStateException("illegal configuration of db");
        }
    }

    private List<NameValuePair> getUrlQueryParams(
        String sql,
        List<ClickHouseExternalData> externalData,
        Map<ClickHouseQueryParam, String> additionalClickHouseDBParams,
        Map<String, String> additionalRequestParams,
        boolean ignoreDatabase
    ) {
        List<NameValuePair> result = new ArrayList<NameValuePair>();

        if (sql != null) {
            result.add(new BasicNameValuePair("query", sql));
        }

        if (externalData != null) {
            for (ClickHouseExternalData externalDataItem : externalData) {
                String name = externalDataItem.getName();
                String format = externalDataItem.getFormat();
                String types = externalDataItem.getTypes();
                String structure = externalDataItem.getStructure();

                if (format != null && !format.isEmpty()) {
                    result.add(new BasicNameValuePair(name + "_format", format));
                }
                if (types != null && !types.isEmpty()) {
                    result.add(new BasicNameValuePair(name + "_types", types));
                }
                if (structure != null && !structure.isEmpty()) {
                    result.add(new BasicNameValuePair(name + "_structure", structure));
                }
            }
        }

        Map<ClickHouseQueryParam, String> params = properties.buildQueryParams(true);
        if (!ignoreDatabase) {
            params.put(ClickHouseQueryParam.DATABASE, initialDatabase);
        }

        if (additionalClickHouseDBParams != null && !additionalClickHouseDBParams.isEmpty()) {
            params.putAll(additionalClickHouseDBParams);
        }

        setStatementPropertiesToParams(params);

        for (Map.Entry<ClickHouseQueryParam, String> entry : params.entrySet()) {
            if (!Strings.isNullOrEmpty(entry.getValue())) {
                result.add(new BasicNameValuePair(entry.getKey().toString(), entry.getValue()));
            }
        }

        if (additionalRequestParams != null) {
            for (Map.Entry<String, String> entry : additionalRequestParams.entrySet()) {
                if (!Strings.isNullOrEmpty(entry.getValue())) {
                    result.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
                }
            }
        }


        return result;
    }

    private void setStatementPropertiesToParams(Map<ClickHouseQueryParam, String> params) {
        if (maxRows > 0) {
            params.put(ClickHouseQueryParam.MAX_RESULT_ROWS, String.valueOf(maxRows));
            params.put(ClickHouseQueryParam.RESULT_OVERFLOW_MODE, "break");
        }
        if(queryTimeout > 0) {
            params.put(ClickHouseQueryParam.MAX_EXECUTION_TIME, String.valueOf(queryTimeout));
        }
    }


    @Override
    public void sendRowBinaryStream(String sql, ClickHouseStreamCallback callback) throws SQLException {
        sendRowBinaryStream(sql, null, callback);
    }

    @Override
    public void sendRowBinaryStream(String sql, Map<ClickHouseQueryParam, String> additionalDBParams, ClickHouseStreamCallback callback) throws SQLException {
        sendStream(
                new ClickHouseStreamHttpEntity(callback, getConnection().getTimeZone(), properties), sql, ClickHouseFormat.RowBinary, additionalDBParams
        );
    }

    @Override
    public void sendNativeStream(String sql, ClickHouseStreamCallback callback) throws SQLException {
        sendNativeStream(sql, null, callback);
    }

    @Override
    public void sendNativeStream(String sql, Map<ClickHouseQueryParam, String> additionalDBParams, ClickHouseStreamCallback callback) throws SQLException {
        sendStream(
                new ClickHouseStreamHttpEntity(callback, getConnection().getTimeZone(), properties), sql, ClickHouseFormat.Native, additionalDBParams
        );
    }

    @Override
    public void sendStream(InputStream content, String table) throws ClickHouseException {
        sendStream(content, table, null);
    }

    @Override
    public void sendStream(InputStream content, String table, Map<ClickHouseQueryParam, String> additionalDBParams) throws ClickHouseException {
        String query = "INSERT INTO " + table;
        sendStream(new InputStreamEntity(content, -1), query, null, additionalDBParams);
    }

    public void sendStream(HttpEntity content, String sql) throws ClickHouseException {
        sendStream(content, sql, ClickHouseFormat.TabSeparated, null);
    }

    public void sendStream(HttpEntity content, String sql, Map<ClickHouseQueryParam, String> additionalDBParams) throws ClickHouseException {
        sendStream(content, sql, ClickHouseFormat.TabSeparated, additionalDBParams);
    }

    private void sendStream(HttpEntity content, String sql, ClickHouseFormat format,
                            Map<ClickHouseQueryParam, String> additionalDBParams) throws ClickHouseException {
        // echo -ne '10\n11\n12\n' | POST 'http://localhost:8123/?query=INSERT INTO t FORMAT TabSeparated'
        HttpEntity entity = null;
        try {
            URI uri = buildRequestUri(null, null, additionalDBParams, null, false);
            HttpEntity requestEntity = new BodyEntityWrapper(sql + " FORMAT " + format.name(), content);

            HttpPost httpPost = new HttpPost(uri);
            if (properties.isDecompress()) {
                requestEntity = new LZ4EntityWrapper(requestEntity, properties.getMaxCompressBufferSize());
            }
            httpPost.setEntity(requestEntity);
            HttpResponse response = client.execute(httpPost);
            entity = response.getEntity();
            checkForErrorAndThrow(entity, response);
        } catch (ClickHouseException e) {
            throw e;
        } catch (Exception e) {
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
    }

    private void checkForErrorAndThrow(HttpEntity entity, HttpResponse response) throws IOException, ClickHouseException {
        if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
            InputStream messageStream = entity.getContent();
            byte[] bytes = StreamUtils.toByteArray(messageStream);
            if (properties.isCompress()) {
                try {
                    messageStream = new ClickHouseLZ4Stream(new ByteArrayInputStream(bytes));
                    bytes = StreamUtils.toByteArray(messageStream);
                } catch (IOException e) {
                    log.warn("error while read compressed stream" + e.getMessage());
                }
            }
            EntityUtils.consumeQuietly(entity);
            String chMessage = new String(bytes, StreamUtils.UTF_8);
            throw ClickHouseExceptionSpecifier.specify(chMessage, properties.getHost(), properties.getPort());
        }
    }

    public void closeOnCompletion() throws SQLException {
        closeOnCompletion = true;
    }

    public boolean isCloseOnCompletion() throws SQLException {
        return closeOnCompletion;
    }
    
    private ClickHouseResultSet createResultSet(InputStream is, int bufferSize, String db, String table, boolean usesWithTotals, 
    		ClickHouseStatement statement, TimeZone timezone, ClickHouseProperties properties) throws IOException {
    	if(isResultSetScrollable) {
    		return new ClickHouseScrollableResultSet(is, bufferSize, db, table, usesWithTotals, statement, timezone, properties);
    	} else {
    		return new ClickHouseResultSet(is, bufferSize, db, table, usesWithTotals, statement, timezone, properties);
    	}
    }
}
