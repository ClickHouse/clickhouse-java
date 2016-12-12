package ru.yandex.clickhouse;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.except.ClickHouseExceptionSpecifier;
import ru.yandex.clickhouse.response.ClickHouseLZ4Stream;
import ru.yandex.clickhouse.response.ClickHouseResponse;
import ru.yandex.clickhouse.response.ClickHouseResultSet;
import ru.yandex.clickhouse.response.FastByteArrayOutputStream;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import ru.yandex.clickhouse.util.Patterns;
import ru.yandex.clickhouse.util.Utils;
import ru.yandex.clickhouse.util.apache.StringUtils;
import ru.yandex.clickhouse.util.guava.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class ClickHouseStatementImpl implements ClickHouseStatement {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseStatementImpl.class);

    private final CloseableHttpClient client;

    private ClickHouseProperties properties = new ClickHouseProperties();

    private ClickHouseConnection connection;

    private ClickHouseResultSet currentResult;

    private int queryTimeout;

    private int maxRows;

    private boolean closeOnCompletion;

    private ObjectMapper objectMapper;

    public ClickHouseStatementImpl(CloseableHttpClient client, ClickHouseConnection connection,
            ClickHouseProperties properties) {
        this.client = client;
        this.connection = connection;
        this.properties = properties;

        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return executeQuery(sql, null);
    }

    public ResultSet executeQuery(String sql, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        InputStream is = getInputStream(sql, additionalDBParams);
        try {
            if (isSelect(sql)) {
                currentResult = new ClickHouseResultSet(properties.isCompress()
                        ? new ClickHouseLZ4Stream(is) : is, properties.getBufferSize(),
                        extractDBName(sql),
                        extractTableName(sql),
                        this
                );
                currentResult.setMaxRows(maxRows);
                return currentResult;
            } else {
                StreamUtils.close(is);
                return null;
            }
        } catch (Exception e) {
            StreamUtils.close(is);
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    public ClickHouseResponse executeQueryClickhouseResponse(String sql) throws SQLException {
        return executeQueryClickhouseResponse(sql, null);
    }

    public ClickHouseResponse executeQueryClickhouseResponse(String sql, Map<ClickHouseQueryParam, String> additionalDBParams) throws SQLException {
        InputStream is = getInputStream(addFormatIfAbsent(sql, "JSONCompact"), additionalDBParams);
        try {
            byte[] bytes = null;
            try {
                if (properties.isCompress()) {
                    bytes = StreamUtils.toByteArray(new ClickHouseLZ4Stream(is));
                } else {
                    bytes = StreamUtils.toByteArray(is);
                }
                return objectMapper.readValue(bytes, ClickHouseResponse.class);
            } catch (IOException e) {
                if (bytes != null) {
                    log.warn("Wrong json: " + new String(bytes, StreamUtils.UTF_8));
                }
                throw e;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            StreamUtils.close(is);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        InputStream is = null;
        try {
            is = getInputStream(sql, null);
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
        return isSelect( sql );
    }

    @Override
    public void close() throws SQLException {
        if (currentResult != null) {
            currentResult.close();
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
        if (max < 0){
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
        return currentResult == null ? 0 : -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
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
    public Connection getConnection() throws SQLException {
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
                && !woSemicolon.endsWith(" JSONCompact")) {
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1);
            }
            sql += " FORMAT " + format + ';';
        }
        return sql;
    }

    private static boolean isSelect(String sql) {
        String upper = sql.toUpperCase().trim();
        return upper.startsWith("SELECT") || upper.startsWith("SHOW") || upper.startsWith("DESC") || upper.startsWith("EXISTS");
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

    private InputStream getInputStream(String sql,
                                       Map<ClickHouseQueryParam, String> additionalClickHouseDBParams
    ) throws ClickHouseException {
        sql = clickhousifySql(sql);
        log.debug("Executing SQL: " + sql);
        boolean ignoreDatabase = sql.toUpperCase().startsWith("CREATE DATABASE");
        URI uri = buildRequestUri(ignoreDatabase, additionalClickHouseDBParams);
        log.debug("Request url: " + uri);
        HttpPost post = new HttpPost(uri);
        post.setEntity(new StringEntity(sql, StreamUtils.UTF_8));
        HttpEntity entity = null;
        try {
            HttpResponse response = client.execute(post);
            entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
                String chMessage;
                try {
                    InputStream messageStream = entity.getContent();
                    if (properties.isCompress()) {
                        messageStream = new ClickHouseLZ4Stream(messageStream);
                    }
                    chMessage = StreamUtils.toString(messageStream);
                } catch (IOException e) {
                    chMessage = "error while read response " + e.getMessage();
                }
                EntityUtils.consumeQuietly(entity);
                throw ClickHouseExceptionSpecifier.specify(chMessage, properties.getHost(), properties.getPort());
            }

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

    URI buildRequestUri(boolean ignoreDatabase, Map<ClickHouseQueryParam, String> additionalClickHouseDBParams) {
        try {
            String query = buildUrlQuery(ignoreDatabase, additionalClickHouseDBParams);
            return new URI("http", null, properties.getHost(), properties.getPort(), "/", query, null);
        } catch (URISyntaxException e) {
            log.error("Mailformed URL: " + e.getMessage());
            throw new IllegalStateException("illegal configuration of db");
        }
    }

    private String buildUrlQuery(boolean ignoreDatabase, Map<ClickHouseQueryParam, String> additionalClickHouseDBParams) {
        Map<ClickHouseQueryParam, String> params = properties.buildQueryParams(ignoreDatabase);

        if (additionalClickHouseDBParams != null && !additionalClickHouseDBParams.isEmpty()) {
            params.putAll(additionalClickHouseDBParams);
        }

        setStatementPropertiesToParams(params);
        List<String> paramPairs = new ArrayList<String>();
        for (Map.Entry<ClickHouseQueryParam, String> entry : params.entrySet()) {
            if (!StringUtils.isEmpty(entry.getValue())) {
                paramPairs.add(entry.getKey().toString() + '=' + entry.getValue());
            }
        }
        return StringUtils.join(paramPairs, '&');
    }

    private void setStatementPropertiesToParams(Map<ClickHouseQueryParam, String> params) {
        if (maxRows > 0) {
            params.put(ClickHouseQueryParam.MAX_RESULT_ROWS, String.valueOf(maxRows));
            params.put(ClickHouseQueryParam.RESULT_OVERFLOW_MODE, "break");
        }
    }

    public void sendStream(InputStream content, String table) throws ClickHouseException {
        String query = "INSERT INTO " + table;
        sendStream(new InputStreamEntity(content, -1), query);
    }

    public void sendStream(HttpEntity content, String sql) throws ClickHouseException {
        // echo -ne '10\n11\n12\n' | POST 'http://localhost:8123/?query=INSERT INTO t FORMAT TabSeparated'
        HttpEntity entity = null;
        try {
            String query = buildUrlQuery(false, null);
            query += "&query=" + sql + " FORMAT TabSeparated";

            URI uri = new URI("http", null, properties.getHost(), properties.getPort(), "/", query, null);
            HttpPost httpPost = new HttpPost(uri);
            httpPost.setEntity(content);
            HttpResponse response = client.execute(httpPost);
            entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
                String chMessage;
                try {
                    chMessage = EntityUtils.toString(response.getEntity());
                } catch (IOException e) {
                    chMessage = "error while read response " + e.getMessage();
                }
                throw ClickHouseExceptionSpecifier.specify(chMessage, properties.getHost(), properties.getPort());
            }
        } catch (ClickHouseException e) {
            throw e;
        } catch (Exception e) {
            throw ClickHouseExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
    }


    public void closeOnCompletion() throws SQLException {
        closeOnCompletion = true;
    }

    public boolean isCloseOnCompletion() throws SQLException {
        return closeOnCompletion;
    }
}
