package ru.yandex.metrika.clickhouse;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import ru.yandex.metrika.clickhouse.copypaste.*;
import ru.yandex.metrika.clickhouse.except.ClickhouseExceptionSpecifier;
import ru.yandex.metrika.clickhouse.util.CopypasteUtils;
import ru.yandex.metrika.clickhouse.util.Logger;

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
/**
 * Created by jkee on 14.03.15.
 */
public class CHStatementImpl implements CHStatement {

    private static final Logger log = Logger.of(CHStatementImpl.class);

    private final CloseableHttpClient client;

    private CHProperties properties = new CHProperties();

    private CHDataSource source;

    private CHResultSet currentResult;

    private int queryTimeout;

    private int maxRows;

    private boolean closeOnCompletion;

    private ObjectMapper objectMapper;

    public CHStatementImpl(CloseableHttpClient client, CHDataSource source,
                           CHProperties properties) {
        this.client = client;
        this.source = source;
        this.properties = properties;

        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return executeQuery(sql, null);
    }

    public ResultSet executeQuery(String sql, Map<CHQueryParam, String> additionalDBParams) throws SQLException {
        InputStream is = getInputStream(sql, additionalDBParams, false);
        try {
            currentResult = new CHResultSet(properties.isCompress()
                    ? new ClickhouseLZ4Stream(is) : is, properties.getBufferSize(),
                    extractDBName(sql),
                    extractTableName(sql)
            );
            currentResult.setMaxRows(maxRows);
            return currentResult;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ClickhouseResponse executeQueryClickhouseResponse(String sql) throws SQLException {
        return executeQueryClickhouseResponse(sql, null);
    }

    public ClickhouseResponse executeQueryClickhouseResponse(String sql, Map<CHQueryParam, String> additionalDBParams) throws SQLException {
        return executeQueryClickhouseResponse(sql, additionalDBParams, false);
    }

    public ClickhouseResponse executeQueryClickhouseResponse(String sql, Map<CHQueryParam, String> additionalDBParams, boolean ignoreDatabase) throws SQLException {
        InputStream is = getInputStream(clickhousifySql(sql, "JSONCompact"), additionalDBParams, ignoreDatabase);
        try {
            byte[] bytes = null;
            try {
                if (properties.isCompress()){
                    bytes = CopypasteUtils.toByteArray(new ClickhouseLZ4Stream(is));
                } else {
                    bytes = CopypasteUtils.toByteArray(is);
                }
                return objectMapper.readValue(bytes, ClickhouseResponse.class);
            } catch (IOException e) {
                if (bytes != null) log.warn("Wrong json: "+new String(bytes));
                throw e;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (is != null) try {is.close();} catch (IOException ignored) { }
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        ResultSet rs = null;
        try {
            rs = executeQuery(sql);
            //noinspection StatementWithEmptyBody
            while (rs.next()) {}
        } finally {
            try { if (rs != null) rs.close(); } catch (Exception ignored) {}
        }
        return 1;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        executeQuery(sql);
        return true;
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
        return 0;
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
        return null;
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
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public static String clickhousifySql(String sql) {
        return clickhousifySql(sql, "TabSeparatedWithNamesAndTypes");
    }

    public static String clickhousifySql(String sql, String format) {
        sql = sql.trim();
        if (!sql.replace(";", "").trim().endsWith(" TabSeparatedWithNamesAndTypes")
                && !sql.replace(";", "").trim().endsWith(" TabSeparated")
                && !sql.replace(";", "").trim().endsWith(" JSONCompact")) {
            if (sql.endsWith(";")) sql = sql.substring(0, sql.length() - 1);
            sql += " FORMAT " + format + ';';
        }
        return sql;
    }

    private String extractTableName(String sql) {
        String s = extractDBAndTableName(sql);
        if (s.contains(".")) {
            return s.substring(s.indexOf(".") + 1);
        } else return s;
    }

    private String extractDBName(String sql) {
        String s = extractDBAndTableName(sql);
        if (s.contains(".")) {
            return s.substring(0, s.indexOf("."));
        } else {
            return source.getDatabase();
        }
    }

    private String extractDBAndTableName(String sql) {
        // паршивый код, надо писать или найти нормальный парсер
        if (CopypasteUtils.startsWithIgnoreCase(sql, "select")) {
            String withoutStrings = CopypasteUtils.retainUnquoted(sql, '\'');
            int fromIndex = withoutStrings.indexOf("from");
            if (fromIndex == -1) fromIndex = withoutStrings.indexOf("FROM");
            if (fromIndex != -1) {
                String fromFrom = withoutStrings.substring(fromIndex);
                String fromTable = fromFrom.substring("from".length()).trim();
                return fromTable.split(" ")[0];
            }
        }
        if (CopypasteUtils.startsWithIgnoreCase(sql, "desc")) {
            return "system.columns"; // bullshit
        }
        if (CopypasteUtils.startsWithIgnoreCase(sql, "show")) {
            return "system.tables"; // bullshit
        }
        return "system.unknown";
    }

    private InputStream getInputStream(String sql,
                                       Map<CHQueryParam, String> additionalClickHouseDBParams,
                                       boolean ignoreDatabase
    ) throws CHException {
        sql = clickhousifySql(sql);
        log.debug("Executing SQL: " + sql);
        URI uri = null;
        try {
            Map<CHQueryParam, String> params = properties.buildParams(ignoreDatabase);
            if (additionalClickHouseDBParams != null && !additionalClickHouseDBParams.isEmpty()) {
                params.putAll(additionalClickHouseDBParams);
            }
            List<String> paramPairs = new ArrayList<String>();
            for (Map.Entry<CHQueryParam, String> entry : params.entrySet()) {
                paramPairs.add(entry.getKey().toString() + '=' + entry.getValue());
            }
            String query = CopypasteUtils.join(paramPairs, '&');
            uri = new URI("http", null, source.getHost(), source.getPort(),
                    "/", query, null);
        } catch (URISyntaxException e) {
            log.error("Mailformed URL: " + e.getMessage());
            throw new IllegalStateException("illegal configuration of db");
        }
        log.debug("Request url: " + uri);
        HttpPost post = new HttpPost(uri);
        post.setEntity(new StringEntity(sql, CopypasteUtils.UTF_8));
        HttpEntity entity = null;
        InputStream is = null;
        try {
            HttpResponse response = client.execute(post);
            entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
                String chMessage;
                try {
                    InputStream messageStream = entity.getContent();
                    if (properties.isCompress()) {
                        messageStream = new ClickhouseLZ4Stream(messageStream);
                    }
                    chMessage = CopypasteUtils.toString(messageStream);
                } catch (IOException e) {
                    chMessage = "error while read response "+ e.getMessage();
                }
                EntityUtils.consumeQuietly(entity);
                throw ClickhouseExceptionSpecifier.specify(chMessage, source.getHost(), source.getPort());
            }
            if (entity.isStreaming()) {
                is = entity.getContent();
            } else {
                FastByteArrayOutputStream baos = new FastByteArrayOutputStream();
                entity.writeTo(baos);
                is = baos.convertToInputStream();
            }
            return is;
        } catch (IOException e) {
            log.info("Error during connection to " + source + ", reporting failure to data source, message: " + e.getMessage());
            EntityUtils.consumeQuietly(entity);
            try { if (is != null) is.close(); } catch (IOException ignored) { }
            log.info("Error sql: " + sql);
            throw new CHException("Unknown IO exception", e);
        }
    }

    public void sendStream(InputStream content, String table) throws CHException  {
        // echo -ne '10\n11\n12\n' | POST 'http://localhost:8123/?query=INSERT INTO t FORMAT TabSeparated'
        HttpEntity entity = null;
        try {
            URI uri = new URI("http", null, source.getHost(), source.getPort(),
                    "/", (CopypasteUtils.isEmpty(source.getDatabase()) ? "" : "database=" + source.getDatabase() + '&')
                    + "query=INSERT INTO " + table + " FORMAT TabSeparated", null);
            HttpPost httpPost = new HttpPost(uri);
            httpPost.setEntity(new InputStreamEntity(content, -1));
            HttpResponse response = client.execute(httpPost);
            entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
                String chMessage;
                try {
                    chMessage = EntityUtils.toString(response.getEntity());
                } catch (IOException e) {
                    chMessage = "error while read response "+ e.getMessage();
                }
                throw ClickhouseExceptionSpecifier.specify(chMessage, source.getHost(), source.getPort());
            }
        } catch (CHException e) {
            throw e;
        } catch (Exception e) {
            throw ClickhouseExceptionSpecifier.specify(e, source.getHost(), source.getPort());
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
