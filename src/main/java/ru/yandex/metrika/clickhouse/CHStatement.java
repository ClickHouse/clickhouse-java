package ru.yandex.metrika.clickhouse;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import ru.yandex.metrika.clickhouse.copypaste.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.sql.*;

/**
 * Created by jkee on 14.03.15.
 */
public class CHStatement implements Statement {

    private final CloseableHttpClient client;

    private final String url;

    private HttpConnectionProperties properties = new HttpConnectionProperties();

    public CHStatement(CloseableHttpClient client, String url) {
        this.client = client;
        this.url = url;
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

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        String csql = clickhousifySql(sql);
        CountingInputStream is = getInputStream(csql);
        try {
            return new CHResultSet(properties.isCompress()
                    ? new ClickhouseLZ4Stream(is) : is, properties.getBufferSize());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CountingInputStream getInputStream(String sql) {
        HttpPost post = new HttpPost(url);
        post.setEntity(new StringEntity(sql, StandardCharsets.UTF_8));
        HttpEntity entity = null;
        InputStream is = null;
        try {
            HttpResponse response = client.execute(post);
            entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
                String chMessage = null;
                try {
                    chMessage = EntityUtils.toString(response.getEntity());
                } catch (IOException e) {
                    chMessage = "error while read response "+ e.getMessage();
                }
                EntityUtils.consumeQuietly(entity);
                throw new RuntimeException("CH error: " + chMessage);
            }
            if (entity.isStreaming()) {
                is = entity.getContent();
            } else {
                FastByteArrayOutputStream baos = new FastByteArrayOutputStream();
                entity.writeTo(baos);
                is = baos.convertToInputStream();
            }
            return new CountingInputStream(is);
        } catch (IOException e) {
            EntityUtils.consumeQuietly(entity);
            throw new RuntimeException(e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws SQLException {
        try {
            client.close();
        } catch (IOException e) {
            throw new CHException("HTTP client close exception", e);
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
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

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
    public boolean execute(String sql) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return null;
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
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
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
}
