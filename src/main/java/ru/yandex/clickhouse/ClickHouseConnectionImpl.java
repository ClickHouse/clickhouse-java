package ru.yandex.clickhouse;

import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.except.ClickHouseUnknownException;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseHttpClientBuilder;
import ru.yandex.clickhouse.util.LogProxy;
import ru.yandex.clickhouse.util.TypeUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;


public class ClickHouseConnectionImpl implements ClickHouseConnection {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseConnectionImpl.class);

    private final CloseableHttpClient httpclient;

    private final ClickHouseProperties properties;

    private final String url;

    private boolean closed = false;

    public ClickHouseConnectionImpl(String url) {
        this(url, new ClickHouseProperties());
    }

    public ClickHouseConnectionImpl(String url, ClickHouseProperties properties) {
        this.url = url;
        try {
            this.properties = ClickhouseJdbcUrlParser.parse(url, properties.asProperties());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        ClickHouseHttpClientBuilder clientBuilder = new ClickHouseHttpClientBuilder(this.properties);
        log.debug("new connection");
        httpclient = clientBuilder.buildClient();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return LogProxy.wrap(ClickHouseStatement.class, new ClickHouseStatementImpl(httpclient, this, properties));
    }

    @Override
    public ClickHouseStatement createClickHouseStatement() throws SQLException {
        return LogProxy.wrap(ClickHouseStatement.class, new ClickHouseStatementImpl(httpclient, this, properties));
    }

    private ClickHouseStatement createClickHouseStatement(CloseableHttpClient httpClient) throws SQLException {
        return LogProxy.wrap(ClickHouseStatement.class, new ClickHouseStatementImpl(httpClient, this, properties));
    }

    public PreparedStatement createPreparedStatement(String sql) throws SQLException {
        return LogProxy.wrap(PreparedStatement.class, new ClickHousePreparedStatementImpl(httpclient, this, properties, sql));
    }

    public ClickHousePreparedStatement createClickHousePreparedStatement(String sql) throws SQLException {
        return LogProxy.wrap(ClickHousePreparedStatement.class, new ClickHousePreparedStatementImpl(httpclient, this, properties, sql));
    }


    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY && resultSetConcurrency != ResultSet.CONCUR_READ_ONLY
                && resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLFeatureNotSupportedException();
        }
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return createPreparedStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {

    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    @Override
    public void commit() throws SQLException {

    }

    @Override
    public void rollback() throws SQLException {

    }

    @Override
    public void close() throws SQLException {
        try {
            httpclient.close();
            closed = true;
        } catch (IOException e) {
            throw new ClickHouseUnknownException("HTTP client close exception", e, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return LogProxy.wrap(DatabaseMetaData.class, new ClickHouseDatabaseMetadata(url, this));
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {

    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }


    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return createPreparedStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createPreparedStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (isClosed()) {
            return false;
        }

        try {
            ClickHouseProperties properties = new ClickHouseProperties(this.properties);
            properties.setConnectionTimeout((int) TimeUnit.SECONDS.toMillis(timeout));
            CloseableHttpClient client = new ClickHouseHttpClientBuilder(properties).buildClient();
            Statement statement = createClickHouseStatement(client);
            statement.execute("SELECT 1");
            statement.close();
            return true;
        } catch (Exception e) {
            boolean isFailOnConnectionTimeout = e.getCause() instanceof ConnectTimeoutException;
            if(!isFailOnConnectionTimeout){
                log.warn("Something had happened while validating a connection", e);
            }

            return false;
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return new ClickHouseArray(TypeUtils.toSqlType(typeName), TypeUtils.isUnsigned(typeName), elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
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

    public void setSchema(String schema) throws SQLException {

    }

    public String getSchema() throws SQLException {
        return null;
    }

    public void abort(Executor executor) throws SQLException {
        this.close();
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    void cleanConnections() {
        httpclient.getConnectionManager().closeExpiredConnections();
        httpclient.getConnectionManager().closeIdleConnections(2 * properties.getSocketTimeout(), TimeUnit.MILLISECONDS);
    }
}
