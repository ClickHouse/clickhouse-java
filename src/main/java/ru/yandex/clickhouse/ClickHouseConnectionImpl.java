package ru.yandex.clickhouse;

import com.google.common.base.Strings;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.except.ClickHouseUnknownException;
import ru.yandex.clickhouse.settings.ClickHouseConnectionSettings;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseHttpClientBuilder;
import ru.yandex.clickhouse.util.LogProxy;
import ru.yandex.clickhouse.util.TypeUtils;
import ru.yandex.clickhouse.util.guava.StreamUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;


public class ClickHouseConnectionImpl implements ClickHouseConnection {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseConnectionImpl.class);

    private final CloseableHttpClient httpclient;

    private final ClickHouseProperties properties;

    private String url;

    private boolean closed = false;

    private TimeZone timezone;
    private volatile String serverVersion;

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
        try {
            httpclient = clientBuilder.buildClient();
        }catch (Exception e) {
            throw  new IllegalStateException("cannot initialize http client", e);
        }
        initTimeZone(this.properties);
    }

    private void initTimeZone(ClickHouseProperties properties) {
        if (properties.isUseServerTimeZone() && !Strings.isNullOrEmpty(properties.getUseTimeZone())) {
            throw new IllegalArgumentException(String.format("only one of %s or %s must be enabled", ClickHouseConnectionSettings.USE_SERVER_TIME_ZONE.getKey(), ClickHouseConnectionSettings.USE_TIME_ZONE.getKey()));
        }
        if (properties.isUseServerTimeZone()) {
            ResultSet rs = null;
            try {
                timezone = TimeZone.getTimeZone("UTC"); // just for next query
                rs = createStatement().executeQuery("select timezone()");
                rs.next();
                String timeZoneName = rs.getString(1);
                timezone = TimeZone.getTimeZone(timeZoneName);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                StreamUtils.close(rs);
            }
        } else if (!Strings.isNullOrEmpty(properties.getUseTimeZone())) {
            timezone = TimeZone.getTimeZone(properties.getUseTimeZone());
        }
    }

    @Override
    public ClickHouseStatement createStatement() throws SQLException {
        return LogProxy.wrap(ClickHouseStatement.class, new ClickHouseStatementImpl(httpclient, this, properties));
    }

    @Deprecated
    @Override
    public ClickHouseStatement createClickHouseStatement() throws SQLException {
        return createStatement();
    }

    @Override
    public TimeZone getTimeZone() {
        return timezone;
    }

    private ClickHouseStatement createClickHouseStatement(CloseableHttpClient httpClient) throws SQLException {
        return LogProxy.wrap(ClickHouseStatement.class, new ClickHouseStatementImpl(httpClient, this, properties));
    }

    public PreparedStatement createPreparedStatement(String sql) throws SQLException {
        return LogProxy.wrap(PreparedStatement.class, ClickHousePreparedStatementFactory.getQuery(sql, httpclient, this, properties, timezone));
    }

    public ClickHousePreparedStatement createClickHousePreparedStatement(String sql) throws SQLException {
        return LogProxy.wrap(ClickHousePreparedStatement.class, ClickHousePreparedStatementFactory.getQuery(sql, httpclient, this, properties, timezone));
    }


    @Override
    public ClickHouseStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    /**
     * lazily calculates and returns server version
     * @return server version string
     * @throws SQLException if something has gone wrong
     */
    @Override
    public String getServerVersion() throws SQLException {
        if (serverVersion == null) {
            ResultSet rs = createStatement().executeQuery("select version()");
            rs.next();
            serverVersion = rs.getString(1);
        }
        return serverVersion;
    }

    @Override
    public ClickHouseStatement createStatement(int resultSetType, int resultSetConcurrency,
                                               int resultSetHoldability) throws SQLException {
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
        properties.setDatabase(catalog);
        URI old = URI.create(url.substring(ClickhouseJdbcUrlParser.JDBC_PREFIX.length()));
        try {
            url = ClickhouseJdbcUrlParser.JDBC_PREFIX +
                    new URI(old.getScheme(), old.getUserInfo(), old.getHost(), old.getPort(),
                            "/" + catalog, old.getQuery(), old.getFragment());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        return properties.getDatabase();
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
            if (!isFailOnConnectionTimeout) {
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
        properties.setDatabase(schema);
    }

    public String getSchema() throws SQLException {
        return properties.getDatabase();
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

    String getUrl() {
        return url;
    }

    ClickHouseProperties getProperties() {
        return properties;
    }
}
