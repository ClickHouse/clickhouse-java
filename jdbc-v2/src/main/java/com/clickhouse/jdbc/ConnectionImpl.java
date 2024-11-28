package com.clickhouse.jdbc;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.jdbc.internal.ClientInfoProperties;
import com.clickhouse.jdbc.internal.JdbcConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;

public class ConnectionImpl implements Connection, JdbcV2Wrapper {
    private static final Logger log = LoggerFactory.getLogger(ConnectionImpl.class);

    protected final String url;
    protected final Client client;
    protected final JdbcConfiguration config;

    private boolean closed = false;
    protected boolean onCluster;//TODO: Placeholder for cluster support
    protected String cluster;
    private String catalog;
    private String schema;
    private QuerySettings defaultQuerySettings;

    private final com.clickhouse.jdbc.metadata.DatabaseMetaData metadata;

    public ConnectionImpl(String url, Properties info) {
        log.debug("Creating connection to {}", url);
        this.url = url;//Raw URL
        this.config = new JdbcConfiguration(url, info);
        this.onCluster = false;
        this.cluster = null;
        String clientName = "ClickHouse JDBC Driver V2/" + Driver.driverVersion;

        if (this.config.isDisableFrameworkDetection()) {
            log.debug("Framework detection is disabled.");
        } else {
            String detectedFrameworks = Driver.FrameworksDetection.getFrameworksDetected();
            log.debug("Detected frameworks: {}", detectedFrameworks);
            clientName += " (" + detectedFrameworks + ")";
        }

        this.client =  new Client.Builder()
                .fromUrl(this.config.getUrl())//URL without prefix
                .setUsername(config.getUser())
                .setPassword(config.getPassword())
                .setClientName(clientName)
                .build();
        this.defaultQuerySettings = new QuerySettings();

        this.metadata = new com.clickhouse.jdbc.metadata.DatabaseMetaData(this, false);
    }

    public String getUser() {
        return config.getUser();
    }

    public String getURL() {
        return url;
    }

    public QuerySettings getDefaultQuerySettings() {
        return defaultQuerySettings;
    }

    public void setDefaultQuerySettings(QuerySettings settings) {
        this.defaultQuerySettings = settings;
    }

    public String getServerVersion() throws SQLException {
        GenericRecord result = client.queryAll("SELECT version() as server_version").stream()
                .findFirst().orElseThrow(() -> new SQLException("Failed to retrieve server version."));

        return result.getString("server_version");
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkOpen();
        return new StatementImpl(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkOpen();
        return new PreparedStatementImpl(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkOpen();
        /// TODO: this is not implemented according to JDBC spec and may not be used.
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkOpen();
        if (!autoCommit) {
            throw new SQLFeatureNotSupportedException("setAutoCommit = false not supported");
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkOpen();
        return true;
    }

    @Override
    public void commit() throws SQLException {
        throw new SQLFeatureNotSupportedException("Commit/Rollback not supported");
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("Commit/Rollback not supported");
    }

    @Override
    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }

        client.close();
        closed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkOpen();
        return this.metadata;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkOpen();
        if (readOnly) {
            throw new SQLFeatureNotSupportedException("read-only=true unsupported");
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkOpen();
//        this.catalog = catalog; currently not supported
    }

    @Override
    public String getCatalog() throws SQLException {
        return catalog;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkOpen();
        if (TRANSACTION_NONE != level) {
            throw new SQLFeatureNotSupportedException("setTransactionIsolation not supported");
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkOpen();
        return TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkOpen();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkOpen();
        //TODO: Should this be a silent ignore?
        throw new SQLFeatureNotSupportedException("Statement with resultSetType and resultSetConcurrency override not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("PreparedStatement with resultSetType and resultSetConcurrency override not supported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("getTypeMap not supported");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("setTypeMap not supported");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkOpen();
        //TODO: Should this be supported?
    }

    @Override
    public int getHoldability() throws SQLException {
        checkOpen();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;//TODO: Check if this is correct
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Savepoint not supported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Savepoint not supported");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Commit/Rollback not supported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Savepoint not supported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkOpen();
        //TODO: Should this be a silent ignore?
        throw new SQLFeatureNotSupportedException("Statement with resultSetType, resultSetConcurrency, and resultSetHoldability override not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkOpen();
        //TODO: Should this be a silent ignore?
        throw new SQLFeatureNotSupportedException("PreparedStatement with resultSetType, resultSetConcurrency, and resultSetHoldability override not supported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkOpen();
        //TODO: Should this be supported?
        throw new SQLFeatureNotSupportedException("prepareStatement(String sql, int autoGeneratedKeys) not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkOpen();
        //TODO: Should this be supported?
        throw new SQLFeatureNotSupportedException("prepareStatement(String sql, int[] columnIndexes) not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkOpen();
        //TODO: Should this be supported?
        throw new SQLFeatureNotSupportedException("prepareStatement(String sql, String[] columnNames) not supported");
    }

    @Override
    public Clob createClob() throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        checkOpen();
        if (timeout < 0) {
            throw new SQLException("Timeout must be >= 0");
        }

        //TODO: This is a placeholder implementation
        return true;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        if (ClientInfoProperties.APPLICATION_NAME.getKey().equals(name)) {
            client.updateClientName(value);
        }
        // TODO: generate warning for unknown properties
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        Set<String> toSet = new HashSet<>();
        Set<String> toReset = new HashSet<>();
        for (ClientInfoProperties p : ClientInfoProperties .values()) {
            String key = p.getKey();
            if (properties.containsKey(key)) {
                toSet.add(key);
            } else {
                toReset.add(key);
            }
        }

        // first we reset value
        for (String key : toReset) {
            setClientInfo(key, null);
        }

        // then we set value, so aliases will not clean values accidentally
        for (String key : toSet) {
            setClientInfo(key, properties.getProperty(key));
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkOpen();
//        Object value = this.defaultQuerySettings.getAllSettings().get(name);
//        return value == null ? null : String.valueOf(value);
        throw new SQLFeatureNotSupportedException("getClientInfo not supported");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkOpen();
//        Properties clientInfo = new Properties();
//        clientInfo.putAll(this.defaultQuerySettings.getAllSettings());
//        return clientInfo;
        throw new SQLFeatureNotSupportedException("getClientInfo not supported");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        try {
            return new com.clickhouse.jdbc.types.Array(List.of(elements));
        } catch (Exception e) {
            throw new SQLException("Failed to create array", e);
        }
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        //TODO: Should this be supported?
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkOpen();
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        checkOpen();
        return schema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException("abort not supported");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        //TODO: Should this be supported?
        throw new SQLFeatureNotSupportedException("setNetworkTimeout not supported");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        //TODO: Should this be supported?
        throw new SQLFeatureNotSupportedException("getNetworkTimeout not supported");
    }

    @Override
    public void beginRequest() throws SQLException {
        Connection.super.beginRequest();
    }

    @Override
    public void endRequest() throws SQLException {
        Connection.super.endRequest();
    }

    @Override
    public boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey, int timeout) throws SQLException {
        return Connection.super.setShardingKeyIfValid(shardingKey, superShardingKey, timeout);
    }

    @Override
    public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) throws SQLException {
        return Connection.super.setShardingKeyIfValid(shardingKey, timeout);
    }

    @Override
    public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey) throws SQLException {
        Connection.super.setShardingKey(shardingKey, superShardingKey);
    }

    @Override
    public void setShardingKey(ShardingKey shardingKey) throws SQLException {
        Connection.super.setShardingKey(shardingKey);
    }

    private void checkOpen() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is closed");
        }
    }
}
