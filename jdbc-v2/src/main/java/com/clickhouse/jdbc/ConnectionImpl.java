package com.clickhouse.jdbc;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.jdbc.internal.ClientInfoProperties;
import com.clickhouse.jdbc.internal.JdbcConfiguration;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.clickhouse.jdbc.internal.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.sql.ShardingKey;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
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
    private String appName;
    private QuerySettings defaultQuerySettings;

    private final com.clickhouse.jdbc.metadata.DatabaseMetaData metadata;
    protected final Calendar defaultCalendar;

    public ConnectionImpl(String url, Properties info) throws SQLException {
        try {
            log.debug("Creating connection to {}", url);
            this.url = url;//Raw URL
            this.config = new JdbcConfiguration(url, info);
            this.onCluster = false;
            this.cluster = null;
            this.appName = "";
            String clientName = "ClickHouse JDBC Driver V2/" + Driver.driverVersion;

            Map<String, String> clientProperties = config.getClientProperties();
            if (clientProperties.get(ClientConfigProperties.CLIENT_NAME.getKey()) != null) {
                this.appName = clientProperties.get(ClientConfigProperties.CLIENT_NAME.getKey()).trim();
                clientName = this.appName + " " + clientName; // Use the application name as client name
            } else if (clientProperties.get(ClientConfigProperties.PRODUCT_NAME.getKey()) != null) {
                // Backward compatibility for old property
                this.appName = clientProperties.get(ClientConfigProperties.PRODUCT_NAME.getKey()).trim();
                clientName = this.appName + " " + clientName; // Use the application name as client name
            }

            if (this.config.isDisableFrameworkDetection()) {
                log.debug("Framework detection is disabled.");
            } else {
                String detectedFrameworks = Driver.FrameworksDetection.getFrameworksDetected();
                log.debug("Detected frameworks: {}", detectedFrameworks);
                if (!detectedFrameworks.trim().isEmpty()) {
                    clientName += " (" + detectedFrameworks + ")";
                }
            }

            this.client = this.config.applyClientProperties(new Client.Builder())
                    .setClientName(clientName)
                    .build();
            this.client.loadServerInfo();
            this.schema = client.getDefaultDatabase();
            this.defaultQuerySettings = new QuerySettings()
                    .serverSetting(ServerSettings.ASYNC_INSERT, "0")
                    .serverSetting(ServerSettings.WAIT_END_OF_QUERY, "0");

            this.metadata = new com.clickhouse.jdbc.metadata.DatabaseMetaData(this, false, url);
            this.defaultCalendar = Calendar.getInstance();
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Failed to create connection", ExceptionUtils.SQL_STATE_CONNECTION_EXCEPTION, e);
        }
    }

    public QuerySettings getDefaultQuerySettings() {
        return defaultQuerySettings;
    }

    public void setDefaultQuerySettings(QuerySettings settings) {
        this.defaultQuerySettings = settings;
    }

    public String getServerVersion() throws SQLException {
        GenericRecord result = client.queryAll("SELECT version() as server_version").stream()
                .findFirst().orElseThrow(() -> new SQLException("Failed to retrieve server version.", ExceptionUtils.SQL_STATE_CLIENT_ERROR));

        return result.getString("server_version");
    }

    /**
     * Returns configuration for current connection. Changes made to the instance of configuration may have side effects.
     * Application should avoid making changes to this object until it is documented.
     * @return - reference to internal instance of JdbcConfiguration
     */
    public JdbcConfiguration getJdbcConfig() {
        return this.config;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkOpen();
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkOpen();
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        checkOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("CallableStatement not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkOpen();
        /// TODO: this is not implemented according to JDBC spec and may not be used.
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("nativeSQL not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkOpen();

        if (!config.isIgnoreUnsupportedRequests() && !autoCommit) {
            throw new SQLFeatureNotSupportedException("setAutoCommit = false not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkOpen();
        return true;
    }

    @Override
    public void commit() throws SQLException {
        if (!config.isIgnoreUnsupportedRequests() ) {
            throw new SQLFeatureNotSupportedException("Commit/Rollback not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void rollback() throws SQLException {
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Commit/Rollback not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
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
        if (!config.isIgnoreUnsupportedRequests() && readOnly) {
            throw new SQLFeatureNotSupportedException("read-only=true unsupported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
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
        if (!config.isIgnoreUnsupportedRequests() && TRANSACTION_NONE != level) {
            throw new SQLFeatureNotSupportedException("setTransactionIsolation not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
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
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkOpen();
        return prepareStatement(sql, resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("CallableStatement not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("getTypeMap not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("setTypeMap not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
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
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Savepoint not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Savepoint not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Commit/Rollback not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Savepoint not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkOpen();
        return new StatementImpl(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkOpen();
        return new PreparedStatementImpl(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("CallableStatement not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkOpen();
        //TODO: Should this be supported?
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("prepareStatement(String sql, int autoGeneratedKeys) not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        } else {
            return prepareStatement(sql);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkOpen();
        //TODO: Should this be supported?
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("prepareStatement(String sql, int[] columnIndexes) not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        } else {
            return prepareStatement(sql);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkOpen();
        //TODO: Should this be supported?
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("prepareStatement(String sql, String[] columnNames) not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        } else {
            return prepareStatement(sql);
        }
    }

    @Override
    public Clob createClob() throws SQLException {
        checkOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Clob not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Blob not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("NClob not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("SQLXML not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        checkOpen();
        if (timeout < 0) {
            throw new SQLException("Timeout must be >= 0", ExceptionUtils.SQL_STATE_CLIENT_ERROR);
        }

        //TODO: This is a placeholder implementation
        return true;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        if (ClientInfoProperties.APPLICATION_NAME.getKey().equals(name)) {
            config.updateUserClient(value, client);
            appName = value;
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
        if (ClientInfoProperties.APPLICATION_NAME.getKey().equals(name)) {
            return appName;
        } else {
            return null;
        }
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkOpen();
        Properties clientInfo = new Properties();
        clientInfo.put(ClientInfoProperties.APPLICATION_NAME.getKey(), getClientInfo(ClientInfoProperties.APPLICATION_NAME.getKey()));
        return clientInfo;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        try {
            return new com.clickhouse.jdbc.types.Array(List.of(elements), typeName, JdbcUtils.convertToSqlType(ClickHouseDataType.valueOf(typeName)).getVendorTypeNumber());
        } catch (Exception e) {
            throw new SQLException("Failed to create array", ExceptionUtils.SQL_STATE_CLIENT_ERROR, e);
        }
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        //TODO: Should this be supported?
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("createStruct not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

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
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("abort not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        //TODO: Should this be supported?
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("setNetworkTimeout not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        //TODO: Should this be supported?
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("getNetworkTimeout not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return -1;
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

    /**
     * Returns instance of the client used to execute queries by this connection.
     * @return - client instance
     */
    public Client getClient() {
        return client;
    }

    private void checkOpen() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is closed", ExceptionUtils.SQL_STATE_CONNECTION_EXCEPTION);
        }
    }
}
