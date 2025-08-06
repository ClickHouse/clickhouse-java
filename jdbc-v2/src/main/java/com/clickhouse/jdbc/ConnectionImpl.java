package com.clickhouse.jdbc;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.clickhouse.jdbc.internal.JdbcConfiguration;
import com.clickhouse.jdbc.internal.JdbcUtils;
import com.clickhouse.jdbc.internal.ParsedPreparedStatement;
import com.clickhouse.jdbc.internal.SqlParser;
import com.clickhouse.jdbc.metadata.DatabaseMetaDataImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
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
import java.time.Duration;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ConnectionImpl implements Connection, JdbcV2Wrapper {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionImpl.class);

    protected final String url;
    private final Client client; // this member is private to force using getClient()
    protected final JdbcConfiguration config;

    private boolean closed = false;
    protected boolean onCluster;//TODO: Placeholder for cluster support
    protected String cluster;
    private String catalog;
    private String schema;
    private String appName;
    private QuerySettings defaultQuerySettings;

    private final DatabaseMetaDataImpl metadata;
    protected final Calendar defaultCalendar;

    private final SqlParser sqlParser;

    private Executor networkTimeoutExecutor;

    public ConnectionImpl(String url, Properties info) throws SQLException {
        try {
            LOG.debug("Creating connection to {}", url);
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
                LOG.debug("Framework detection is disabled.");
            } else {
                String detectedFrameworks = Driver.FrameworksDetection.getFrameworksDetected();
                LOG.debug("Detected frameworks: {}", detectedFrameworks);
                if (!detectedFrameworks.trim().isEmpty()) {
                    clientName += " (" + detectedFrameworks + ")";
                }
            }

            this.client = this.config.applyClientProperties(new Client.Builder())
                    .setClientName(clientName)
                    .build();
            String serverTimezone = this.client.getServerTimeZone();
            if (serverTimezone == null) {
                // we cannot operate without timezone
                this.client.loadServerInfo();
            }
            this.schema = client.getDefaultDatabase();
            this.defaultQuerySettings = new QuerySettings()
                    .serverSetting(ServerSettings.ASYNC_INSERT, "0")
                    .serverSetting(ServerSettings.WAIT_END_OF_QUERY, "0");

            this.metadata = new DatabaseMetaDataImpl(this, false, url);
            this.defaultCalendar = Calendar.getInstance();

            this.sqlParser = new SqlParser();
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Failed to create connection", ExceptionUtils.SQL_STATE_CONNECTION_EXCEPTION, e);
        }
    }

    public SqlParser getSqlParser() {
        return sqlParser;
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
        ensureOpen();
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        ensureOpen();
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        ensureOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("CallableStatement not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        ensureOpen();
        // Currently it replaces escaped functions with real ones.
        return StatementImpl.escapedSQLToNative(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        ensureOpen();

        if (!config.isIgnoreUnsupportedRequests() && !autoCommit) {
            throw new SQLFeatureNotSupportedException("setAutoCommit = false not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        ensureOpen();
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
        closed = true; // mark as closed to prevent further invocations
        client.close(); // this will disrupt pending requests.
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        ensureOpen();
        return this.metadata;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        ensureOpen();
        if (!config.isIgnoreUnsupportedRequests() && readOnly) {
            throw new SQLFeatureNotSupportedException("read-only=true unsupported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        ensureOpen();
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        ensureOpen();
//        this.catalog = catalog; currently not supported
    }

    @Override
    public String getCatalog() throws SQLException {
        return catalog;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        ensureOpen();
        if (!config.isIgnoreUnsupportedRequests() && TRANSACTION_NONE != level) {
            throw new SQLFeatureNotSupportedException("setTransactionIsolation not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        ensureOpen();
        return TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        ensureOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        ensureOpen();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        ensureOpen();
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        ensureOpen();
        return prepareStatement(sql, resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        ensureOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("CallableStatement not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        ensureOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("getTypeMap not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        ensureOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("setTypeMap not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        ensureOpen();
        //TODO: Should this be supported?
    }

    @Override
    public int getHoldability() throws SQLException {
        ensureOpen();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;//TODO: Check if this is correct
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        ensureOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Savepoint not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        ensureOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Savepoint not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        ensureOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Commit/Rollback not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        ensureOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Savepoint not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        ensureOpen();
        checkResultSetFlags(resultSetType, resultSetConcurrency, resultSetHoldability);
        return new StatementImpl(this);
    }

    private void checkResultSetFlags(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (!config.isIgnoreUnsupportedRequests()) {
            if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
                throw new SQLFeatureNotSupportedException("Cannot create statement with result set type other then ResultSet.TYPE_FORWARD_ONLY",
                        ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
            }
            if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
                throw new SQLFeatureNotSupportedException("Cannot create statement with result set concurrency other then ResultSet.CONCUR_READ_ONLY",
                        ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
            }
            if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
                throw new SQLFeatureNotSupportedException("Cannot create statement with result set holdability other then ResultSet.CLOSE_CURSORS_AT_COMMIT",
                        ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
            }
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        ensureOpen();
        checkResultSetFlags(resultSetType, resultSetConcurrency, resultSetHoldability);
        ParsedPreparedStatement parsedStatement = sqlParser.parsePreparedStatement(sql);

        if (parsedStatement.isInsert() && config.isBetaFeatureEnabled(DriverProperties.BETA_ROW_BINARY_WRITER)) {
            /*
             * RowBinary can be used when
             * - INSERT INTO t (c1, c2) VALUES (?, ?)
             * - INSERT INTO t VALUES (?, ?, ?)
             * - number of arguments matches schema or column list
             * RowBinary cannot be used when
             * - INSERT INTO t VALUES (now(), ?, ?) !# there is a function in the values
             * - INSERT INTO t VALUES (now(), ?, 1), (now(), ?, 2) !# multiple values list
             * - INSERT INTO t SELECT ?, ?, ? !# insert from select
             */
            if (!parsedStatement.isInsertWithSelect() && parsedStatement.getAssignValuesGroups() == 1
                    && !parsedStatement.isUseFunction()) {
                TableSchema tableSchema = client.getTableSchema(parsedStatement.getTable(), schema);
                return new WriterStatementImpl(this, sql, tableSchema, parsedStatement);
            }
        }
        return new PreparedStatementImpl(this, sql, parsedStatement);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        ensureOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("CallableStatement not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        ensureOpen();
        //TODO: Should this be supported?
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("prepareStatement(String sql, int autoGeneratedKeys) not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        } else {
            return prepareStatement(sql);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        ensureOpen();
        //TODO: Should this be supported?
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("prepareStatement(String sql, int[] columnIndexes) not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        } else {
            return prepareStatement(sql);
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        ensureOpen();
        //TODO: Should this be supported?
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("prepareStatement(String sql, String[] columnNames) not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        } else {
            return prepareStatement(sql);
        }
    }

    @Override
    public Clob createClob() throws SQLException {
        ensureOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Clob not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        ensureOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("Blob not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        ensureOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("NClob not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        ensureOpen();
        if (!config.isIgnoreUnsupportedRequests()) {
            throw new SQLFeatureNotSupportedException("SQLXML not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
        }

        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException("Timeout must be >= 0", ExceptionUtils.SQL_STATE_CLIENT_ERROR);
        }
        if (isClosed()) {
            return false;
        }
        return timeout == 0
            ? client.ping()
            : client.ping(Duration.ofSeconds(timeout).toMillis());
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
        ensureOpen();
        if (ClientInfoProperties.APPLICATION_NAME.getKey().equals(name)) {
            return appName;
        } else {
            return null;
        }
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        ensureOpen();
        Properties clientInfo = new Properties();
        clientInfo.put(ClientInfoProperties.APPLICATION_NAME.getKey(), appName);
        return clientInfo;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        try {
            List<Object> list =
                    (elements == null || elements.length == 0) ? Collections.emptyList() : Arrays.stream(elements, 0, elements.length).collect(Collectors.toList());
            return new com.clickhouse.jdbc.types.Array(list, typeName, JdbcUtils.convertToSqlType(ClickHouseDataType.valueOf(typeName)).getVendorTypeNumber());
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
        ensureOpen();
        this.schema = schema;
        defaultQuerySettings.setDatabase(this.schema);
    }

    @Override
    public String getSchema() throws SQLException {
        ensureOpen();
        return schema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (executor == null) {
            throw new SQLException("Executor must be not null");
        }
        // This method should check permissions with SecurityManager but the one is deprecated.
        // There is no replacement for SecurityManger and it is marked for removal.
        this.close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        ensureOpen();

        // Very good mail thread about this method implementation. https://mail.openjdk.org/pipermail/jdbc-spec-discuss/2017-November/000236.html

        // This method should check permissions with SecurityManager but the one is deprecated.
        // There is no replacement for SecurityManger and it is marked for removal.
        if (milliseconds > 0 && executor == null) {
            // we need executor only for positive timeout values.
            throw new SQLException("Executor must be not null");
        }
        if (milliseconds < 0) {
            throw new SQLException("Timeout must be >= 0");
        }

        // How it should work:
        // if timeout is set with this method then any timeout exception should be reported to the connection
        // when connection get signal about timeout it uses executor to abort itself
        // Some connection pools set timeout before calling Connection#close() to ensure that this operation will not hang
        // Socket timeout is propagated with QuerySettings this connection has.
        networkTimeoutExecutor = executor;
        defaultQuerySettings.setOption(ClientConfigProperties.SOCKET_OPERATION_TIMEOUT.getKey(), (long)milliseconds);
    }


    // Should be called by child object to notify about timeout.
    public void onNetworkTimeout() throws SQLException {
        if (isClosed() || networkTimeoutExecutor == null) {
            return; // we closed already so do nothing.
        }

        networkTimeoutExecutor.execute(() -> {
            try {
                this.abort(networkTimeoutExecutor);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to abort connection", e);
            }
        });
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        Long networkTimeout = defaultQuerySettings.getNetworkTimeout();
        return networkTimeout == null ? 0 : networkTimeout.intValue();
    }

    /**
     * Returns instance of the client used to execute queries by this connection.
     * @return - client instance
     */
    public Client getClient() throws SQLException {
        ensureOpen();
        return client;
    }

    private void ensureOpen() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is closed", ExceptionUtils.SQL_STATE_CONNECTION_EXCEPTION);
        }
    }
}
