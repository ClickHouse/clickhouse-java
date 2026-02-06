package com.clickhouse.jdbc.dispatcher.proxy;

import com.clickhouse.jdbc.dispatcher.DispatcherException;
import com.clickhouse.jdbc.dispatcher.DriverVersion;
import com.clickhouse.jdbc.dispatcher.loader.DriverVersionManager;
import com.clickhouse.jdbc.dispatcher.strategy.RetryContext;
import com.clickhouse.jdbc.dispatcher.strategy.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Proxy wrapper for java.sql.Connection that supports failover between driver versions.
 * <p>
 * This proxy maintains connections to multiple driver versions and can switch between
 * them when failures occur. It tracks which connection is currently active and manages
 * the lifecycle of all connections.
 */
public class ConnectionProxy implements Connection {

    private static final Logger log = LoggerFactory.getLogger(ConnectionProxy.class);

    private final DriverVersionManager versionManager;
    private final String url;
    private final Properties properties;
    private final RetryStrategy retryStrategy;

    // Map of version -> connection for that version
    private final Map<String, Connection> versionConnections = new ConcurrentHashMap<>();

    // Current active version and connection
    private volatile DriverVersion currentVersion;
    private volatile Connection currentConnection;
    private volatile boolean closed = false;

    /**
     * Creates a new ConnectionProxy.
     *
     * @param versionManager the manager containing all driver versions
     * @param url            the JDBC URL used for connections
     * @param properties     the connection properties
     * @param retryStrategy  the retry strategy for failover
     * @param initialVersion the initial driver version to use
     * @param initialConn    the initial connection
     */
    public ConnectionProxy(DriverVersionManager versionManager, String url, Properties properties,
                           RetryStrategy retryStrategy, DriverVersion initialVersion, Connection initialConn) {
        this.versionManager = versionManager;
        this.url = url;
        this.properties = properties;
        this.retryStrategy = retryStrategy;
        this.currentVersion = initialVersion;
        this.currentConnection = initialConn;
        this.versionConnections.put(initialVersion.getVersion(), initialConn);
    }

    /**
     * Returns the list of available driver versions.
     */
    public List<DriverVersion> getAvailableVersions() {
        return versionManager.getVersions();
    }

    /**
     * Gets or creates a connection for the specified driver version.
     *
     * @param version the driver version
     * @return a connection for that version
     * @throws SQLException if connection cannot be established
     */
    public Connection getConnectionForVersion(DriverVersion version) throws SQLException {
        String versionStr = version.getVersion();

        // Check if we already have a connection for this version
        Connection conn = versionConnections.get(versionStr);
        if (conn != null && !conn.isClosed()) {
            return conn;
        }

        // Create a new connection using the target driver version
        log.info("Creating new connection for driver version {}", versionStr);
        try {
            conn = version.getDriver().connect(url, properties);
            if (conn == null) {
                throw new SQLException("Driver returned null connection for URL: " + url);
            }
            versionConnections.put(versionStr, conn);
            return conn;
        } catch (SQLException e) {
            log.error("Failed to create connection with driver version {}: {}",
                    versionStr, e.getMessage());
            throw e;
        }
    }

    /**
     * Switches to a different driver version, creating a connection if needed.
     *
     * @param version the version to switch to
     * @throws SQLException if switching fails
     */
    public void switchToVersion(DriverVersion version) throws SQLException {
        if (version.equals(currentVersion)) {
            return;
        }

        Connection newConn = getConnectionForVersion(version);
        this.currentVersion = version;
        this.currentConnection = newConn;
        log.info("Switched to driver version {}", version.getVersion());
    }

    /**
     * Returns the current driver version.
     */
    public DriverVersion getCurrentVersion() {
        return currentVersion;
    }

    /**
     * Returns the current underlying connection.
     */
    public Connection getCurrentConnection() {
        return currentConnection;
    }

    /**
     * Returns the retry strategy.
     */
    public RetryStrategy getRetryStrategy() {
        return retryStrategy;
    }

    // ==================== Statement Creation with Retry Support ====================

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        Statement stmt = currentConnection.createStatement();
        return new StatementProxy(this, stmt, currentVersion, retryStrategy);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        List<DispatcherException.VersionFailure> failures = new ArrayList<>();
        List<DriverVersion> versionsToTry = retryStrategy.getVersionsToTry(
                versionManager.getVersions(),
                new RetryContext(RetryContext.OperationType.PREPARE_STATEMENT, "prepareStatement", 1)
        );

        for (int attempt = 0; attempt < versionsToTry.size(); attempt++) {
            DriverVersion version = versionsToTry.get(attempt);
            RetryContext context = new RetryContext(
                    RetryContext.OperationType.PREPARE_STATEMENT, "prepareStatement", attempt + 1
            );

            try {
                Connection conn = getConnectionForVersion(version);
                PreparedStatement pstmt = conn.prepareStatement(sql);
                retryStrategy.onSuccess(version, context);
                return new PreparedStatementProxy(this, pstmt, version, retryStrategy, sql);
            } catch (SQLException e) {
                log.warn("prepareStatement failed with version {}: {}", version.getVersion(), e.getMessage());
                failures.add(new DispatcherException.VersionFailure(version.getVersion(), e));
                retryStrategy.onFailure(version, context, e);

                if (attempt == versionsToTry.size() - 1) {
                    throw new DispatcherException("All driver versions failed for prepareStatement", failures);
                }
            }
        }

        throw new DispatcherException("No driver versions available", failures);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        checkClosed();
        return currentConnection.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        return currentConnection.nativeSQL(sql);
    }

    // ==================== Transaction Management ====================

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        // Apply to all open connections
        for (Connection conn : versionConnections.values()) {
            if (!conn.isClosed()) {
                conn.setAutoCommit(autoCommit);
            }
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return currentConnection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        // Commit current connection
        currentConnection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        // Rollback current connection
        currentConnection.rollback();
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;

        // Close all connections
        List<SQLException> exceptions = new ArrayList<>();
        for (Connection conn : versionConnections.values()) {
            try {
                if (!conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                exceptions.add(e);
            }
        }
        versionConnections.clear();

        if (!exceptions.isEmpty()) {
            SQLException first = exceptions.get(0);
            for (int i = 1; i < exceptions.size(); i++) {
                first.addSuppressed(exceptions.get(i));
            }
            throw first;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }

    // ==================== Metadata ====================

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return currentConnection.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        for (Connection conn : versionConnections.values()) {
            if (!conn.isClosed()) {
                conn.setReadOnly(readOnly);
            }
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return currentConnection.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        for (Connection conn : versionConnections.values()) {
            if (!conn.isClosed()) {
                conn.setCatalog(catalog);
            }
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return currentConnection.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        for (Connection conn : versionConnections.values()) {
            if (!conn.isClosed()) {
                conn.setTransactionIsolation(level);
            }
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return currentConnection.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return currentConnection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        currentConnection.clearWarnings();
    }

    // ==================== Statement Creation Variants ====================

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        Statement stmt = currentConnection.createStatement(resultSetType, resultSetConcurrency);
        return new StatementProxy(this, stmt, currentVersion, retryStrategy);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        checkClosed();
        PreparedStatement pstmt = currentConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
        return new PreparedStatementProxy(this, pstmt, currentVersion, retryStrategy, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        checkClosed();
        return currentConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkClosed();
        return currentConnection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        currentConnection.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        for (Connection conn : versionConnections.values()) {
            if (!conn.isClosed()) {
                conn.setHoldability(holdability);
            }
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return currentConnection.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        checkClosed();
        return currentConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkClosed();
        return currentConnection.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkClosed();
        currentConnection.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkClosed();
        currentConnection.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        checkClosed();
        Statement stmt = currentConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        return new StatementProxy(this, stmt, currentVersion, retryStrategy);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                               int resultSetHoldability) throws SQLException {
        checkClosed();
        PreparedStatement pstmt = currentConnection.prepareStatement(sql, resultSetType, resultSetConcurrency,
                resultSetHoldability);
        return new PreparedStatementProxy(this, pstmt, currentVersion, retryStrategy, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                          int resultSetHoldability) throws SQLException {
        checkClosed();
        return currentConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        PreparedStatement pstmt = currentConnection.prepareStatement(sql, autoGeneratedKeys);
        return new PreparedStatementProxy(this, pstmt, currentVersion, retryStrategy, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        PreparedStatement pstmt = currentConnection.prepareStatement(sql, columnIndexes);
        return new PreparedStatementProxy(this, pstmt, currentVersion, retryStrategy, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        PreparedStatement pstmt = currentConnection.prepareStatement(sql, columnNames);
        return new PreparedStatementProxy(this, pstmt, currentVersion, retryStrategy, sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        checkClosed();
        return currentConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkClosed();
        return currentConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkClosed();
        return currentConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkClosed();
        return currentConnection.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (closed) {
            return false;
        }
        return currentConnection.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        for (Connection conn : versionConnections.values()) {
            try {
                if (!conn.isClosed()) {
                    conn.setClientInfo(name, value);
                }
            } catch (SQLException e) {
                // Ignore closed connections
            }
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        for (Connection conn : versionConnections.values()) {
            try {
                if (!conn.isClosed()) {
                    conn.setClientInfo(properties);
                }
            } catch (SQLException e) {
                // Ignore closed connections
            }
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return currentConnection.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return currentConnection.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkClosed();
        return currentConnection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkClosed();
        return currentConnection.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        for (Connection conn : versionConnections.values()) {
            if (!conn.isClosed()) {
                conn.setSchema(schema);
            }
        }
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return currentConnection.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        closed = true;
        for (Connection conn : versionConnections.values()) {
            conn.abort(executor);
        }
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkClosed();
        for (Connection conn : versionConnections.values()) {
            if (!conn.isClosed()) {
                conn.setNetworkTimeout(executor, milliseconds);
            }
        }
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkClosed();
        return currentConnection.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        return currentConnection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this) || currentConnection.isWrapperFor(iface);
    }
}
