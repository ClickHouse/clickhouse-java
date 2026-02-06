package com.clickhouse.jdbc.dispatcher.proxy;

import com.clickhouse.jdbc.dispatcher.DispatcherException;
import com.clickhouse.jdbc.dispatcher.DriverVersion;
import com.clickhouse.jdbc.dispatcher.strategy.RetryContext;
import com.clickhouse.jdbc.dispatcher.strategy.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Proxy wrapper for java.sql.Statement that supports failover between driver versions.
 * <p>
 * This proxy implements retry logic for statement operations. When an operation fails,
 * it can attempt the same operation using a different driver version according to the
 * configured retry strategy.
 */
public class StatementProxy implements Statement {

    private static final Logger log = LoggerFactory.getLogger(StatementProxy.class);

    protected final ConnectionProxy connectionProxy;
    protected volatile Statement delegate;
    protected volatile DriverVersion currentVersion;
    protected final RetryStrategy retryStrategy;

    /**
     * Creates a new StatementProxy.
     *
     * @param connectionProxy the parent connection proxy
     * @param delegate        the underlying statement
     * @param currentVersion  the driver version that created this statement
     * @param retryStrategy   the retry strategy to use for failover
     */
    public StatementProxy(ConnectionProxy connectionProxy, Statement delegate,
                          DriverVersion currentVersion, RetryStrategy retryStrategy) {
        this.connectionProxy = connectionProxy;
        this.delegate = delegate;
        this.currentVersion = currentVersion;
        this.retryStrategy = retryStrategy;
    }

    /**
     * Executes a query with retry support across driver versions.
     */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        List<DispatcherException.VersionFailure> failures = new ArrayList<>();
        List<DriverVersion> versionsToTry = retryStrategy.getVersionsToTry(
                connectionProxy.getAvailableVersions(),
                RetryContext.forQuery(sql, 1)
        );

        for (int attempt = 0; attempt < versionsToTry.size(); attempt++) {
            DriverVersion version = versionsToTry.get(attempt);
            RetryContext context = RetryContext.forQuery(sql, attempt + 1);

            try {
                // Switch to the target version if needed
                Statement stmt = getStatementForVersion(version);
                ResultSet rs = stmt.executeQuery(sql);
                retryStrategy.onSuccess(version, context);
                return new ResultSetProxy(rs, version, this);
            } catch (SQLException e) {
                log.warn("executeQuery failed with version {}: {}", version.getVersion(), e.getMessage());
                failures.add(new DispatcherException.VersionFailure(version.getVersion(), e));
                retryStrategy.onFailure(version, context, e);

                // If this was the last version, throw the aggregated exception
                if (attempt == versionsToTry.size() - 1) {
                    throw new DispatcherException("All driver versions failed for executeQuery", failures);
                }
            }
        }

        throw new DispatcherException("No driver versions available", failures);
    }

    /**
     * Executes an update with retry support across driver versions.
     */
    @Override
    public int executeUpdate(String sql) throws SQLException {
        List<DispatcherException.VersionFailure> failures = new ArrayList<>();
        List<DriverVersion> versionsToTry = retryStrategy.getVersionsToTry(
                connectionProxy.getAvailableVersions(),
                RetryContext.forUpdate(sql, 1)
        );

        for (int attempt = 0; attempt < versionsToTry.size(); attempt++) {
            DriverVersion version = versionsToTry.get(attempt);
            RetryContext context = RetryContext.forUpdate(sql, attempt + 1);

            try {
                Statement stmt = getStatementForVersion(version);
                int result = stmt.executeUpdate(sql);
                retryStrategy.onSuccess(version, context);
                return result;
            } catch (SQLException e) {
                log.warn("executeUpdate failed with version {}: {}", version.getVersion(), e.getMessage());
                failures.add(new DispatcherException.VersionFailure(version.getVersion(), e));
                retryStrategy.onFailure(version, context, e);

                if (attempt == versionsToTry.size() - 1) {
                    throw new DispatcherException("All driver versions failed for executeUpdate", failures);
                }
            }
        }

        throw new DispatcherException("No driver versions available", failures);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        List<DispatcherException.VersionFailure> failures = new ArrayList<>();
        List<DriverVersion> versionsToTry = retryStrategy.getVersionsToTry(
                connectionProxy.getAvailableVersions(),
                new RetryContext(RetryContext.OperationType.OTHER, "execute", 1)
        );

        for (int attempt = 0; attempt < versionsToTry.size(); attempt++) {
            DriverVersion version = versionsToTry.get(attempt);
            RetryContext context = new RetryContext(RetryContext.OperationType.OTHER, "execute", attempt + 1);

            try {
                Statement stmt = getStatementForVersion(version);
                boolean result = stmt.execute(sql);
                retryStrategy.onSuccess(version, context);
                return result;
            } catch (SQLException e) {
                log.warn("execute failed with version {}: {}", version.getVersion(), e.getMessage());
                failures.add(new DispatcherException.VersionFailure(version.getVersion(), e));
                retryStrategy.onFailure(version, context, e);

                if (attempt == versionsToTry.size() - 1) {
                    throw new DispatcherException("All driver versions failed for execute", failures);
                }
            }
        }

        throw new DispatcherException("No driver versions available", failures);
    }

    /**
     * Gets or creates a statement for the specified driver version.
     */
    protected Statement getStatementForVersion(DriverVersion version) throws SQLException {
        if (version.equals(currentVersion)) {
            return delegate;
        }

        // Get connection for the target version and create a new statement
        Connection conn = connectionProxy.getConnectionForVersion(version);
        Statement newStmt = conn.createStatement();

        // Copy statement properties
        try {
            newStmt.setQueryTimeout(delegate.getQueryTimeout());
            newStmt.setMaxRows(delegate.getMaxRows());
            newStmt.setFetchSize(delegate.getFetchSize());
        } catch (SQLException e) {
            log.debug("Could not copy all statement properties: {}", e.getMessage());
        }

        // Update current delegate
        this.delegate = newStmt;
        this.currentVersion = version;

        return newStmt;
    }

    /**
     * Returns the underlying delegate statement.
     */
    public Statement getDelegate() {
        return delegate;
    }

    /**
     * Returns the current driver version.
     */
    public DriverVersion getCurrentVersion() {
        return currentVersion;
    }

    // ==================== Statement Interface Implementation ====================
    // Non-retry operations delegate directly

    @Override
    public void close() throws SQLException {
        delegate.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return delegate.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        delegate.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return delegate.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        delegate.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        delegate.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return delegate.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        delegate.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        delegate.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return delegate.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        delegate.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        delegate.setCursorName(name);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        ResultSet rs = delegate.getResultSet();
        return rs != null ? new ResultSetProxy(rs, currentVersion, this) : null;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return delegate.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return delegate.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        delegate.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return delegate.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        delegate.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return delegate.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return delegate.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return delegate.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        delegate.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        delegate.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return delegate.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connectionProxy;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return delegate.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        ResultSet rs = delegate.getGeneratedKeys();
        return rs != null ? new ResultSetProxy(rs, currentVersion, this) : null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return delegate.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return delegate.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return delegate.executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return delegate.execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return delegate.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return delegate.execute(sql, columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return delegate.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return delegate.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        delegate.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return delegate.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        delegate.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return delegate.isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this) || delegate.isWrapperFor(iface);
    }
}
