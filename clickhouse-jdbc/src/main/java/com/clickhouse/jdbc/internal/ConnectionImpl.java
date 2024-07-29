package com.clickhouse.jdbc.internal;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseTransaction;
import com.clickhouse.client.api.Client;
import com.clickhouse.data.ClickHouseVersion;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDatabaseMetaData;
import com.clickhouse.jdbc.ClickHouseStatement;
import com.clickhouse.jdbc.JdbcConfig;
import com.clickhouse.jdbc.parser.ClickHouseSqlStatement;

import java.io.Serializable;
import java.net.URI;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.util.Calendar;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Stack;
import java.util.TimeZone;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connection implementation to interface with underlying ClickHouse client.
 */
public class ConnectionImpl implements ClickHouseConnection {

    private final String jdbcUrl;
    private final Properties config;

    // State
    private AtomicBoolean closed = new AtomicBoolean(false);

    private AtomicBoolean autoCommit = new AtomicBoolean(true);

    private AtomicBoolean readOnly = new AtomicBoolean(false);

    private String catalog;

    private SQLWarning warnings;

    private short resultSetHoldability = -1;

    private Client client;

    public ConnectionImpl(String jdbcUrl, Properties config) {
        this.jdbcUrl = jdbcUrl;
        this.config = config;
        this.catalog = config.getProperty("database", "default");
        this.warnings = null;
    }


    @Override
    public String nativeSQL(String sql) throws SQLException {
        ensureOpen();
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        ensureOpen();
        if (!autoCommit) {
            throw new IllegalArgumentException("ClickHouse does not support transactions");
        }
        //TODO: implement transaction support
        this.autoCommit.set(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        ensureOpen();
        return autoCommit.get();
    }

    @Override
    public void commit() throws SQLException {
        ensureOpen();
        if (autoCommit.get()) {
            throw new SQLException("Cannot commit in auto-commit mode");
        }

    }

    @Override
    public void rollback() throws SQLException {
        ensureOpen();
        if (autoCommit.get()) {
            throw new SQLException("Cannot rollback in auto-commit mode");
        }
    }

    @Override
    public void close() throws SQLException {
        if (closed.get()) {
            return;
        }
        // TODO: close underlying client
        // see javadoc for Connection#close
        // https://docs.oracle.com/javase/8/docs/api/java/sql/Connection.html#close--
        // this method should not commit any transactions because it is application responsibility only
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed.get();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        ensureOpen();
        return new ClickHouseDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        ensureOpen();
        this.readOnly.set(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        ensureOpen();
        return readOnly.get();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        ensureOpen();
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        return catalog;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        ensureOpen();
        throw new SQLException("ClickHouse does not support transactions");
        // TODO: implement transaction support
        // Note: NONE is special case that should not be used
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        // TODO: implement transaction support
        return TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return warnings;
    }

    @Override
    public synchronized void clearWarnings() throws SQLException {
        SQLWarning current = warnings;
        warnings = null;
        Stack<SQLWarning> warningStack = new Stack<>();
        while (current != null) {
            warningStack.push(current);
            current = current.getNextWarning();
        }
        while (!warningStack.isEmpty()) {
            warningStack.pop().setNextWarning(null);
        }
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
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            /* TODO: support holdability and document it. Check ResultSet interface for what operations should be
             * supported in open stata
             */
            throw new SQLFeatureNotSupportedException("Only ResultSet.CLOSE_CURSORS_AT_COMMIT is supported");
        }
        resultSetHoldability = (short) holdability;
    }

    @Override
    public int getHoldability() throws SQLException {
        return resultSetHoldability;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }


    @Override
    public ClickHouseStatement createStatement(int resultSetType, int resultSetConcurrency,
                                               int resultSetHoldability) throws SQLException {
        ensureOpen();
        return new StatementImpl(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        // TODO: send a ping and remember the last time it was successful
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        //TODO: read javadoc
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        //TODO: read javadoc
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
    public void setSchema(String schema) throws SQLException {
        // ignore - currently is not supported
    }

    @Override
    public String getSchema() throws SQLException {
        return null; // not supported
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        // TODO: implement abort
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        // TODO: implement network timeout as described by javadoc
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    //------------------------------------------ClickHouseConnection Implementation ----------------------------------

    @Override
    public void begin() throws SQLException {

    }

    @Override
    public ClickHouseConfig getConfig() {
        return null;
    }

    @Override
    public boolean allowCustomSetting() {
        return false;
    }

    @Override
    public String getCurrentDatabase() {
        return null;
    }

    @Override
    public void setCurrentDatabase(String database, boolean check) throws SQLException {

    }

    @Override
    public String getCurrentUser() {
        return null;
    }

    @Override
    public Calendar getDefaultCalendar() {
        return null;
    }

    @Override
    public Optional<TimeZone> getEffectiveTimeZone() {
        return Optional.empty();
    }

    @Override
    public TimeZone getJvmTimeZone() {
        return null;
    }

    @Override
    public TimeZone getServerTimeZone() {
        return null;
    }

    @Override
    public ClickHouseVersion getServerVersion() {
        return null;
    }

    @Override
    public ClickHouseTransaction getTransaction() {
        return null;
    }

    @Override
    public URI getUri() {
        return null;
    }

    @Override
    public JdbcConfig getJdbcConfig() {
        return null;
    }

    @Override
    public long getMaxInsertBlockSize() {
        return 0;
    }

    @Override
    public boolean isTransactionSupported() {
        return false;
    }

    @Override
    public boolean isImplicitTransactionSupported() {
        return false;
    }

    @Override
    public String newQueryId() {
        return null;
    }

    @Override
    public ClickHouseSqlStatement[] parse(String sql, ClickHouseConfig config, Map<String, Serializable> settings) {
        return new ClickHouseSqlStatement[0];
    }

    //------------------------------------------ java.sql.Wrapper Implementation ----------------------------------

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    //------------------------------------------ End Interfaces Implementation ----------------------------------

    private void ensureOpen() throws SQLException {
        if (closed.get()) {
            throw new SQLException("Connection is closed");
        }
    }
}
