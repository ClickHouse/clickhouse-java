package com.clickhouse.jdbc;

import com.clickhouse.client.api.Client;
import com.clickhouse.jdbc.internal.JdbcConfiguration;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class ConnectionImpl implements Connection, JdbcWrapper {
    private static final Logger log = LoggerFactory.getLogger(ConnectionImpl.class);

    private final Client client;
    private final JdbcConfiguration config;

    private boolean closed = false;
    private String catalog;
    private String schema;

    public ConnectionImpl(String url, Properties info) {
        this.config = new JdbcConfiguration(url, info);
        this.client = new Client.Builder()
                .addEndpoint(config.getProtocol() + "://" + config.getHost() + ":" + config.getPort())
                .setUsername(config.getUser())
                .setPassword(config.getPassword())
                .compressServerResponse(true)
                .setDefaultDatabase(config.getDatabase())
                .build();
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
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAutoCommit not supported");
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
        return new com.clickhouse.jdbc.metadata.DatabaseMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkOpen();
        if (!readOnly) {
            throw new SQLFeatureNotSupportedException("read-only=false unsupported");
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkOpen();
        return true;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkOpen();
        this.catalog = catalog;
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
        throw new SQLClientInfoException("ClientInfo not supported", null);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException("ClientInfo not supported", null);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkOpen();
        return new Properties();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        //TODO: Should this be supported?
        return null;
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
