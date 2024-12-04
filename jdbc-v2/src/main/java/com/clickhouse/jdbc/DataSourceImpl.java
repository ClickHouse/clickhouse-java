package com.clickhouse.jdbc;

import com.clickhouse.jdbc.internal.ExceptionUtils;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ConnectionBuilder;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.ShardingKeyBuilder;
import java.util.Properties;
import java.util.logging.Logger;

public class DataSourceImpl implements DataSource, JdbcV2Wrapper {
    private static final Logger log = Logger.getLogger(DataSourceImpl.class.getName());
    private String url;
    private Properties info;
    private final Driver driver;
    private PrintWriter logWriter;

    public void setUrl(String url) {
        this.url = url;
    }

    private Properties getProperties() {
        Properties copy = new Properties();
        copy.putAll(info);
        return copy;
    }

    public void setProperties(Properties info) {
        this.info = info;
    }

    
    public DataSourceImpl() {//No-arg constructor required by the standard
        this(null, new Properties());
    }

    public DataSourceImpl(String url, Properties info) {
        this.url = url;
        this.info = info;
        this.driver = new Driver(this);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return driver.connect(this.url, this.info);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        Properties info = getProperties();
        info.setProperty("user", username);
        info.setProperty("password", password);

        return driver.connect(this.url, info);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ConnectionBuilder createConnectionBuilder() throws SQLException {
        return DataSource.super.createConnectionBuilder();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported", ExceptionUtils.SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    @Override
    public ShardingKeyBuilder createShardingKeyBuilder() throws SQLException {
        return DataSource.super.createShardingKeyBuilder();
    }
}
