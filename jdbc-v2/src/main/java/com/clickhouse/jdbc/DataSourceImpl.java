package com.clickhouse.jdbc;

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
    private String url;
    private Properties info;
    private Driver driver;

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

    public DataSourceImpl() {
        this(null, new Properties());
    }

    public DataSourceImpl(String url) {
        this(url, new Properties());
    }

    public DataSourceImpl(String url, Properties info) {
        this.url = url;
        this.info = info;
        this.driver = new Driver();
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
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public ConnectionBuilder createConnectionBuilder() throws SQLException {
        return DataSource.super.createConnectionBuilder();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public ShardingKeyBuilder createShardingKeyBuilder() throws SQLException {
        return DataSource.super.createShardingKeyBuilder();
    }
}
