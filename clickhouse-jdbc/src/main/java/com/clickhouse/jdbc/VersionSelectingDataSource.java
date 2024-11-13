package com.clickhouse.jdbc;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

public class VersionSelectingDataSource implements javax.sql.DataSource, com.clickhouse.jdbc.JdbcV2Wrapper {
    private final DataSource dataSource;

    public VersionSelectingDataSource(String url) throws SQLException {
        this(url, new Properties());
    }

    public VersionSelectingDataSource(String url, Properties properties) throws SQLException {
        if (VersionSelectingDriver.isV2(url)) {
            //v2
            this.dataSource = new com.clickhouse.jdbc.DataSourceImpl(url, properties);
        } else {
            //v1
            this.dataSource = new ClickHouseDataSource(url, properties);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return dataSource.getConnection(username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return dataSource.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        dataSource.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        dataSource.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return dataSource.getLoginTimeout();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return dataSource.getParentLogger();
    }
}
