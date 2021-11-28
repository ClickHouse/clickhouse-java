package com.clickhouse.jdbc;

import javax.sql.DataSource;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.jdbc.internal.ClickHouseJdbcUrlParser;
import com.clickhouse.jdbc.internal.ClickHouseJdbcUrlParser.ConnectionInfo;

import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class ClickHouseDataSource extends Wrapper implements DataSource {
    private final String url;

    protected final ClickHouseDriver driver = new ClickHouseDriver();

    protected final Properties properties;
    protected final ClickHouseNode server;
    protected final URI uri;

    protected PrintWriter printWriter;
    protected int loginTimeoutSeconds = 0;

    public ClickHouseDataSource(String url) {
        this(url, new Properties());
    }

    public ClickHouseDataSource(String url, Properties properties) {
        if (url == null) {
            throw new IllegalArgumentException("Incorrect ClickHouse jdbc url. It must be not null");
        }
        this.url = url;

        try {
            ConnectionInfo connInfo = ClickHouseJdbcUrlParser.parse(url, properties);
            this.properties = connInfo.getProperties();
            this.server = connInfo.getServer();
            this.uri = connInfo.getUri();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public ClickHouseConnection getConnection() throws SQLException {
        return driver.connect(url, properties);
    }

    @Override
    public ClickHouseConnection getConnection(String username, String password) throws SQLException {
        if (username == null || username.isEmpty()) {
            throw SqlExceptionUtils.clientError("Non-empty user name is required");
        }

        if (password == null) {
            password = "";
        }

        Properties props = new Properties(properties);
        props.setProperty(ClickHouseDefaults.USER.getKey(), username);
        props.setProperty(ClickHouseDefaults.PASSWORD.getKey(), password);
        return driver.connect(url, props);
    }

    public String getHost() {
        return server.getHost();
    }

    public int getPort() {
        return server.getPort();
    }

    public String getDatabase() {
        return server.getDatabase().orElse((String) ClickHouseDefaults.DATABASE.getEffectiveDefaultValue());
    }

    // public String getUrl() {
    // return url;
    // }

    public Properties getProperties() {
        return properties;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return printWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        printWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        loginTimeoutSeconds = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeoutSeconds;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return ClickHouseDriver.parentLogger;
    }
}
