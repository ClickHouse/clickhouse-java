package com.clickhouse.jdbc;

import javax.sql.DataSource;

import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.jdbc.internal.ClickHouseConnectionImpl;
import com.clickhouse.jdbc.internal.ClickHouseJdbcUrlParser;
import com.clickhouse.jdbc.internal.ClickHouseJdbcUrlParser.ConnectionInfo;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

@Deprecated
public class DataSourceV1 extends JdbcWrapper implements DataSource {
    private final String url;
    private final Properties props;

    protected final DriverV1 driver;
    protected final ConnectionInfo connInfo;

    protected PrintWriter printWriter;
    protected int loginTimeoutSeconds = 0;

    public DataSourceV1(String url) throws SQLException {
        this(url, new Properties());
    }

    public DataSourceV1(String url, Properties properties) throws SQLException {
        if (url == null) {
            throw new IllegalArgumentException("Incorrect ClickHouse jdbc url. It must be not null");
        }
        this.url = url;
        this.props = new Properties();
        if (properties != null && !properties.isEmpty()) {
            this.props.putAll(properties);
        }

        this.driver = new DriverV1();
        this.connInfo = ClickHouseJdbcUrlParser.parse(url, properties);
    }

    @Override
    public ClickHouseConnection getConnection() throws SQLException {
        return new ClickHouseConnectionImpl(connInfo);
    }

    @Override
    public ClickHouseConnection getConnection(String username, String password) throws SQLException {
        if (username == null || username.isEmpty()) {
            throw SqlExceptionUtils.clientError("Non-empty user name is required");
        }

        if (password == null) {
            password = "";
        }

        if (username.equals(props.getProperty(ClickHouseDefaults.USER.getKey()))
                && password.equals(props.getProperty(ClickHouseDefaults.PASSWORD.getKey()))) {
            return new ClickHouseConnectionImpl(connInfo);
        }

        Properties properties = new Properties();
        properties.putAll(this.props);
        properties.setProperty(ClickHouseDefaults.USER.getKey(), username);
        properties.setProperty(ClickHouseDefaults.PASSWORD.getKey(), password);
        return new ClickHouseConnectionImpl(url, properties);
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
        return DriverV1.parentLogger;
    }
}
