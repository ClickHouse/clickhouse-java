package com.clickhouse.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;

public abstract class JdbcIntegrationTest extends BaseIntegrationTest {
    private static final String CLASS_PREFIX = "ClickHouse";
    private static final String CLASS_SUFFIX = "Test";

    protected static final ClickHouseProtocol DEFAULT_PROTOCOL = ClickHouseProtocol
            .valueOf(System.getProperty("protocol", "http").toUpperCase());

    protected final String dbName;

    protected String buildJdbcUrl(ClickHouseProtocol protocol, String prefix, String url) {
        if (url != null && url.startsWith("jdbc:")) {
            return url;
        }

        if (protocol == null) {
            protocol = DEFAULT_PROTOCOL;
        }

        StringBuilder builder = new StringBuilder();
        if (prefix == null || prefix.isEmpty()) {
            builder.append("jdbc:clickhouse:").append(protocol.name().toLowerCase()).append("://");
        } else if (!prefix.startsWith("jdbc:")) {
            builder.append("jdbc:").append(prefix);
        } else {
            builder.append(prefix);
        }

        builder.append(getServerAddress(protocol));

        if (url != null && !url.isEmpty()) {
            if (url.charAt(0) != '/') {
                builder.append('/');
            }

            builder.append(url);
        }

        return builder.toString();
    }

    public JdbcIntegrationTest() {
        String className = getClass().getSimpleName();
        if (className.startsWith(CLASS_PREFIX)) {
            className = className.substring(CLASS_PREFIX.length());
        }
        if (className.endsWith(CLASS_SUFFIX)) {
            className = className.substring(0, className.length() - CLASS_SUFFIX.length());
        }

        this.dbName = "test_" + className.toLowerCase(Locale.ROOT);
    }

    public String getServerAddress(ClickHouseProtocol protocol) {
        return getServerAddress(protocol, false);
    }

    public String getServerAddress(ClickHouseProtocol protocol, boolean useIPaddress) {
        ClickHouseNode server = getServer(protocol);

        return new StringBuilder().append(useIPaddress ? getIpAddress(server) : server.getHost()).append(':')
                .append(server.getPort()).toString();
    }

    public String getServerAddress(ClickHouseProtocol protocol, String customHostOrIp) {
        ClickHouseNode server = getServer(protocol);
        return new StringBuilder()
                .append(customHostOrIp == null || customHostOrIp.isEmpty() ? server.getHost() : customHostOrIp)
                .append(':').append(server.getPort()).toString();
    }

    public ClickHouseDataSource newDataSource() throws SQLException {
        return newDataSource(null, new Properties());
    }

    public ClickHouseDataSource newDataSource(Properties properties) throws SQLException {
        return newDataSource(null, properties);
    }

    public ClickHouseDataSource newDataSource(String url) throws SQLException {
        return newDataSource(url, new Properties());
    }

    public ClickHouseDataSource newDataSource(String url, Properties properties) throws SQLException {
        return new ClickHouseDataSource(buildJdbcUrl(DEFAULT_PROTOCOL, null, url), properties);
    }

    public ClickHouseConnection newConnection() throws SQLException {
        return newConnection(null);
    }

    public ClickHouseConnection newConnection(Properties properties) throws SQLException {
        try (ClickHouseConnection conn = newDataSource(properties).getConnection();
                ClickHouseStatement stmt = conn.createStatement();) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName);
        }

        return newDataSource(dbName, properties == null ? new Properties() : properties).getConnection();
    }

    public Connection newMySqlConnection(Properties properties) throws SQLException {
        if (properties == null) {
            properties = new Properties();
        }

        if (!properties.containsKey("user")) {
            properties.setProperty("user", "default");
        }
        if (!properties.containsKey("password")) {
            properties.setProperty("password", "");
        }

        Connection conn = DriverManager.getConnection(buildJdbcUrl(ClickHouseProtocol.MYSQL, "jdbc:mysql://", dbName),
                properties);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName);
        }

        return conn;
    }

    public void closeConnection(Connection conn) throws SQLException {
        if (conn == null) {
            return;
        }

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + dbName);
        } finally {
            conn.close();
        }
    }
}
