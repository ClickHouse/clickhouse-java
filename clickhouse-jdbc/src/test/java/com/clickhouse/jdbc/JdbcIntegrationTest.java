package com.clickhouse.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

import com.clickhouse.client.ClickHouseServerForTest;
import org.testng.Assert;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.http.config.ClickHouseHttpOption;

import javax.sql.DataSource;

public abstract class JdbcIntegrationTest extends BaseIntegrationTest {
    private static final String CLASS_PREFIX = "ClickHouse";
    private static final String CLASS_SUFFIX = "Test";

    protected static final String CUSTOM_PROTOCOL_NAME = System.getProperty("protocol", "http").toUpperCase();
    protected static final ClickHouseProtocol DEFAULT_PROTOCOL = ClickHouseProtocol
            .valueOf(CUSTOM_PROTOCOL_NAME.indexOf("HTTP") >= 0 ? "HTTP" : CUSTOM_PROTOCOL_NAME);

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

        if (CUSTOM_PROTOCOL_NAME.indexOf("HTTP") >= 0 && !"HTTP".equals(CUSTOM_PROTOCOL_NAME)) {
            builder.append('?').append(ClickHouseHttpOption.CONNECTION_PROVIDER.getKey()).append('=')
                    .append(CUSTOM_PROTOCOL_NAME);
        }
        return builder.toString();
    }

    protected void checkRowCount(Statement stmt, String queryOrTableName, int expectedRowCount) throws SQLException {
        String sql = queryOrTableName.indexOf(' ') > 0 ? queryOrTableName
                : "select count(1) from ".concat(queryOrTableName);
        try (ResultSet rs = stmt.executeQuery(sql)) {
            Assert.assertTrue(rs.next(), "Should have at least one record");
            Assert.assertEquals(rs.getInt(1), expectedRowCount);
            Assert.assertFalse(rs.next(), "Should have only one record");
        }
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

    public DataSource newDataSource() throws SQLException {
        return newDataSource(null, new Properties());
    }

    public DataSource newDataSource(Properties properties) throws SQLException {
        return newDataSource(null, properties);
    }

    public DataSource newDataSource(String url) throws SQLException {
        return newDataSource(url, new Properties());
    }

    public DataSource newDataSource(String url, Properties properties) throws SQLException {
        if (properties == null) {
            properties = new Properties();
        }
        if (!properties.containsKey("password")) {
            properties.put("password", getPassword());
        }
        if (!properties.containsKey("user")) {
            properties.put("user", "default");
        }

        if (isCloud()) {
            url = String.format("jdbc:clickhouse:https://%s/%s", getServerAddress(ClickHouseProtocol.HTTP), ClickHouseServerForTest.getDatabase());
            return new ClickHouseDataSource(buildJdbcUrl(DEFAULT_PROTOCOL, null, url), properties);
        }
        return new ClickHouseDataSource(buildJdbcUrl(DEFAULT_PROTOCOL, null, url), properties);
    }

    public ClickHouseConnection newConnection() throws SQLException {
        return newConnection(null);
    }

    public ClickHouseConnection newConnection(Properties properties) throws SQLException {
        try (Connection conn = newDataSource(properties).getConnection();
                Statement stmt = conn.createStatement();) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + ClickHouseServerForTest.getDatabase());
        }

        return (ClickHouseConnection) newDataSource(ClickHouseServerForTest.getDatabase(), properties == null ? new Properties() : properties).getConnection();
    }

    public Connection newMySqlConnection(Properties properties) throws SQLException {
        if (properties == null) {
            properties = new Properties();
        }

        if (!properties.containsKey("user")) {
            properties.setProperty("user", "default");
        }
        if (!properties.containsKey("password")) {
            properties.setProperty("password", getPassword());
        }

        String url = buildJdbcUrl(ClickHouseProtocol.MYSQL, "jdbc:mysql://", ClickHouseServerForTest.getDatabase());
        url += url.indexOf('?') > 0 ? "&useSSL="+isCloud() : "?useSSL="+isCloud();
        Connection conn = DriverManager.getConnection(url, properties);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + ClickHouseServerForTest.getDatabase());
        }

        return conn;
    }

    public void closeConnection(Connection conn) throws SQLException {
        if (conn == null) {
            return;
        }

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + ClickHouseServerForTest.getDatabase());
        } finally {
            conn.close();
        }
    }


    public String getEndpointString() {
        return getEndpointString(false);
    }
    public String getEndpointString(boolean includeDbName) {
        return (isCloud() ? "https" : "http") + "://" + getServerAddress(ClickHouseProtocol.HTTP) + "/" + (includeDbName ? ClickHouseServerForTest.getDatabase() : "");
    }
}
