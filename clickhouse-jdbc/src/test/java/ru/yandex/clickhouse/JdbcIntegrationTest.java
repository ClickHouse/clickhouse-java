package ru.yandex.clickhouse;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import ru.yandex.clickhouse.settings.ClickHouseProperties;
import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;

public abstract class JdbcIntegrationTest extends BaseIntegrationTest {
    private static final String CLASS_PREFIX = "ClickHouse";
    private static final String CLASS_SUFFIX = "Test";

    protected final String dbName;

    public JdbcIntegrationTest() {
        String className = getClass().getSimpleName();
        if (className.startsWith(CLASS_PREFIX)) {
            className = className.substring(CLASS_PREFIX.length());
        }
        if (className.endsWith(CLASS_SUFFIX)) {
            className = className.substring(0, className.length() - CLASS_SUFFIX.length());
        }

        this.dbName = "test_" + className.toLowerCase();
    }

    public String getClickHouseHttpAddress() {
        return getClickHouseHttpAddress(false);
    }

    public String getClickHouseHttpAddress(boolean useIPaddress) {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);

        String ipAddress = server.getHost();
        try {
            ipAddress = InetAddress.getByName(ipAddress).getHostAddress();
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(
                    String.format("Not able to resolve %s to get its IP address", server.getHost()), e);
        }
        return new StringBuilder().append(useIPaddress ? ipAddress : server.getHost()).append(':')
                .append(server.getPort()).toString();
    }

    public String getClickHouseHttpAddress(String customHostOrIp) {
        ClickHouseNode server = getServer(ClickHouseProtocol.HTTP);
        return new StringBuilder()
                .append(customHostOrIp == null || customHostOrIp.isEmpty() ? server.getHost() : customHostOrIp)
                .append(':').append(server.getPort()).toString();
    }

    public ClickHouseDataSource newDataSource() {
        return newDataSource(new ClickHouseProperties());
    }

    public ClickHouseDataSource newDataSource(ClickHouseProperties properties) {
        return newDataSource("jdbc:clickhouse://" + getClickHouseHttpAddress(), properties);
    }

    public ClickHouseDataSource newDataSource(String url) {
        return newDataSource(url, new ClickHouseProperties());
    }

    public ClickHouseDataSource newDataSource(String url, ClickHouseProperties properties) {
        String baseUrl = "jdbc:clickhouse://" + getClickHouseHttpAddress();
        if (url == null) {
            url = baseUrl;
        } else if (!url.startsWith("jdbc:")) {
            url = baseUrl + "/" + url;
        }

        return new ClickHouseDataSource(url, properties);
    }

    public BalancedClickhouseDataSource newBalancedDataSource(String... addresses) {
        return newBalancedDataSource(new ClickHouseProperties(), addresses);
    }

    public BalancedClickhouseDataSource newBalancedDataSource(ClickHouseProperties properties, String... addresses) {
        return newBalancedDataSourceWithSuffix(null, properties, addresses);
    }

    public BalancedClickhouseDataSource newBalancedDataSourceWithSuffix(String urlSuffix,
            ClickHouseProperties properties, String... addresses) {
        StringBuilder url = new StringBuilder().append("jdbc:clickhouse://");
        if (addresses == null || addresses.length == 0) {
            url.append(getClickHouseHttpAddress());
        } else {
            int position = url.length();
            for (int i = 0; i < addresses.length; i++) {
                url.append(',').append(addresses[i]);
            }
            url.deleteCharAt(position);
        }

        if (urlSuffix != null) {
            url.append('/').append(urlSuffix);
        }

        return new BalancedClickhouseDataSource(url.toString(), properties);
    }

    public ClickHouseConnection newConnection() throws SQLException {
        return newConnection(null);
    }

    public ClickHouseConnection newConnection(ClickHouseProperties properties) throws SQLException {
        try (ClickHouseConnection conn = newDataSource().getConnection();
                ClickHouseStatement stmt = conn.createStatement();) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName);
        }

        return newDataSource(dbName, properties == null ? new ClickHouseProperties() : properties).getConnection();
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
