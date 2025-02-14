package com.clickhouse.jdbc;

import com.clickhouse.client.ClickHouseServerForTest;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public abstract class JdbcIntegrationTest extends BaseIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcIntegrationTest.class);

    public String getEndpointString() {
        return getEndpointString(isCloud());
    }
    public String getEndpointString(boolean includeDbName) {
        return "jdbc:ch:" + (isCloud() ? "" : "http://") +
                ClickHouseServerForTest.getClickHouseAddress(ClickHouseProtocol.HTTP, false) + "/" + (includeDbName ? ClickHouseServerForTest.getDatabase() : "");
    }

    public Connection getJdbcConnection() throws SQLException {
        return getJdbcConnection(null);
    }

    public Connection getJdbcConnection(Properties properties) throws SQLException {
        Properties info = new Properties();
        info.setProperty("user", "default");
        info.setProperty("password", ClickHouseServerForTest.getPassword());

        if (properties != null) {
            info.putAll(properties);
        }

        info.setProperty(ClientConfigProperties.DATABASE.getKey(), ClickHouseServerForTest.getDatabase());

        return new ConnectionImpl(getEndpointString(), info);
    }

    protected static String getDatabase() {
        return ClickHouseServerForTest.getDatabase();
    }

    protected boolean runQuery(String query) {
        return runQuery(query, new Properties());
    }

    protected boolean runQuery(String query, Properties connProperties) {
        try (Connection connection = getJdbcConnection(connProperties)) {
            try (Statement stmt = connection.createStatement()) {
                return stmt.execute(query);
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to run query: {}", query, e);
            return false;
        }
    }


    protected boolean earlierThan(int major, int minor) {
        String serverVersion = getServerVersion();
        if (serverVersion == null) {
            return false;
        }

        String[] parts = serverVersion.split("\\.");
        if (parts.length < 2) {
            return false;
        }

        try {
            int serverMajor = Integer.parseInt(parts[0]);
            int serverMinor = Integer.parseInt(parts[1]);
            return serverMajor < major || (serverMajor == major && serverMinor < minor);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    protected String getServerVersion() {
        try (ConnectionImpl connection = (ConnectionImpl) getJdbcConnection()) {
            GenericRecord result = connection.client.queryAll("SELECT version() as server_version").stream()
                    .findFirst().orElseThrow(() -> new SQLException("Failed to retrieve server version."));
            return result.getString("server_version");
        } catch (SQLException e) {
            return null;
        }
    }
}
