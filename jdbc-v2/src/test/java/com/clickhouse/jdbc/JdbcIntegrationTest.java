package com.clickhouse.jdbc;

import com.clickhouse.client.ClickHouseServerForTest;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public abstract class JdbcIntegrationTest extends BaseIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcIntegrationTest.class);

    public String getEndpointString() {
        return getEndpointString(isCloud());
    }
    public String getEndpointString(boolean includeDbName) {
        return "jdbc:ch:" + (isCloud() ? "https" : "http") + "://" +
                ClickHouseServerForTest.getClickHouseAddress(ClickHouseProtocol.HTTP, false) + "/" + (includeDbName ? ClickHouseServerForTest.getDatabase() : "");
    }

    public Connection getJdbcConnection() throws SQLException {
        Properties info = new Properties();
        info.setProperty("user", "default");
        info.setProperty("password", ClickHouseServerForTest.getPassword());
        LOGGER.info("Connecting to {}", getEndpointString());

        return new ConnectionImpl(getEndpointString(), info);
        //return DriverManager.getConnection(getEndpointString(), "default", ClickHouseServerForTest.getPassword());
    }

    protected static String getDatabase() {
        return isCloud() ? ClickHouseServerForTest.getDatabase() : "default";
    }
}
