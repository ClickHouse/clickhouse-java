package com.clickhouse.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;

public class ClickHouseDataSourceTest extends JdbcIntegrationTest {
    @Test(groups = "integration")
    public void testGetConnection() throws SQLException {
        String url = "jdbc:ch://" + getServerAddress(DEFAULT_PROTOCOL);
        String urlWithCredentials = "jdbc:ch://default@" + getServerAddress(ClickHouseProtocol.HTTP);
        String clientName = "client1";
        int maxExecuteTime = 1234;
        boolean continueBatchOnError = true;

        Properties properties = new Properties();
        properties.setProperty(ClickHouseDefaults.USER.getKey(), "default");
        properties.setProperty(ClickHouseDefaults.PASSWORD.getKey(), "");
        properties.setProperty(ClickHouseClientOption.CLIENT_NAME.getKey(), clientName);
        properties.setProperty(ClickHouseClientOption.MAX_EXECUTION_TIME.getKey(), Integer.toString(maxExecuteTime));
        properties.setProperty(JdbcConfig.PROP_CONTINUE_BATCH, Boolean.toString(continueBatchOnError));
        String params = String.format("?%s=%s&%s=%d&%s", ClickHouseClientOption.CLIENT_NAME.getKey(), clientName,
                ClickHouseClientOption.MAX_EXECUTION_TIME.getKey(), maxExecuteTime, JdbcConfig.PROP_CONTINUE_BATCH);

        for (ClickHouseDataSource ds : new ClickHouseDataSource[] {
                new ClickHouseDataSource(url, properties),
                new ClickHouseDataSource(urlWithCredentials, properties),
                new ClickHouseDataSource(url + params),
                new ClickHouseDataSource(urlWithCredentials + params),
        }) {
            for (ClickHouseConnection connection : new ClickHouseConnection[] {
                    ds.getConnection(),
                    ds.getConnection("default", ""),
                    new ClickHouseDriver().connect(url, properties),
                    new ClickHouseDriver().connect(urlWithCredentials, properties),
                    new ClickHouseDriver().connect(url + params, new Properties()),
                    new ClickHouseDriver().connect(urlWithCredentials + params, new Properties()),
                    (ClickHouseConnection) DriverManager.getConnection(url, properties),
                    (ClickHouseConnection) DriverManager.getConnection(urlWithCredentials, properties),
                    (ClickHouseConnection) DriverManager.getConnection(url + params),
                    (ClickHouseConnection) DriverManager.getConnection(urlWithCredentials + params),
                    (ClickHouseConnection) DriverManager.getConnection(url + params, "default", ""),
                    (ClickHouseConnection) DriverManager.getConnection(urlWithCredentials + params, "default", ""),
            }) {
                try (ClickHouseConnection conn = connection; Statement stmt = conn.createStatement()) {
                    Assert.assertEquals(conn.getClientInfo(ClickHouseConnection.PROP_APPLICATION_NAME), clientName);
                    Assert.assertEquals(stmt.getQueryTimeout(), maxExecuteTime);
                    Assert.assertEquals(conn.getJdbcConfig().isContinueBatchOnError(), continueBatchOnError);
                }
            }
        }
    }
}
