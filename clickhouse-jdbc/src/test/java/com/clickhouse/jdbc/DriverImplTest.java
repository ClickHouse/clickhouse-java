package com.clickhouse.jdbc;

import java.sql.SQLException;

import com.clickhouse.client.ClickHouseProtocol;

import com.clickhouse.jdbc.internal.ClickHouseJdbcUrlParser;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DriverImplTest extends JdbcIntegrationTest {
    @Test(groups = "integration")
    public void testAcceptUrl() throws SQLException {
        String address = getServerAddress(ClickHouseProtocol.HTTP, true);
        DriverImpl driver = new DriverImpl();
        Assert.assertTrue(driver.acceptsURL("jdbc:clickhouse://" + address));
        Assert.assertTrue(driver.acceptsURL("jdbc:clickhouse:http://" + address));
        Assert.assertTrue(driver.acceptsURL("jdbc:ch://" + address));
        Assert.assertTrue(driver.acceptsURL("jdbc:ch:http://" + address));
    }

    @Test(groups = "integration")
    public void testConnect() throws SQLException {
        if (isCloud()) return; //TODO: testConnect - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        String address = getServerAddress(ClickHouseProtocol.HTTP, true);
        DriverImpl driver = new DriverImpl();
        ClickHouseConnection conn = driver.connect("jdbc:clickhouse://" + address, null);
        conn.close();
    }
}
