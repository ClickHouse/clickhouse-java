package com.clickhouse.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import com.clickhouse.client.ClickHouseProtocol;

import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.jdbc.internal.ClickHouseJdbcUrlParser;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseDriverTest extends JdbcIntegrationTest {
    @Test(groups = "integration")
    public void testAcceptUrl() throws SQLException {
        String address = getServerAddress(ClickHouseProtocol.HTTP, true);
        ClickHouseDriver driver = new ClickHouseDriver();
        Assert.assertTrue(driver.acceptsURL("jdbc:clickhouse://" + address));
        Assert.assertTrue(driver.acceptsURL("jdbc:clickhouse:http://" + address));
        Assert.assertTrue(driver.acceptsURL("jdbc:ch://" + address));
        Assert.assertTrue(driver.acceptsURL("jdbc:ch:http://" + address));
    }

    @Test(groups = "integration")
    public void testConnect() throws SQLException {
        if (isCloud()) return; //TODO: testConnect - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        System.setProperty("clickhouse.jdbc.v1","true");
        String address = getServerAddress(ClickHouseProtocol.HTTP, true);
        ClickHouseDriver driver = new ClickHouseDriver();
        Connection conn = driver.connect("jdbc:clickhouse://default:" + ClickHouseServerForTest.getPassword() + "@" + address, null);
        conn.close();
    }
    @Test(groups = "integration")
    public void testV2Driver() {
        System.setProperty("clickhouse.jdbc.v1","false");
        ClickHouseDriver driver = new ClickHouseDriver();
        Boolean V1 = false;
        Boolean V2 = true;
        Assert.assertEquals(driver.isV2("jdbc:clickhouse://localhost:8123"), V2);
        Assert.assertEquals(driver.isV2("jdbc:clickhouse://localhost:8123?clickhouse.jdbc.v1=true"), V1);
        Assert.assertEquals(driver.isV2("jdbc:clickhouse://localhost:8123?clickhouse.jdbc.v1=false"), V2);
        Assert.assertEquals(driver.isV2("jdbc:clickhouse://localhost:8123?clickhouse.jdbc.v2=true"), V2);
        Assert.assertEquals(driver.isV2("jdbc:clickhouse://localhost:8123?clickhouse.jdbc.v2=false"), V1);
        System.setProperty("clickhouse.jdbc.v1","true");
    }
}
