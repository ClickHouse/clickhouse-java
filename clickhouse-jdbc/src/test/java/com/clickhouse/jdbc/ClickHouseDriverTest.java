package com.clickhouse.jdbc;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;

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
        if (isCloud())  {
            return; //
        }

        System.setProperty("clickhouse.jdbc.v1","true");
        String address = getServerAddress(ClickHouseProtocol.HTTP, true);
        ClickHouseDriver driver = new ClickHouseDriver();
        Connection conn = driver.connect("jdbc:clickhouse://default:" + ClickHouseServerForTest.getPassword() + "@" + address, null);
        conn.close();
        System.setProperty("clickhouse.jdbc.v1","false");
        ClickHouseDriver driver2 = new ClickHouseDriver();
        Connection conn2 = driver2.connect("jdbc:clickhouse://default:" + ClickHouseServerForTest.getPassword() + "@" + address, null);
        conn2.close();
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
