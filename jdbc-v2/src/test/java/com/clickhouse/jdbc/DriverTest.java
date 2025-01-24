package com.clickhouse.jdbc;

import com.clickhouse.client.api.ClientConfigProperties;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


public class DriverTest extends JdbcIntegrationTest {
    @Test(groups = { "integration" })
    public void testDriver() {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            Assert.fail("Failed to register ClickHouse JDBC driver", e);
        }
    }

    @Test(groups = { "integration" })
    public void testConnect() {
        try {
            Driver driver = new Driver();
            Properties props = new Properties();
            props.put(ClientConfigProperties.USER.getKey(), ClientConfigProperties.USER.getDefaultValue());
            props.put(ClientConfigProperties.PASSWORD.getKey(), getPassword());
            Assert.assertNotNull(driver.connect(getEndpointString(), props));
        } catch (SQLException e) {
            Assert.fail("Failed to connect to ClickHouse", e);
        }
    }

    @Test(groups = { "integration" })
    public void testAcceptsURL() {
        try {
            Driver driver = new Driver();
            Assert.assertTrue(driver.acceptsURL(getEndpointString()));
            Assert.assertTrue(driver.acceptsURL("jdbc:ch://localhost:8123"));
            Assert.assertTrue(driver.acceptsURL("jdbc:clickhouse://localhost:8123"));
            Assert.assertTrue(driver.acceptsURL("jdbc:clickhouse://localhost:8123?user=default&password=clickhouse"));
            Assert.assertFalse(driver.acceptsURL("jdbc:something://localhost:8123"));
        } catch (SQLException e) {
            Assert.fail("Failed to accept URL", e);
        }
    }

    @Test (enabled = false) //Disabled for now because it's not implemented
    public void testGetPropertyInfo() {
        try {
            Driver driver = new Driver();
            Assert.assertEquals(driver.getPropertyInfo(getEndpointString(), new Properties()).length, 7);
            Properties sample = new Properties();
            sample.setProperty("testing", "true");
            Assert.assertEquals(driver.getPropertyInfo(getEndpointString(), sample).length, 7);
        } catch (SQLException e) {
            Assert.fail("Failed to get property info", e);
        }
    }

    @Test(groups = { "integration" })
    public void testGetMajorVersion() {
        Driver driver = new Driver();
        Assert.assertEquals(driver.getMajorVersion(), 0);
    }

    @Test(groups = { "integration" })
    public void testGetMinorVersion() {
        Driver driver = new Driver();
        Assert.assertEquals(driver.getMinorVersion(), 8);
    }

    @Test(groups = { "integration" })
    public void testJdbcCompliant() {
        Driver driver = new Driver();
        Assert.assertFalse(driver.jdbcCompliant());
    }

    @Test(groups = { "integration" })
    public void testGetParentLogger() {
        try {
            Driver driver = new Driver();
            driver.getParentLogger();
            Assert.fail("Should not reach here");
        } catch (SQLException e) {
            Assert.assertEquals(e.getMessage(), "Method not supported");
        }
    }
}
