package com.clickhouse.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DriverTest extends JdbcIntegrationTest {
    @Test
    public void testDriver() {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            Assert.fail("Failed to register ClickHouse JDBC driver", e);
        }
    }

    @Test
    public void testConnect() {
        try {
            Driver driver = new Driver();
            Assert.assertNotNull(driver.connect(getEndpointString(), new Properties()));
        } catch (SQLException e) {
            Assert.fail("Failed to connect to ClickHouse", e);
        }
    }

    @Test
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

    @Test
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

    @Test
    public void testGetMajorVersion() {
        Driver driver = new Driver();
        Assert.assertEquals(driver.getMajorVersion(), 1);
    }

    @Test
    public void testGetMinorVersion() {
        Driver driver = new Driver();
        Assert.assertEquals(driver.getMinorVersion(), 0);
    }

    @Test
    public void testJdbcCompliant() {
        Driver driver = new Driver();
        Assert.assertFalse(driver.jdbcCompliant());
    }

    @Test
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
