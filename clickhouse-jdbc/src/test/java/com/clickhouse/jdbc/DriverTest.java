package com.clickhouse.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.clickhouse.jdbc.internal.ClickHouseJdbcUrlParser;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class DriverTest extends JdbcIntegrationTest {
    private Driver driver;

    @BeforeTest
    public void setUp() {
        driver = new Driver();
    }

    @AfterTest
    public void tearDown() {
        driver = null;
    }

    @Test(groups = { "unit" })
    public void testDriver() {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            Assert.fail("Failed to register ClickHouse JDBC driver", e);
        }
    }

    @Test(groups = { "unit" })
    public void testConnect() {
        try {
            Assert.assertNotNull(driver.connect(getEndpointString(), new Properties()));
        } catch (SQLException e) {
            Assert.fail("Failed to connect to ClickHouse", e);
        }
    }

    @Test(groups = { "unit" })
    public void testAcceptsURL() {
        try {
            Assert.assertTrue(driver.acceptsURL(getEndpointString()));
            Assert.assertTrue(driver.acceptsURL("jdbc:ch://localhost:8123"));
            Assert.assertTrue(driver.acceptsURL("jdbc:clickhouse://localhost:8123"));
            Assert.assertTrue(driver.acceptsURL("jdbc:clickhouse://localhost:8123?user=default&password=clickhouse"));
            Assert.assertFalse(driver.acceptsURL("jdbc:something://localhost:8123"));
        } catch (SQLException e) {
            Assert.fail("Failed to accept URL", e);
        }
    }

    @Test(groups = { "unit" })
    public void testGetPropertyInfo() {
        try {
            Assert.assertEquals(driver.getPropertyInfo(getEndpointString(), new Properties()).length, 7);
            Properties sample = new Properties();
            sample.setProperty("testing", "true");
            Assert.assertEquals(driver.getPropertyInfo(getEndpointString(), sample).length, 7);
        } catch (SQLException e) {
            Assert.fail("Failed to get property info", e);
        }
    }

    @Test(groups = { "unit" })
    public void testGetMajorVersion() {
        Assert.assertEquals(driver.getMajorVersion(), 0);
    }

    @Test(groups = { "unit" })
    public void testGetMinorVersion() {
        Assert.assertEquals(driver.getMinorVersion(), 0);
    }

    @Test(groups = { "unit" })
    public void testJdbcCompliant() {
        Assert.assertFalse(driver.jdbcCompliant());
    }

    @Test(groups = { "unit" })
    public void testGetParentLogger() {
        try {
            driver.getParentLogger();
            Assert.fail("Should not reach here");
        } catch (SQLException e) {
            Assert.assertEquals(e.getMessage(), "Method not supported");
        }
    }
}
