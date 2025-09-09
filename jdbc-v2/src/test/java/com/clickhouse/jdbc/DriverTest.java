package com.clickhouse.jdbc;

import com.clickhouse.client.api.ClientConfigProperties;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.testng.AssertJUnit.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;


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

    @Test(groups = {"integration"})
    public void testGetPropertyInfo() {
        try {
            Driver driver = new Driver();
            driver.getPropertyInfo(getEndpointString(), new Properties());
            Assert.assertEquals(driver.getPropertyInfo(getEndpointString(), new Properties()).length, 7);
            Properties sample = new Properties();
            sample.setProperty("testing", "true");
            Assert.assertEquals(driver.getPropertyInfo(getEndpointString(), sample).length, 7);
        } catch (SQLException e) {
            Assert.fail("Failed to get property info", e);
        }
    }

    @Test(groups = {"integration"})
    public void testGetDriverVersion() {
        Driver driver = new Driver();
        assertTrue(driver.getMajorVersion() > 0);
        assertTrue(driver.getMinorVersion() > -1);
    }

    @Test(groups = {"integration"}, dataProvider = "testParsingDriverVersionDP")
    public void testParsingDriverVersion(String version, int expectedMajor, int expectedMinor) {
        int[] versions = Driver.parseVersion(version);
        Assert.assertEquals(versions, new int[] { expectedMajor, expectedMinor });
    }

    @DataProvider(name = "testParsingDriverVersionDP")
    public static Object[][] testParsingDriverVersionDP() {
        return new Object[][]{
                {"", 0, 0},
                {null, 0, 0},
                {"0.9.1", 0x09, 0x01},
                {"1.0.0", 0x010000, 0},
                {"1.0.1", 0x010000, 1},
                {"1.1.1", 0x010001, 1},
                {"1.2.1", 0x010002, 1},
                {"5000.20.1", 0x13880014, 1},
                {"2.1.1", 0x020001, 1},
        };
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
