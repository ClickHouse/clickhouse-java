package com.clickhouse.jdbc;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientMisconfigurationException;
import com.clickhouse.client.api.ServerException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


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

    @Test(groups = {"integration"}, dataProvider = "testGetPropertyInfoDP")
    public void testGetPropertyInfo(String url, Properties props, Map<String, String> checkProperties) {
        final Map<String, String> checkPropertiesCopy = new HashMap<>(checkProperties);
        try {
            Driver driver = new Driver();
            DriverPropertyInfo[] properties = driver.getPropertyInfo(url, props);

            for (DriverPropertyInfo property : properties) {
                Object expectedValue = checkPropertiesCopy.remove(property.name);
                if (expectedValue != null) {
                    assertEquals(property.value, expectedValue);
                } else {
                    for (DriverProperties driverProp : DriverProperties.values()) {
                        if (driverProp.getKey().equalsIgnoreCase(property.name)) {
                            assertEquals(property.value, driverProp.getDefaultValue());
                        }
                    }
                    for (ClientConfigProperties clientProp : ClientConfigProperties.values()) {
                        if (clientProp.getKey().equalsIgnoreCase(property.name)) {
                            assertEquals(property.value, clientProp.getDefaultValue());
                        }
                    }
                }
            }

            Assert.assertTrue(checkPropertiesCopy.isEmpty(), "Not checked properties: " + checkProperties);
        } catch (SQLException e) {
            Assert.fail("Failed to get property info", e);
        }
    }

    @DataProvider(name = "testGetPropertyInfoDP")
    public Object[][] testGetPropertyInfoDP() {
        return new Object[][]{
                {"jdbc:ch://localhost:8123/?async=true", null, Map.of(ClientConfigProperties.ASYNC_OPERATIONS.getKey(), "true")},
                {"jdbc:ch://localhost:8123/?connection_ttl=10000&max_threads_per_client=100", null, Map.of(ClientConfigProperties.CONNECTION_TTL.getKey(), "10000",
                        ClientConfigProperties.MAX_THREADS_PER_CLIENT.getKey(), "100")},
                {"jdbc:ch://localhost:8123/?client_retry_on_failures=NoHttpResponse,SocketTimeout", null, Map.of(ClientConfigProperties.CLIENT_RETRY_ON_FAILURE.getKey(), "NoHttpResponse,SocketTimeout")},
                {"jdbc:ch://localhost:8123/?connection_ttl=10000&client_retry_on_failures=NoHttpResponse,SocketTimeout&max_threads_per_client=100",
                        null, Map.of(ClientConfigProperties.CLIENT_RETRY_ON_FAILURE.getKey(), "NoHttpResponse,SocketTimeout",
                        ClientConfigProperties.CONNECTION_TTL.getKey(), "10000",
                        ClientConfigProperties.MAX_THREADS_PER_CLIENT.getKey(), "100")}
        };
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
        assertEquals(versions, new int[] { expectedMajor, expectedMinor });
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
                {"2.1.1.0", 0x020001, 1},
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
            assertEquals(e.getMessage(), "Method not supported");
        }
    }

    @Test(groups = { "integration" })
    public void testUnknownSettings() throws Exception {
        Driver driver = new Driver();
        try {
            driver.connect(getEndpointString() + "?unknown_setting=1", new Properties());
            Assert.fail("Exception expected");
        } catch (SQLException e) {
            Assert.assertTrue(e.getCause() instanceof ClientMisconfigurationException);
            Assert.assertTrue(e.getCause().getMessage().contains("unknown_setting"));
        }

        try {
            Properties properties = new Properties();
            properties.put("unknown_setting1", "1");
            driver.connect(getEndpointString(), properties).close();
            Assert.fail("Exception expected");
        } catch (SQLException e) {
            Assert.assertTrue(e.getCause() instanceof ClientMisconfigurationException);
            Assert.assertTrue(e.getCause().getMessage().contains("unknown_setting1"));
        }

        {
            Properties properties = new Properties();
            properties.put(DriverProperties.serverSetting("unknown_setting2"), "1");
            try (Connection connection = getJdbcConnection(properties)) {
                try {
                    connection.createStatement().execute("SELECT 1");
                    Assert.fail("Exception expected");
                } catch (SQLException e) {
                    Assert.assertTrue(e.getCause() instanceof ServerException);
                    assertEquals(((ServerException) e.getCause()).getCode(), ServerException.UNKNOWN_SETTING);
                }
            }
        }

        {
            Properties properties = new Properties();
            properties.put(DriverProperties.SECURE_CONNECTION.getKey(), "true");
            properties.put(DriverProperties.SCHEMA_TERM.getKey(), "catalog");
            try (Connection connection = getJdbcConnection(properties);
                 Statement stmt = connection.createStatement()) {
                // Verifies that creating a connection and statement with SECURE_CONNECTION and SCHEMA_TERM properties does not throw any exceptions.
            }
        }

        // next should not throw exception
        driver.connect(getEndpointString() + "?unknown_setting=1&" + ClientConfigProperties.NO_THROW_ON_UNKNOWN_CONFIG + "=1", new Properties()).close();
    }
}
