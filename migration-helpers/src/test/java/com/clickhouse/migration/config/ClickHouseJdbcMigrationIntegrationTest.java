package com.clickhouse.migration.config;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class ClickHouseJdbcMigrationIntegrationTest extends BaseIntegrationTest {

    @BeforeClass
    public static void setUpContainer() {
        ClickHouseServerForTest.beforeSuite();
        try {
            Class.forName("com.clickhouse.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("ClickHouse JDBC Driver not found on classpath", e);
        }
    }

    @AfterClass
    public static void tearDownContainer() {
        ClickHouseServerForTest.afterSuite();
    }

    @Test
    public void testMinimalJdbcConnectivityWithConvertedProperties() throws SQLException {
        String hostAndPort = ClickHouseServerForTest.getClickHouseAddress(ClickHouseProtocol.HTTP, false);
        String database = ClickHouseServerForTest.getDatabase();
        String url = "jdbc:clickhouse:http://" + hostAndPort + "/" + database;

        // Legacy v1 properties containing renamed properties, un-prefixed server settings, and deprecated properties
        Properties v1Props = new Properties();
        v1Props.setProperty("user", ClickHouseServerForTest.getUsername());
        v1Props.setProperty("password", ClickHouseServerForTest.getPassword());
        v1Props.setProperty("connect_timeout", "5000");           // Renamed to connection_timeout
        v1Props.setProperty("buffer_size", "65536");              // Renamed to client_network_buffer_size
        v1Props.setProperty("max_threads", "4");                  // Server setting -> clickhouse_setting_max_threads
        v1Props.setProperty("protocol", "http");                  // Deprecated -> ignored
        v1Props.setProperty("use_compilation", "true");           // Deprecated -> ignored
        v1Props.setProperty("custom_settings", "join_use_nulls=1");

        Properties v2Props = ConfigurationMigrationHelper.convertProperties(v1Props);

        // Verify minimal JDBC connectivity with converted properties
        try (Connection conn = DriverManager.getConnection(url, v2Props);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {

            Assert.assertTrue(rs.next(), "ResultSet should contain at least one row.");
            Assert.assertEquals(rs.getInt(1), 1, "First column should be 1.");
        }
    }

    @Test
    public void testMinimalJdbcConnectivityWithConvertedUrl() throws SQLException {
        String hostAndPort = ClickHouseServerForTest.getClickHouseAddress(ClickHouseProtocol.HTTP, false);
        String database = ClickHouseServerForTest.getDatabase();
        String password = ClickHouseServerForTest.getPassword();
        String username = ClickHouseServerForTest.getUsername();

        // Legacy v1 connection URL containing query parameters
        String v1Url = "jdbc:clickhouse:http://" + hostAndPort + "/" + database
                + "?user=" + username
                + "&password=" + password
                + "&connect_timeout=5000&max_threads=4&protocol=http&use_compilation=true";

        String v2Url = ConfigurationMigrationHelper.convertUrl(v1Url);

        Assert.assertFalse(v2Url.contains("protocol="), "Converted URL should not contain deprecated protocol param.");
        Assert.assertFalse(v2Url.contains("use_compilation="), "Converted URL should not contain deprecated use_compilation param.");
        Assert.assertTrue(v2Url.contains("connection_timeout=5000"), "Converted URL should contain connection_timeout=5000.");
        Assert.assertTrue(v2Url.contains("clickhouse_setting_max_threads=4"), "Converted URL should contain clickhouse_setting_max_threads=4.");

        // Verify minimal JDBC connectivity with converted URL
        try (Connection conn = DriverManager.getConnection(v2Url);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1, 'test_container'")) {

            Assert.assertTrue(rs.next(), "ResultSet should contain at least one row.");
            Assert.assertEquals(rs.getInt(1), 1, "First column should be 1.");
            Assert.assertEquals(rs.getString(2), "test_container", "Second column should match 'test_container'.");
        }
    }
}
