package com.clickhouse.jdbc;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.internal.ServerSettings;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;

public class ReadonlyProfileTest extends JdbcIntegrationTest {

    private String password;
    private String user1;
    private String user2;
    private String profile1;
    private String profile2;

    @BeforeClass(groups = { "integration" })
    public void setup() throws SQLException {
        if (isCloud()) {
            return;
        }
        // Append fixed character classes so the random password satisfies server
        // password-complexity policies (uppercase, lowercase, digit, special).
        password = UUID.randomUUID().toString() + "Aa1!";
        String suffix = "_" + UUID.randomUUID().toString().replace("-", "");
        user1 = "jdbc_test_user_readonly_1" + suffix;
        user2 = "jdbc_test_user_readonly_2" + suffix;
        profile1 = "jdbc_test_profile_readonly_1" + suffix;
        profile2 = "jdbc_test_profile_readonly_2" + suffix;
        com.clickhouse.client.ClickHouseServerForTest.beforeSuite();
        try (Connection conn = getJdbcConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SETTINGS PROFILE " + profile1 + " SETTINGS readonly=1");
            stmt.execute("CREATE SETTINGS PROFILE " + profile2 + " SETTINGS readonly=2");
            waitForAllReplicas("settings_profiles", "name IN ('" + profile1 + "', '" + profile2 + "')", 2);
            stmt.execute("CREATE USER " + user1 + " IDENTIFIED WITH plaintext_password BY '" + password + "' SETTINGS PROFILE " + profile1);
            stmt.execute("CREATE USER " + user2 + " IDENTIFIED WITH plaintext_password BY '" + password + "' SETTINGS PROFILE " + profile2);
            String users = "'" + user1 + "', '" + user2 + "'";
            waitForAllReplicas("users", "name IN (" + users + ")", 2);
            String db = getDatabase();
            stmt.execute("GRANT SELECT ON " + db + ".* TO " + user1);
            stmt.execute("GRANT SELECT ON " + db + ".* TO " + user2);
            waitForAllReplicas("grants", "user_name IN (" + users + ")", 2);
        }
    }

    @AfterClass(groups = { "integration" })
    public void teardown() throws SQLException {
        if (isCloud()) {
            return;
        }
        try (Connection conn = getJdbcConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP USER IF EXISTS " + user1);
            stmt.execute("DROP USER IF EXISTS " + user2);
            stmt.execute("DROP SETTINGS PROFILE IF EXISTS " + profile1);
            stmt.execute("DROP SETTINGS PROFILE IF EXISTS " + profile2);
        }
    }

    @Test(groups = { "integration" })
    public void testReadonly1CannotChangeSettings() throws Exception {
        if (isCloud()) {
            throw new SkipException("Should not be tested in cloud because creates users and profiles");
        }
        Properties properties = new Properties();
        properties.setProperty("user", user1);
        properties.setProperty("password", password);
        properties.put(ClientConfigProperties.serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING), "1");
        
        try (Connection conn = getJdbcConnection(properties)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1")) {
                Assert.fail("Should have thrown an exception because readonly=1 prevents changing settings");
            }
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Cannot modify"), "Exception message should indicate setting cannot be modified: " + e.getMessage());
        }
    }

    @Test(groups = { "integration" })
    public void testReadonly2CanChangeSettings() throws Exception {
        if (isCloud()) {
            throw new SkipException("Should not be tested in cloud because creates users and profiles");
        }
        if (isVersionMatch("(,24.8]")) {
            throw new SkipException("Old versions do not support JSON");
        }

        Properties properties = new Properties();
        properties.setProperty("user", user2);
        properties.setProperty("password", password);
        properties.put(ClientConfigProperties.serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING), "1");
        properties.put(ClientConfigProperties.serverSetting("allow_experimental_json_type"), "1");
        
        try (Connection conn = getJdbcConnection(properties)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT '{\"key\":\"value\"}'::JSON")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "{\"key\":\"value\"}");
            }
        }
    }
}
