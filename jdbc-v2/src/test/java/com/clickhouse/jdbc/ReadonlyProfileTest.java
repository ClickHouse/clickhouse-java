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

    @BeforeClass(groups = { "integration" })
    public void setup() throws SQLException {
        // Append fixed character classes so the random password satisfies server
        // password-complexity policies (uppercase, lowercase, digit, special).
        password = UUID.randomUUID().toString() + "Aa1!";
        com.clickhouse.client.ClickHouseServerForTest.beforeSuite();
        try (Connection conn = getJdbcConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SETTINGS PROFILE IF NOT EXISTS jdbc_test_profile_readonly_1 SETTINGS readonly=1");
            stmt.execute("CREATE SETTINGS PROFILE IF NOT EXISTS jdbc_test_profile_readonly_2 SETTINGS readonly=2");
            stmt.execute("DROP USER IF EXISTS jdbc_test_user_readonly_1");
            stmt.execute("DROP USER IF EXISTS jdbc_test_user_readonly_2");
            stmt.execute("CREATE USER jdbc_test_user_readonly_1 IDENTIFIED WITH plaintext_password BY '" + password + "' SETTINGS PROFILE jdbc_test_profile_readonly_1");
            stmt.execute("CREATE USER jdbc_test_user_readonly_2 IDENTIFIED WITH plaintext_password BY '" + password + "' SETTINGS PROFILE jdbc_test_profile_readonly_2");
            String db = getDatabase();
            stmt.execute("GRANT SELECT ON " + db + ".* TO jdbc_test_user_readonly_1");
            stmt.execute("GRANT SELECT ON " + db + ".* TO jdbc_test_user_readonly_2");
        }
    }

    @AfterClass(groups = { "integration" })
    public void teardown() throws SQLException {
        try (Connection conn = getJdbcConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP USER IF EXISTS jdbc_test_user_readonly_1");
            stmt.execute("DROP USER IF EXISTS jdbc_test_user_readonly_2");
            stmt.execute("DROP SETTINGS PROFILE IF EXISTS jdbc_test_profile_readonly_1");
            stmt.execute("DROP SETTINGS PROFILE IF EXISTS jdbc_test_profile_readonly_2");
        }
    }

    @Test(groups = { "integration" })
    public void testReadonly1CannotChangeSettings() throws Exception {
        if (isCloud()) {
            throw new SkipException("Should not be tested in cloud because creates users and profiles");
        }
        Properties properties = new Properties();
        properties.setProperty("user", "jdbc_test_user_readonly_1");
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
        properties.setProperty("user", "jdbc_test_user_readonly_2");
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
