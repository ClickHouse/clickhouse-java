package com.clickhouse.jdbc;

import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.http.config.HttpConnectionProvider;
import com.clickhouse.data.ClickHouseVersion;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

public class AccessManagementTest extends JdbcIntegrationTest {
    @BeforeMethod(groups = "integration")
    public void setV1() {
        System.setProperty("clickhouse.jdbc.v1","true");
    }
    @Test(groups = "integration", dataProvider = "setRolesArgsForTestSetRole")
    public void testSetRoleDifferentConnections(String[] roles, String setRoleExpr, String[] activeRoles,
                                                String connectionProvider) throws SQLException {
        if (isCloud()) return; //TODO: testSetRoleDifferentConnections - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747

        String url = String.format("jdbc:ch:%s", getEndpointString());
        Properties properties = new Properties();
        properties.setProperty(ClickHouseDefaults.PASSWORD.getKey(), ClickHouseServerForTest.getPassword());
        properties.setProperty(ClickHouseHttpOption.REMEMBER_LAST_SET_ROLES.getKey(), "true");
        properties.setProperty(ClickHouseHttpOption.CONNECTION_PROVIDER.getKey(), connectionProvider);
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        String serverVersion = getServerVersion(dataSource.getConnection());
        if (ClickHouseVersion.of(serverVersion).check("(,24.3]")) {
            System.out.println("Test is skipped: feature is supported since 24.4");
            return;
        }

        try (Connection connection = dataSource.getConnection("access_dba", "123")) {
            Statement st = connection.createStatement();

            st.execute("DROP ROLE IF EXISTS " + String.join(", ", roles));
            st.execute("DROP USER IF EXISTS some_user");
            st.execute("CREATE ROLE " + String.join(", ", roles));
            st.execute("CREATE USER some_user IDENTIFIED WITH no_password");
            st.execute("GRANT " + String.join(", ", roles) + " TO some_user");
            st.execute("SET DEFAULT ROLE NONE TO some_user");
        } catch (Exception e) {
            Assert.fail("Failed to prepare for the test", e);
        }

        try (Connection connection = dataSource.getConnection("some_user", "")) {
            Statement st = connection.createStatement();
            st.execute(setRoleExpr);
            assertRolesEquals(connection, activeRoles);
            // Check roles are reset
            st.execute("SET ROLE NONE");
            assertRolesEquals(connection);
        } catch (Exception e) {
            Assert.fail("Failed", e);
        }
    }

    @DataProvider(name = "setRolesArgsForTestSetRole")
    private static Object[][] setRolesArgsForTestSetRole() {
        return new Object[][]{
                {new String[]{"ROL1", "ROL2"}, "set role ROL2", new String[]{"ROL2"},
                        HttpConnectionProvider.HTTP_URL_CONNECTION.name()},
                {new String[]{"ROL1", "ROL2"}, "set role ROL2", new String[]{"ROL2"},
                        HttpConnectionProvider.APACHE_HTTP_CLIENT.name()},
                {new String[]{"ROL1", "ROL2"}, "set role ROL2, ROL1", new String[]{"ROL1", "ROL2"},
                        HttpConnectionProvider.APACHE_HTTP_CLIENT.name()},
                {new String[]{"ROL1", "\"ROL2,☺\""}, "set role  \"ROL2,☺\", ROL1", new String[]{"ROL2,☺", "ROL1"},
                        HttpConnectionProvider.APACHE_HTTP_CLIENT.name()},
                {new String[]{"ROL1", "ROL2"}, "set role  ROL2 ,   ROL1  ", new String[]{"ROL2", "ROL1"},
                        HttpConnectionProvider.APACHE_HTTP_CLIENT.name()},
        };
    }

    private void assertRolesEquals(Connection connection, String... expected) throws SQLException {
        try {
            Statement st = connection.createStatement();
            ResultSet resultSet = st.executeQuery("select currentRoles()");

            Assert.assertTrue(resultSet.next());
            String[] roles = (String[]) resultSet.getArray(1).getArray();
            Arrays.sort(roles);
            Arrays.sort(expected);
            Assert.assertEquals(roles, expected,
                    "Memorized roles: " + Arrays.toString(roles) + " != Expected: " + Arrays.toString(expected));
            System.out.println("Roles: " + Arrays.toString(roles));
        } catch (Exception e) {
            Assert.fail("Failed", e);
        }
    }

    private String getServerVersion(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT version()");
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (SQLException e) {
            Assert.fail("Failed to get server version", e);
        }
        return null;
    }

    @Test
    public void testSetRolesAccessingTableRows() throws SQLException {
        if (isCloud()) return; //TODO: testSetRolesAccessingTableRows - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        String url = String.format("jdbc:ch:%s", getEndpointString());
        Properties properties = new Properties();
        properties.setProperty(ClickHouseDefaults.PASSWORD.getKey(), ClickHouseServerForTest.getPassword());
        properties.setProperty(ClickHouseHttpOption.REMEMBER_LAST_SET_ROLES.getKey(), "true");
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        String serverVersion = getServerVersion(dataSource.getConnection());
        if (ClickHouseVersion.of(serverVersion).check("(,24.3]")) {
            System.out.println("Test is skipped: feature is supported since 24.4");
            return;
        }

        try (Connection connection = dataSource.getConnection("access_dba", "123")) {
            Statement st = connection.createStatement();
            st.execute("DROP ROLE IF EXISTS row_a");
            st.execute("DROP USER IF EXISTS some_user");

            st.execute("CREATE ROLE row_a, row_b");
            st.execute("CREATE USER some_user IDENTIFIED WITH no_password");
            st.execute("GRANT row_a, row_b TO some_user");

            st.execute("CREATE OR REPLACE TABLE test_table (`s` String ) ENGINE = MergeTree ORDER BY tuple();");
            st.execute("INSERT INTO test_table VALUES ('a'), ('b')");

            st.execute("GRANT SELECT ON test_table TO some_user");
            st.execute("CREATE ROW POLICY OR REPLACE policy_row_b ON test_table FOR SELECT USING s = 'b' TO row_b;");
            st.execute("CREATE ROW POLICY OR REPLACE policy_row_a ON test_table FOR SELECT USING s = 'a' TO row_a;");
        } catch (Exception e) {
            Assert.fail("Failed on setup", e);
        }

        try (Connection connection = dataSource.getConnection("some_user", "")) {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM test_table");
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.next());

            st.execute("SET ROLE row_a");
            rs = st.executeQuery("SELECT * FROM test_table");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "a");
            Assert.assertFalse(rs.next());

            st.execute("SET ROLE row_b");
            rs = st.executeQuery("SELECT * FROM test_table");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "b");
            Assert.assertFalse(rs.next());


            st.execute("SET ROLE row_a, row_b");
            rs = st.executeQuery("SELECT * FROM test_table ORDER BY s");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "a");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "b");
            Assert.assertFalse(rs.next());

            st.execute("SET ROLE row_b");
            rs = st.executeQuery("SELECT * FROM test_table");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "b");
            Assert.assertFalse(rs.next());

            st.execute("SET ROLE NONE");
            rs = st.executeQuery("SELECT * FROM test_table");
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.next());
        } catch (Exception e) {
            Assert.fail("Failed to check roles", e);
        }
    }

    @Test(groups = "integration", dataProvider = "passwordAuthMethods")
    public void testPasswordAuthentication(String identifyWith, String identifyBy) throws SQLException {
        if (isCloud()) return; // Doesn’t allow to create users with specific passwords
        String url = String.format("jdbc:ch:%s", getEndpointString());
        Properties properties = new Properties();
        properties.setProperty(ClickHouseHttpOption.REMEMBER_LAST_SET_ROLES.getKey(), "true");
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);

        try (Connection connection = dataSource.getConnection("access_dba", "123")) {
            Statement st = connection.createStatement();
            st.execute("DROP USER IF EXISTS some_user");
            st.execute("CREATE USER some_user IDENTIFIED WITH " + identifyWith + " BY '" + identifyBy + "'");
        } catch (Exception e) {
            Assert.fail("Failed on setup", e);
        }

        try (Connection connection = dataSource.getConnection("some_user", identifyBy)) {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT user() AS user_name");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "some_user");
        } catch (Exception e) {
            Assert.fail("Failed to authenticate", e);
        }
    }

    @DataProvider(name = "passwordAuthMethods")
    private static Object[][] passwordAuthMethods() {
        return new Object[][] {
                { "plaintext_password", "password" },
                { "plaintext_password", "" },
                { "plaintext_password", "S3Cr=?t"},
                { "plaintext_password", "123§" },
                { "sha256_password", "password" },
                { "sha256_password", "123§" },
                { "sha256_password", "S3Cr=?t"},
                { "sha256_password", "S3Cr?=t"},
        };
    }

    @Test(groups = "integration", dataProvider = "headerAuthDataProvider")
    public void testSwitchingBasicAuthToClickHouseHeaders(String identifyWith, String identifyBy, boolean shouldFail) throws SQLException {
        if (isCloud()) return; // Doesn't allow to create users with specific passwords
        String url = String.format("jdbc:ch:%s", getEndpointString());
        Properties properties = new Properties();
        properties.put(ClickHouseHttpOption.USE_BASIC_AUTHENTICATION.getKey(), false);
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);

        try (Connection connection = dataSource.getConnection("access_dba", "123")) {
            Statement st = connection.createStatement();
            st.execute("DROP USER IF EXISTS some_user");
            st.execute("CREATE USER some_user IDENTIFIED WITH " + identifyWith + " BY '" + identifyBy + "'");
        } catch (Exception e) {
            Assert.fail("Failed on setup", e);
        }

        try (Connection connection = dataSource.getConnection("some_user", identifyBy)) {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT user() AS user_name");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "some_user");
            if (shouldFail) {
                Assert.fail("Expected authentication to fail");
            }
        } catch (Exception e) {
            if (!shouldFail) {
                Assert.fail("Failed to authenticate", e);
            }
        }
    }

    @DataProvider(name = "headerAuthDataProvider")
    private static Object[][] headerAuthDataProvider() {
        return new Object[][] {
                { "plaintext_password", "password", false },
                { "plaintext_password", "", false },
                { "plaintext_password", "S3Cr=?t", true},
                { "plaintext_password", "123§", true },
                { "sha256_password", "password", false},
                { "sha256_password", "123§", true },
                { "sha256_password", "S3Cr=?t", true},
                { "sha256_password", "S3Cr?=t", false},
        };
    }
}
