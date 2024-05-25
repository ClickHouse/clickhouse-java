package com.clickhouse.jdbc;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.http.config.HttpConnectionProvider;
import com.clickhouse.data.ClickHouseVersion;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

public class AccessManagementTest extends JdbcIntegrationTest {

    @Test(groups = "integration", dataProvider = "setRolesArgsForTestSetRole")
    public void testSetRoleDifferentConnections(String[] roles, String setRoleExpr, String[] activeRoles,
                                                String connectionProvider) throws SQLException {

        String httpEndpoint = "http://" + getServerAddress(ClickHouseProtocol.HTTP) + "/";
        String url = String.format("jdbc:ch:%s", httpEndpoint);
        Properties properties = new Properties();
        properties.setProperty(ClickHouseHttpOption.REMEMBER_LAST_SET_ROLES.getKey(), "true");
        properties.setProperty(ClickHouseHttpOption.CONNECTION_PROVIDER.getKey(), connectionProvider);
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        String serverVersion = getServerVersion(dataSource.getConnection());

        try (Connection connection = dataSource.getConnection("access_dba", "123")) {
            Statement st = connection.createStatement();

            st.execute("DROP ROLE IF EXISTS " + String.join(", ", roles));
            st.execute("DROP USER IF EXISTS some_user");
            st.execute("CREATE ROLE " + String.join(", ", roles));
            st.execute("CREATE USER some_user IDENTIFIED WITH no_password");
            st.execute("GRANT " + String.join(", ", roles) + " TO some_user");
            st.execute("SET DEFAULT ROLE NONE TO some_user");
        } catch (Exception e) {
            Assert.fail("Failed", e);
        }

        try (Connection connection = dataSource.getConnection("some_user", "")) {
            Statement st = connection.createStatement();
            st.execute(setRoleExpr);
            assertRolesEquals(connection, activeRoles);
            // Check roles are reset
            st.execute("SET ROLE NONE");
            assertRolesEquals(connection);
        } catch (SQLException e) {
            if (e.getErrorCode() == ClickHouseException.ERROR_UNKNOWN_SETTING) {
                if (ClickHouseVersion.of(serverVersion).check("(,24.3]")) {
                    return;
                }
            }
            Assert.fail("Failed", e);
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
        } catch (SQLException e) {
            if (e.getErrorCode() == ClickHouseException.ERROR_UNKNOWN_SETTING) {
               throw e;
            }
            Assert.fail("Failed", e);
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
}
