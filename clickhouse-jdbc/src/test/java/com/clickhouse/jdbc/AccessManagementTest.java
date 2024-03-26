package com.clickhouse.jdbc;

import com.clickhouse.client.ClickHouseProtocol;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class AccessManagementTest extends JdbcIntegrationTest {

    @Test(groups = "integration")
    public void testSetRoleDifferentConnections() throws SQLException {
        /*
            Tests:
            * Simple expressions
            * Composite expressions with multiple roles
            * Composite expressions with mixed statements:
                - update table1 SET a = 1; set role ROL1; update table2 SET b = 2; set role NONE;
         */

        String httpEndpoint = "http://" + getServerAddress(ClickHouseProtocol.HTTP) + "/";
        String url = String.format("jdbc:ch:%s", httpEndpoint);
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url);



        try (Connection connection = dataSource.getConnection("default", "")) {
            Statement st = connection.createStatement();

//            st.execute("DROP ROLE IF EXISTS ROL1, \"role☺,\"");
            st.execute("DROP ROLE IF EXISTS ROL1, \"ROL2,☺\"");
            st.execute("DROP USER IF EXISTS some_user");
            st.execute("create role ROL1; create role \"ROL2,☺\";");
            st.execute("create user some_user IDENTIFIED WITH no_password");
            st.execute("grant ROL1 to some_user");
            st.execute("grant \"ROL2,☺\" to some_user");

        } catch (Exception e) {
            Assert.fail("Failed", e);
        }

        try (Connection connection = dataSource.getConnection("some_user", "")) {
            assertRolesEquals(connection, "ROL1", "ROL2,☺");

            Statement st = connection.createStatement();
            st.execute("set role \"ROL2,☺\"");
//            st.execute("set\n role\n ROL1, \"ROL2,\"");
            assertRolesEquals(connection, "ROL2,☺");

        } catch (Exception e) {
            Assert.fail("Failed", e);
        }
    }

    private void assertRolesEquals(Connection connection, String ...expected) {
        try {
            Statement st = connection.createStatement();
            ResultSet resultSet = st.executeQuery("select currentRoles()");

            Assert.assertTrue(resultSet.next());
            String[] roles = (String[]) resultSet.getArray(1).getArray();
            Arrays.sort(roles);
            Arrays.sort(expected);
            System.out.println("currentRoles = " + Arrays.asList(roles));
            Assert.assertEquals(roles, expected);

        } catch (Exception e) {
            Assert.fail("Failed", e);
        }
    }
}
