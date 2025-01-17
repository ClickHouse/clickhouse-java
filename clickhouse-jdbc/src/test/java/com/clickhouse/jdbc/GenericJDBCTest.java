package com.clickhouse.jdbc;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.*;
import java.util.Properties;

import static org.testng.Assert.assertThrows;

public class GenericJDBCTest extends JdbcIntegrationTest {
    public Connection getConnection(Properties properties) throws SQLException {
        if (properties == null) {
            properties = new Properties();
        }

        return newDataSource(properties).getConnection();
    }

    @Test
    public void connectionTest() throws SQLException {
        try (Connection connection = getConnection(null)) {
            Assert.assertNotNull(connection);
            Assert.assertTrue(connection.isValid(1));
        }
    }

    @Test
    public void connectionWithPropertiesTest() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "default");
        properties.setProperty("password", "FAKE123456FAKE");

        assertThrows(SQLException.class, () -> getConnection(properties));
    }

    @Test
    public void basicStatementTest() {
        try (Connection connection = getConnection(null);
             Statement statement = connection.createStatement()) {
            Assert.assertNotNull(statement);
            ResultSet resultSet = statement.executeQuery("SELECT 1");
            Assert.assertNotNull(resultSet);
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getInt(1), 1);
        } catch (SQLException e) {
            Assert.fail("Failed to create statement", e);
        }
    }

    @Test
    public void basicPreparedStatementTest() {
        try (Connection connection = getConnection(null);
             PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
            Assert.assertNotNull(statement);
            statement.setInt(1, 1);
            ResultSet resultSet = statement.executeQuery();
            Assert.assertNotNull(resultSet);
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getInt(1), 1);
        } catch (SQLException e) {
            Assert.fail("Failed to create statement", e);
        }
    }
}
