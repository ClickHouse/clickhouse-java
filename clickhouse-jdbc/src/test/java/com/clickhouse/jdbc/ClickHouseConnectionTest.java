package com.clickhouse.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;

import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseClientOption;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseConnectionTest extends JdbcIntegrationTest {
    @Override
    public ClickHouseConnection newConnection(Properties properties) throws SQLException {
        return newDataSource(properties).getConnection();
    }

    @Test(groups = "integration")
    public void testCreateArray() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties())) {
            Array array = conn.createArrayOf("Array(Int8)", null);
            Assert.assertEquals(array.getArray(), new byte[0]);
        }
    }

    @Test(groups = "integration")
    public void testNonExistDatabase() throws Exception {
        String database = UUID.randomUUID().toString();
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.DATABASE.getKey(), database);
        SQLException exp = null;
        try (ClickHouseConnection conn = newConnection(props)) {
            // do nothing
        }
        Assert.assertNull(exp, "Should not have SQLException even the database does not exist");

        try (ClickHouseConnection conn = newConnection(props)) {
            Assert.assertEquals(conn.getSchema(), database);
            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("select 1")) {
                Assert.fail("Should have SQLException");
            }
        } catch (SQLException e) {
            exp = e;
        }
        Assert.assertNotNull(exp, "Should have SQLException since the database does not exist");
        Assert.assertEquals(exp.getErrorCode(), 81);

        props.setProperty(JdbcConfig.PROP_CREATE_DATABASE, Boolean.TRUE.toString());
        try (ClickHouseConnection conn = newConnection(props)) {
            exp = null;
        }
        Assert.assertNull(exp, "Should not have SQLException because database will be created automatically");

        props.setProperty(JdbcConfig.PROP_CREATE_DATABASE, Boolean.FALSE.toString());
        try (ClickHouseConnection conn = newConnection(props)) {
            Assert.assertEquals(conn.getSchema(), database);
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            ClickHouseUtils.format("select * from system.databases where name='%s'", database))) {
                Assert.assertTrue(rs.next(), "Should have at least one record in system.databases table");
                Assert.assertEquals(rs.getString("name"), database);
                Assert.assertFalse(rs.next(), "Should have only one record in system.databases table");
                exp = new SQLException();
            }
        }
        Assert.assertNotNull(exp, "Should not have SQLException because the database has been created");
    }
}
