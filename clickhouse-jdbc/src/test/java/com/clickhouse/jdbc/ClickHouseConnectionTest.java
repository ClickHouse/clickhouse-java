package com.clickhouse.jdbc;

import java.sql.Array;
import java.sql.SQLException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseConnectionTest extends JdbcIntegrationTest {
    @Test(groups = "integration")
    public void testCreateArray() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties())) {
            Array array = conn.createArrayOf("Array(Int8)", null);
            Assert.assertEquals(array.getArray(), new byte[0]);
        }
    }
}
