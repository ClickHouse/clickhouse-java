package com.clickhouse.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseDatabaseMetaDataTest extends JdbcIntegrationTest {
    @Test(groups = "integration")
    public void testGetTypeInfo() throws SQLException {
        Properties props = new Properties();
        props.setProperty("decompress", "0");
        try (ClickHouseConnection conn = newConnection(props); ResultSet rs = conn.getMetaData().getTypeInfo()) {
            while (rs.next()) {
                Assert.assertNotNull(rs.getString(1));
            }
        }
    }
}
