package com.clickhouse.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
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

    @Test(groups = "integration")
    public void testTableComment() throws SQLException {
        String tableName = "test_table_comment";
        String tableComment = "table comments";
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement()) {
            // https://github.com/ClickHouse/ClickHouse/pull/30852
            if (!conn.getServerVersion().check("[21.6,)")) {
                return;
            }

            s.execute(String.format(Locale.ROOT,
                    "drop table if exists %1$s; create table %1$s(s String) engine=Memory comment '%2$s'",
                    tableName, tableComment));
            try (ResultSet rs = conn.getMetaData().getTables(null, "%", tableName, null)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString("remarks"), tableComment);
                Assert.assertFalse(rs.next());
            }
        }
    }
}
