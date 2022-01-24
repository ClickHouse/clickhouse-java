package com.clickhouse.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

import com.clickhouse.client.ClickHouseColumn;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHouseDatabaseMetaDataTest extends JdbcIntegrationTest {
    @DataProvider(name = "selectedColumns")
    private Object[][] getSelectedColumns() {
        return new Object[][] {
                // COLUMN_SIZE, DECIMAL_DIGITS, CHAR_OCTET_LENGTH
                // new Object[] { "Bool", 1, null, null }, // Bool was an alias before 21.12
                new Object[] { "Int8", 3, 0, null },
                new Object[] { "UInt8", 3, 0, null },
                new Object[] { "FixedString(3)", 3, null, 3 },
                new Object[] { "String", 0, null, null },
                new Object[] { "Date", 10, 0, null },
                new Object[] { "DateTime64(5)", 29, 5, null },
                new Object[] { "Decimal64(10)", 18, 10, null },
                new Object[] { "Decimal(10,2)", 10, 2, null },
                new Object[] { "Decimal(12,0)", 12, 0, null },
                new Object[] { "Float32", 12, 0, null },
                new Object[] { "Float64", 22, 0, null } };
    }

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

    @Test(dataProvider = "selectedColumns", groups = "integration")
    public void testGetColumns(String columnType, Integer columnSize, Integer decimalDigits, Integer octectLength)
            throws SQLException {
        ClickHouseColumn c = ClickHouseColumn.of("x", columnType);
        String tableName = "test_get_column_" + c.getDataType().name().toLowerCase();
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement()) {
            s.execute("drop table if exists " + tableName + "; "
                    + "create table " + tableName + "(x " + columnType + ") engine=Memory");
            try (ResultSet rs = conn.getMetaData().getColumns(conn.getCatalog(), conn.getSchema(), tableName, "%")) {
                Assert.assertTrue(rs.next(), "Should have one record");
                Assert.assertEquals(rs.getString("cOLUMN_NAME"), "x");
                Assert.assertEquals(rs.getObject("COLUMN_SIZE"), columnSize);
                Assert.assertEquals(rs.getObject("DECIMAL_DIGITS"), decimalDigits);
                Assert.assertEquals(rs.getObject("CHAR_OCTET_LENGTH"), octectLength);
                Assert.assertFalse(rs.next(), "Should have only one record");
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
            try (ResultSet rs = conn.getMetaData().getTables(conn.getCatalog(), conn.getSchema(), tableName, null)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString("remarks"), tableComment);
                Assert.assertFalse(rs.next());
            }
        }
    }
}
