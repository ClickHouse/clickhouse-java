package com.clickhouse.jdbc;

import com.clickhouse.client.api.internal.ServerSettings;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

@Test(groups = {"integration"})
public class WriterStatementImplTest extends JdbcIntegrationTest {


    @Test(groups = {"integration"})
    public void testTargetTypeMethodThrowException() throws SQLException {

        Properties properties = new Properties();
        properties.setProperty(DriverProperties.BETA_ROW_BINARY_WRITER.getKey(), "true");
        try (Connection connection = getJdbcConnection(properties);
             PreparedStatement stmt = connection.prepareStatement("INSERT INTO system.numbers VALUES (?, ?)")) {
            Assert.assertTrue(stmt instanceof WriterStatementImpl);

            Assert.expectThrows(SQLException.class, () -> stmt.setObject(1, "", JDBCType.VARCHAR.getVendorTypeNumber()));
            Assert.expectThrows(SQLException.class, () -> stmt.setObject(1, "", JDBCType.VARCHAR));
            Assert.expectThrows(SQLException.class, () -> stmt.setObject(1, "", JDBCType.DECIMAL.getVendorTypeNumber(), 3));
            Assert.expectThrows(SQLException.class, () -> stmt.setObject(1, "", JDBCType.DECIMAL, 3));
        }
    }

    @DataProvider(name = "insertColumnListForms")
    Object[][] insertColumnListForms() {
        return new Object[][]{
                {"INSERT INTO %s (`field1`, `field2`, `field3`) VALUES (?, ?, ?)"},
                {"INSERT INTO %s (field1, field2, field3) VALUES (?, ?, ?)"},
        };
    }

    @Test(groups = {"integration"}, dataProvider = "insertColumnListForms")
    public void testInsertWithQuotedColumnNames(String sqlTemplate) throws SQLException {
        String table = "bt_writer_cols";
        Properties properties = new Properties();
        properties.setProperty(DriverProperties.BETA_ROW_BINARY_WRITER.getKey(), "true");
        // ANTLR4 backend extracts the explicit column list (default JAVACC does not).
        properties.setProperty(DriverProperties.SQL_PARSER.getKey(), "ANTLR4");
        properties.setProperty(ASYNC_INSERT_SETTING_KEY, ServerSettings.OFF);
        try (Connection connection = getJdbcConnection(properties)) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + table);
                stmt.execute("CREATE TABLE " + table +
                        " (field1 String, field2 Int32, field3 String) Engine MergeTree ORDER BY ()");
            }

            try (PreparedStatement ps = connection.prepareStatement(String.format(sqlTemplate, table))) {
                Assert.assertTrue(ps instanceof WriterStatementImpl);
                ps.setString(1, "alpha");
                ps.setInt(2, 42);
                ps.setString(3, "gamma");
                Assert.assertEquals(ps.executeUpdate(), 1);
            }

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT field1, field2, field3 FROM " + table)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "alpha");
                Assert.assertEquals(rs.getInt(2), 42);
                Assert.assertEquals(rs.getString(3), "gamma");
                Assert.assertFalse(rs.next());
            } finally {
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("DROP TABLE IF EXISTS " + table);
                }
            }
        }
    }
}
