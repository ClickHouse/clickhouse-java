package com.clickhouse.jdbc;

import com.clickhouse.client.api.internal.ServerSettings;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
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

    /**
     * The writer's cleanup paths must be defensive: if the backing buffer fails to close, both the
     * post-insert reset (in {@code executeUpdate()}'s {@code finally}) and {@link WriterStatementImpl#close()}
     * must swallow the failure after logging it at DEBUG, never surfacing an I/O error from cleanup. A buffer
     * that throws on close is injected to exercise both catch blocks deterministically.
     */
    @Test(groups = {"integration"})
    public void testWriterCleanupSwallowsBufferCloseFailure() throws Exception {
        String table = "bt_writer_cleanup_close_fail";
        Properties properties = new Properties();
        properties.setProperty(DriverProperties.BETA_ROW_BINARY_WRITER.getKey(), "true");
        properties.setProperty(ASYNC_INSERT_SETTING_KEY, ServerSettings.OFF);
        try (Connection connection = getJdbcConnection(properties)) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + table);
                stmt.execute("CREATE TABLE " + table + " (field1 Int32) Engine MergeTree ORDER BY ()");
            }

            final int[] closeAttempts = {0};
            PreparedStatement ps = connection.prepareStatement("INSERT INTO " + table + " (field1) VALUES (?)");
            Assert.assertTrue(ps instanceof WriterStatementImpl);

            // Replace the writer's backing buffer with one that fails on close, so both cleanup paths hit
            // their defensive catch blocks: executeUpdate()'s finally -> resetWriter(), and close().
            Field outField = WriterStatementImpl.class.getDeclaredField("out");
            outField.setAccessible(true);
            outField.set(ps, new ByteArrayOutputStream() {
                @Override
                public void close() throws IOException {
                    closeAttempts[0]++;
                    throw new IOException("injected buffer close failure");
                }
            });

            // executeUpdate()'s finally resets the writer, closing the buffer; the injected failure must be
            // swallowed (an empty insert may error server-side, but the close failure must never be the cause).
            try {
                ps.executeUpdate();
            } catch (SQLException e) {
                Assert.assertFalse(hasInjectedCause(e),
                        "resetWriter()'s buffer-close failure must be swallowed, not surfaced: " + e);
            }

            // close() also closes the buffer; it must swallow the failure and not throw.
            ps.close();

            Assert.assertTrue(closeAttempts[0] >= 2,
                    "both executeUpdate()'s resetWriter and close() must attempt to close the writer buffer and "
                            + "swallow the failure; observed close attempts = " + closeAttempts[0]);

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    @DataProvider(name = "insertTargetFunctionForms")
    Object[][] insertTargetFunctionForms() {
        return new Object[][]{
                {"JAVACC", "INSERT INTO FUNCTION null('id UInt32') VALUES (?)"},
                {"JAVACC", "INSERT INTO TABLE FUNCTION null('id UInt32') VALUES (?)"},
                {"ANTLR4", "INSERT INTO FUNCTION null('id UInt32') VALUES (?)"},
                {"ANTLR4", "INSERT INTO TABLE FUNCTION null('id UInt32') VALUES (?)"},
                {"ANTLR4_PARAMS_PARSER", "INSERT INTO FUNCTION null('id UInt32') VALUES (?)"},
                {"ANTLR4_PARAMS_PARSER", "INSERT INTO TABLE FUNCTION null('id UInt32') VALUES (?)"},
        };
    }

    /**
     * An INSERT whose target is a table function has no plain table to fetch a schema for, so it must stay
     * on the regular SQL path instead of being routed to the RowBinary writer, which would look the
     * function name up as a table and fail with UNKNOWN_TABLE.
     */
    @Test(groups = {"integration"}, dataProvider = "insertTargetFunctionForms")
    public void testInsertIntoTableFunctionUsesSqlPath(String parser, String sql) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(DriverProperties.BETA_ROW_BINARY_WRITER.getKey(), "true");
        properties.setProperty(DriverProperties.SQL_PARSER.getKey(), parser);
        properties.setProperty(ASYNC_INSERT_SETTING_KEY, ServerSettings.OFF);
        try (Connection connection = getJdbcConnection(properties);
             PreparedStatement ps = connection.prepareStatement(sql)) {
            Assert.assertFalse(ps instanceof WriterStatementImpl,
                    "INSERT into a table function must not use the RowBinary writer: " + sql);
            ps.setInt(1, 42);
            Assert.assertEquals(ps.executeUpdate(), 1);
        }
    }

    @DataProvider(name = "insertTargetPlainTableForms")
    Object[][] insertTargetPlainTableForms() {
        return new Object[][]{
                {"JAVACC", "INSERT INTO %s VALUES (?)"},
                {"JAVACC", "INSERT INTO TABLE %s VALUES (?)"},
                {"ANTLR4", "INSERT INTO %s VALUES (?)"},
                {"ANTLR4", "INSERT INTO TABLE %s VALUES (?)"},
                {"ANTLR4_PARAMS_PARSER", "INSERT INTO %s VALUES (?)"},
                {"ANTLR4_PARAMS_PARSER", "INSERT INTO TABLE %s VALUES (?)"},
        };
    }

    @Test(groups = {"integration"}, dataProvider = "insertTargetPlainTableForms")
    public void testInsertIntoPlainTableStillUsesWriter(String parser, String sqlTemplate) throws SQLException {
        String table = "bt_writer_plain_table";
        Properties properties = new Properties();
        properties.setProperty(DriverProperties.BETA_ROW_BINARY_WRITER.getKey(), "true");
        properties.setProperty(DriverProperties.SQL_PARSER.getKey(), parser);
        properties.setProperty(ASYNC_INSERT_SETTING_KEY, ServerSettings.OFF);
        try (Connection connection = getJdbcConnection(properties)) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + table);
                stmt.execute("CREATE TABLE " + table + " (id Int32) Engine MergeTree ORDER BY ()");
            }

            try (PreparedStatement ps = connection.prepareStatement(String.format(sqlTemplate, table))) {
                Assert.assertTrue(ps instanceof WriterStatementImpl,
                        "INSERT into a plain table must keep using the RowBinary writer: " + sqlTemplate);
                ps.setInt(1, 42);
                Assert.assertEquals(ps.executeUpdate(), 1);
            } finally {
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("DROP TABLE IF EXISTS " + table);
                }
            }
        }
    }

    private static boolean hasInjectedCause(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof IOException && "injected buffer close failure".equals(c.getMessage())) {
                return true;
            }
        }
        return false;
    }
}
