package com.clickhouse.jdbc;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

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

    @DataProvider(name = "nonNullableArrayNullTables")
    public static Object[][] nonNullableArrayNullTables() {
        return new Object[][] {
                {"id Int32, arr Array(Int32), tail Int32"},
                {"id Int32, arr Array(Int32), tail Int32 DEFAULT 99"},
        };
    }

    @Test(groups = {"integration"}, dataProvider = "nonNullableArrayNullTables")
    public void testWriterNullIntoNonNullableArrayThrows(String columns) throws Exception {
        String table = "writer_stmt_arr_null_" + UUID.randomUUID().toString().replace('-', '_');
        runQuery("DROP TABLE IF EXISTS " + table);
        runQuery("CREATE TABLE " + table + " (" + columns + ") Engine = MergeTree ORDER BY id");

        Properties properties = new Properties();
        properties.setProperty(DriverProperties.BETA_ROW_BINARY_WRITER.getKey(), "true");
        try (Connection connection = getJdbcConnection(properties);
             PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + table + " VALUES (?, ?, ?)")) {
            Assert.assertTrue(stmt instanceof WriterStatementImpl);
            stmt.setInt(1, 1);
            stmt.setNull(2, Types.ARRAY);
            stmt.setInt(3, 7);

            SQLException thrown = Assert.expectThrows(SQLException.class, stmt::execute);
            Assert.assertTrue(hasNonNullableColumnMessage(thrown),
                    "Expected a clear non-nullable column error using " + columns + ", but got: " + thrown);
        }
    }

    @DataProvider(name = "nonNullableArrayRoundTrip")
    public static Object[][] nonNullableArrayRoundTrip() {
        return new Object[][] {
                {new ArrayList<Integer>(), 0, ""},
                {Arrays.asList(1, 2, 3), 3, "1,2,3"},
        };
    }

    @Test(groups = {"integration"}, dataProvider = "nonNullableArrayRoundTrip")
    public void testWriterNonNullableArrayRoundTrips(List<Integer> arr, int expectedLength, String expectedConcat) throws Exception {
        String table = "writer_stmt_arr_round_trip_" + UUID.randomUUID().toString().replace('-', '_');
        runQuery("DROP TABLE IF EXISTS " + table);
        runQuery("CREATE TABLE " + table + " (id Int32, arr Array(Int32), tail Int32) Engine = MergeTree ORDER BY id");

        Properties properties = new Properties();
        properties.setProperty(DriverProperties.BETA_ROW_BINARY_WRITER.getKey(), "true");
        try (Connection connection = getJdbcConnection(properties);
             PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + table + " VALUES (?, ?, ?)")) {
            stmt.setInt(1, 1);
            stmt.setObject(2, arr);
            stmt.setInt(3, 7);
            stmt.execute();
        }

        try (Connection connection = getJdbcConnection();
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT toInt32(length(arr)) AS alen, arrayStringConcat(arr, ',') AS acat, tail FROM "
                     + table + " ORDER BY id")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt("alen"), expectedLength);
            Assert.assertEquals(rs.getString("acat"), expectedConcat);
            Assert.assertEquals(rs.getInt("tail"), 7);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = {"integration"})
    public void testWriterNullIntoDefaultedArrayUsesDefault() throws Exception {
        String table = "writer_stmt_arr_null_default_" + UUID.randomUUID().toString().replace('-', '_');
        runQuery("DROP TABLE IF EXISTS " + table);
        runQuery("CREATE TABLE " + table + " (id Int32, arr Array(Int32) DEFAULT [1, 2], tail Int32) Engine = MergeTree ORDER BY id");

        Properties properties = new Properties();
        properties.setProperty(DriverProperties.BETA_ROW_BINARY_WRITER.getKey(), "true");
        try (Connection connection = getJdbcConnection(properties);
             PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + table + " VALUES (?, ?, ?)")) {
            stmt.setInt(1, 1);
            stmt.setNull(2, Types.ARRAY);
            stmt.setInt(3, 7);
            stmt.execute();
        }

        try (Connection connection = getJdbcConnection();
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT toInt32(length(arr)) AS alen, arrayStringConcat(arr, ',') AS acat, tail FROM "
                     + table + " ORDER BY id")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt("alen"), 2);
            Assert.assertEquals(rs.getString("acat"), "1,2");
            Assert.assertEquals(rs.getInt("tail"), 7);
        }
    }

    private static boolean hasNonNullableColumnMessage(Throwable thrown) {
        for (Throwable t = thrown; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains("An attempt to write null into not nullable column")) {
                return true;
            }
        }
        return false;
    }
}
