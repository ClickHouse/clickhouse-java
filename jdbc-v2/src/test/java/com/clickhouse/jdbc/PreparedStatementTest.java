package com.clickhouse.jdbc;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseVersion;
import com.clickhouse.data.Tuple;
import com.clickhouse.jdbc.internal.JdbcUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

@Test(groups = { "integration" })
public class PreparedStatementTest extends JdbcIntegrationTest {

    @Test(groups = { "integration" })
    public void testSetNull() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setNull(1, Types.INTEGER);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertNull(rs.getObject(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetBoolean() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setBoolean(1, true);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertTrue(rs.getBoolean(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetByte() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setByte(1, (byte) 1);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals((byte) 1, rs.getByte(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetShort() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setShort(1, (short) 1);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals((short) 1, rs.getShort(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetInt() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setInt(1, 1);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetLong() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setLong(1, 1L);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1L, rs.getLong(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetFloat() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setFloat(1, 1.0f);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1.0f, rs.getFloat(1), 0.0f);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetDouble() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setDouble(1, 1.0);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1.0, rs.getDouble(1), 0.0);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetString() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setString(1, "test");
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("test", rs.getString(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetBytes() throws Exception {
        // see com.clickhouse.jdbc.JdbcDataTypeTests.testStringsUsedAsBytes
        // setBytes is dedicated for binary strings (see spec).
        // arrays are set via Array object.
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setBytes(1, new byte[] { 1, 2, 3 });
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getBytes(1), new byte[] { 1, 2, 3 });
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetDate() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT toDate(?)")) {
                stmt.setDate(1, java.sql.Date.valueOf("2021-01-01"), new GregorianCalendar(TimeZone.getTimeZone("UTC")));
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getDate(1), java.sql.Date.valueOf("2021-01-01"));
                    assertFalse(rs.next());
                }

                stmt.setDate(1, java.sql.Date.valueOf("2021-01-02"));
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getDate(1), java.sql.Date.valueOf("2021-01-02"));
                    assertFalse(rs.next());
                }

                stmt.setObject(1, java.sql.Date.valueOf("2021-01-02"));
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getDate(1), java.sql.Date.valueOf("2021-01-02"));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetTime() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT toDateTime(?)")) {
                stmt.setTime(1, java.sql.Time.valueOf("12:34:56"), new GregorianCalendar(TimeZone.getTimeZone("UTC")));
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getTime(1).toString(), "12:34:56");
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetTimestamp() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT toDateTime64(?, 3)")) {
                stmt.setTimestamp(1, java.sql.Timestamp.valueOf("2021-01-01 01:34:56.456"), new GregorianCalendar(TimeZone.getTimeZone("UTC")));
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getTimestamp(1).toString(), "2021-01-01 01:34:56.456");
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testBigDecimal() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setBigDecimal(1, java.math.BigDecimal.valueOf(1.0));
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(java.math.BigDecimal.valueOf(1.0), rs.getBigDecimal(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testPrimitiveArrays() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setObject(1, new String[][] {new String[]{"a"}, new String[]{"b"}, new String[]{"c"}});
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    Array a1 = rs.getArray(1);
                    assertNotNull(a1);
                    assertEquals(Arrays.deepToString((Object[]) a1.getArray()), "[[a], [b], [c]]");
                    Array a2 = rs.getObject(1, Array.class);
                    assertNotNull(a2);
                    assertEquals(Arrays.deepToString((Object[]) a2.getArray()), "[[a], [b], [c]]");
                    Array a3 = rs.getObject(1) instanceof Array ? (Array) rs.getObject(1) : null;
                    assertNotNull(a3);
                    assertEquals(Arrays.deepToString((Object[]) a3.getArray()), "[[a], [b], [c]]");
                    assertFalse(rs.next());
                }
            }

            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setObject(1, new Object[] {1, 2, 3});
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    Array a1 = rs.getArray(1);
                    assertNotNull(a1);
                    assertEquals(Arrays.deepToString((Object[]) a1.getArray()), "[1, 2, 3]");
                }
            }
        }
    }

    @Test
    public void testTuple() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setObject(1, new Tuple("a", 1));

                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(Arrays.asList(rs.getObject(1, Object[].class)).toString(), "[a, 1]");
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = {"integration"})
    public void testSetAsciiStream() throws Exception {
        final String value = "Some long string with '' to check quote escaping";
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ? as v1, ? as v2")) {
                stmt.setAsciiStream(1, new ByteArrayInputStream(value.getBytes()));
                stmt.setAsciiStream(2, new ByteArrayInputStream(value.getBytes()), 10);

                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), value);
                    assertEquals(rs.getString(2), value.subSequence(0, 10));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = {"integration"})
    public void testSetUnicodeStream() throws Exception {
        final String value = "Some long string with '' to check quote escaping";
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ? as v1, ? as v2")) {
                stmt.setUnicodeStream(1, new ByteArrayInputStream(value.getBytes()),  value.length());
                stmt.setUnicodeStream(2, new ByteArrayInputStream(value.getBytes()), 10);

                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), value);
                    assertEquals(rs.getString(2), value.subSequence(0, 10));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = {"integration"})
    public void testSetBinaryStream() throws Exception {
        final String value = "Some long string with '' to check quote escaping";
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ? as v1, ? as v2")) {
                stmt.setBinaryStream(1, new ByteArrayInputStream(value.getBytes()));
                stmt.setBinaryStream(2, new ByteArrayInputStream(value.getBytes()), 10);

                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), value);
                    assertEquals(rs.getString(2), value.subSequence(0, 10));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testSetNCharacterStream() throws Exception {
        final String value = "Some long string with '' to check quote escaping";
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ? as v1, ? as v2")) {
                stmt.setNCharacterStream(1, new InputStreamReader(new ByteArrayInputStream(value.getBytes())));
                stmt.setNCharacterStream(2, new InputStreamReader(new ByteArrayInputStream(value.getBytes())), 10);

                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), value);
                    assertEquals(rs.getString(2), value.subSequence(0, 10));
                    assertFalse(rs.next());
                }
            }
        }
    }


    @Test(groups = { "integration" })
    public void testEscapeStrings() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT FALSE OR ? = 'test', ?")) {
                stmt.setString(1, "test\\' OR 1 = 1 --");
                stmt.setString(2, "test\\\\' OR 1 = 1 --");
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), "false");
                    assertEquals(rs.getString(2), "test\\\\' OR 1 = 1 --");
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testTernaryOperator() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ( TRUE ? 1 : 0) as val1, ? as val2")) {
                stmt.setString(1, "test\\' OR 1 = 1 --");
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString("val1"), "1");
                    assertEquals(rs.getString(2), "test\\' OR 1 = 1 --");
                    assertFalse(rs.next());
                }
            }
        }
    }


    @Test(groups = "integration")
    void testWithClause() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("with data as (SELECT number FROM numbers(100)) select * from data ")) {
                stmt.execute();
                ResultSet rs = stmt.getResultSet();
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertEquals(count, 100);
            }
        }
    }

    @Test(groups = "integration")
    void testWithClauseWithParams() throws Exception {
        final String table = "test_with_stmt";
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + table);
                stmt.execute("CREATE TABLE " + table + " (v1 String) Engine MergeTree ORDER BY ()");
                stmt.execute("INSERT INTO " + table + " VALUES ('A'), ('B')");
            }
            final Timestamp target_time = Timestamp.valueOf(LocalDateTime.now());
            try (PreparedStatement stmt = conn.prepareStatement("WITH " +
                    " toDateTime(?) as target_time, " +
                    " (SELECT 123) as magic_number" +
                    " SELECT *, target_time, magic_number FROM " + table)) {
                stmt.setTimestamp(1, target_time);
                stmt.execute();
                ResultSet rs = stmt.getResultSet();
                int count = 0;
                assertEquals(rs.getMetaData().getColumnCount(), 3);
                while (rs.next()) {
                    Assert.assertEquals(
                            rs.getTimestamp("target_time").toLocalDateTime().truncatedTo(ChronoUnit.SECONDS),
                            target_time.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime().truncatedTo(ChronoUnit.SECONDS));
                    Assert.assertEquals(rs.getString("magic_number"), "123");
                    Assert.assertEquals(
                            rs.getTimestamp(2).toLocalDateTime().truncatedTo(ChronoUnit.SECONDS),
                            target_time.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime().truncatedTo(ChronoUnit.SECONDS));
                    Assert.assertEquals(rs.getString(3), "123");

                    count++;
                }
                assertEquals(count, 2, "Expected 2 rows");

            }
        }
    }

    @Test(groups = { "integration" })
    void testMultipleWithClauses() throws Exception {
        try (Connection conn = getJdbcConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "WITH data1 AS (SELECT 1 AS a), " +
                             "     data2 AS (SELECT a + 1 AS b FROM data1) " +
                             "SELECT * FROM data2")) {
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test(groups = { "integration" })
    void testRecursiveWithClause() throws Exception {
        if (ClickHouseVersion.of(getServerVersion()).check("(,24.3]")) {
            return; // recursive CTEs were introduced in 24.4
        }

        try (Connection conn = getJdbcConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "WITH RECURSIVE numbers AS (" +
                             "    SELECT 1 AS n " +
                             "    UNION ALL " +
                             "    SELECT n + 1 FROM numbers WHERE n < 5" +
                             ") " +
                             "SELECT * FROM numbers ORDER BY n")) {
            ResultSet rs = stmt.executeQuery();
            for (int i = 1; i <= 5; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());
        }
    }

    @Test(groups = { "integration" })
    void testWithClauseWithMultipleParameters() throws Exception {
        try (Connection conn = getJdbcConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "WITH data AS (" +
                             "    (SELECT number AS n " +
                             "    FROM numbers(?) " +
                             "    WHERE n > ?)" +
                             ") " +
                             "SELECT * FROM data WHERE n < ?")) {
//"WITH data AS (    (SELECT number AS n     FROM numbers(?)     WHERE n > ?)) SELECT * FROM data WHERE n < ?"
            stmt.setInt(1, 10);  // numbers(10) = 0-9
            stmt.setInt(2, 3);   // n > 3
            stmt.setInt(3, 7);   // n < 7

            ResultSet rs = stmt.executeQuery();
            int count = 0;
            int expected = 4;     // 4,5,6
            while (rs.next()) {
                count++;
                int n = rs.getInt(1);
                assertTrue(n > 3 && n < 7);
            }
            assertEquals(3, count);
        }
    }

    @Test(groups = { "integration" })
    void testSelectFromArray() throws Exception {
        try (Connection conn = getJdbcConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT * FROM numbers(?)")) {
            stmt.setInt(1, 10);  // numbers(10) = 0-9
            ResultSet rs = stmt.executeQuery();
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertEquals(10, count);
        }
    }

    @Test(groups = { "integration" })
    void testInsert() throws Exception {
        int ROWS = 1000;
        String payload = RandomStringUtils.random(1024, true, true);
        try (Connection conn = getJdbcConnection()) {
            for (int j = 0; j < 10; j++) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE insert_batch ( `off16` Int16, `str` String, `p_int8` Int8, `p_int16` Int16, `p_int32` Int32, `p_int64` Int64, `p_float32` Float32, `p_float64` Float64, `p_bool` Bool) ENGINE = Memory");
                }
                String insertQuery = "INSERT INTO insert_batch (off16, str, p_int8, p_int16, p_int32, p_int64, p_float32, p_float64, p_bool) VALUES (?,?,?,?,?,?,?,?,?)";
                try (PreparedStatement stmt = conn.prepareStatement(insertQuery)) {
                    for (int i = 0; i < ROWS; i++) {
                        stmt.setShort(1, (short) i);
                        stmt.setString(2, payload);
                        stmt.setByte(3, (byte)i);
                        stmt.setShort(4, (short)i);
                        stmt.setInt(5, i);
                        stmt.setLong(6, (long)i);
                        stmt.setFloat(7, (float)(i*0.1));
                        stmt.setDouble(8, (double)(i*0.1));
                        stmt.setBoolean(9, true);
                        stmt.addBatch();
                    }
                    long startBatchTime = System.currentTimeMillis();
                    stmt.executeBatch();
                    long endBatchTime = System.currentTimeMillis();
                    System.out.println("Insertion Time for Final Batch: " + (endBatchTime - startBatchTime) + " ms");
                }
                // count rows
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM insert_batch")) {
                        assertTrue(rs.next());
                        assertEquals(rs.getInt(1), ROWS);
                    }
                }
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("DROP TABLE insert_batch");
                }

            }

        }
    }

    @Test(dataProvider = "testGetMetadataDataProvider")
    void testGetMetadata(String sql, int colCountBeforeExecution, Object[] values,
                         int colCountAfterExecution) throws Exception {
        String tableName = "test_get_metadata";
        runQuery("CREATE TABLE IF NOT EXISTS " + tableName + " ( a1 String, b2 Float, b3 Float ) Engine=MergeTree ORDER BY ()");

        try (Connection conn = getJdbcConnection();
             PreparedStatement stmt = conn.prepareStatement(String.format(sql, tableName))) {
            ResultSetMetaData metadataRs = stmt.getMetaData();
            assertNotNull(metadataRs);
            Assert.assertEquals(metadataRs.getColumnCount(), colCountBeforeExecution);

            for (int i = 1; i <= metadataRs.getColumnCount(); i++) {
                assertEquals(metadataRs.getSchemaName(i), stmt.getConnection().getSchema());
            }

            if (values != null) {
                for (int i = 0; i < values.length; i++) {
                    stmt.setObject(i + 1, values[i]);
                }
            }

            stmt.execute();
            metadataRs = stmt.getMetaData();

            assertNotNull(metadataRs);
            assertEquals(metadataRs.getColumnCount(), colCountAfterExecution);
            for (int i = 1; i <= metadataRs.getColumnCount(); i++) {
                assertEquals(metadataRs.getSchemaName(i), stmt.getConnection().getSchema());
            }
        }
    }

    @DataProvider(name = "testGetMetadataDataProvider")
    static Object[][] testGetMetadataDataProvider() {
        return new Object[][] {
                {"INSERT INTO `%s` VALUES (?, ?, ?)", 3, new Object[]{"test", 0.3f, 0.4f}, 3},
                {"SELECT * FROM `%s`", 3, null, 3},
                {"SHOW TABLES", 0, null, 1}
        };
    }

    @Test(groups = { "integration" })
    void testMetabaseBug01() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE `users` (`id` Int32, `name` Nullable(String), `last_login` Nullable(DateTime64(3, 'GMT0')), `password` Nullable(String)) ENGINE Memory;");
                stmt.execute("CREATE TABLE `users_tmp` (`id` Int32, `name` Nullable(String), `last_login` Nullable(DateTime64(3, 'GMT0')), `password` Nullable(String)) ENGINE Memory;");
                stmt.execute("CREATE TABLE `users_tmp01` (`id` Int32, `name` Nullable(String), `last_login` Nullable(DateTime64(3, 'GMT0')), `password` Nullable(String)) ENGINE Memory;");
                stmt.execute("CREATE TABLE `users_tmp02` (`id` Int32, `name` Nullable(String), `last_login` Nullable(DateTime64(3, 'GMT0')), `password` Nullable(String)) ENGINE Memory;");
            }
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO `users` (`name`, `last_login`, `password`, `id`) VALUES (?, `parseDateTimeBestEffort`(?, ?), ?, 1), (?, `parseDateTimeBestEffort`(?, ?), ?, 2), (?, `parseDateTimeBestEffort`(?, ?), ?, 3), (?, `parseDateTimeBestEffort`(?, ?), ?, 4), (?, `parseDateTimeBestEffort`(?, ?), ?, 5), (?, `parseDateTimeBestEffort`(?, ?), ?, 6), (?, `parseDateTimeBestEffort`(?, ?), ?, 7), (?, `parseDateTimeBestEffort`(?, ?), ?, 8), (?, `parseDateTimeBestEffort`(?, ?), ?, 9), (?, `parseDateTimeBestEffort`(?, ?), ?, 10), (?, `parseDateTimeBestEffort`(?, ?), ?, 11), (?, `parseDateTimeBestEffort`(?, ?), ?, 12), (?, `parseDateTimeBestEffort`(?, ?), ?, 13), (?, `parseDateTimeBestEffort`(?, ?), ?, 14), (?, `parseDateTimeBestEffort`(?, ?), ?, 15)")) {
                stmt.setObject(1, "Plato Yeshua");
                stmt.setObject(2, "2014-04-01 08:30:00.000");
                stmt.setObject(3, "UTC");
                stmt.setObject(4, "4be68cda-6fd5-4ba7-944e-2b475600bda5");
                stmt.setObject(5, "Felipinho Asklepios");
                stmt.setObject(6, "2014-12-05 15:15:00.000");
                stmt.setObject(7, "UTC");
                stmt.setObject(8, "5bb19ad9-f3f8-421f-9750-7d398e38428d");
                stmt.setObject(9, "Kaneonuskatew Eiran");
                stmt.setObject(10, "2014-11-06 16:15:00.000");
                stmt.setObject(11, "UTC");
                stmt.setObject(12, "a329ccfe-b99c-42eb-9c93-cb9adc3eb1ab");
                stmt.setObject(13, "Simcha Yan");
                stmt.setObject(14, "2014-01-01 08:30:00.000");
                stmt.setObject(15, "UTC");
                stmt.setObject(16, "a61f97c6-4484-4a63-b37e-b5e58bfa2ecb");
                stmt.setObject(17, "Quentin Sören");
                stmt.setObject(18, "2014-10-03 17:30:00.000");
                stmt.setObject(19, "UTC");
                stmt.setObject(20, "10a0fea8-9bb4-48fe-a336-4d9cbbd78aa0");
                stmt.setObject(21, "Shad Ferdynand");
                stmt.setObject(22, "2014-08-02 12:30:00.000");
                stmt.setObject(23, "UTC");
                stmt.setObject(24, "d35c9d78-f9cf-4f52-b1cc-cb9078eebdcb");
                stmt.setObject(25, "Conchúr Tihomir");
                stmt.setObject(26, "2014-08-02 09:30:00.000");
                stmt.setObject(27, "UTC");
                stmt.setObject(28, "900335ad-e03b-4259-abc7-76aac21cedca");
                stmt.setObject(29, "Szymon Theutrich");
                stmt.setObject(30, "2014-02-01 10:15:00.000");
                stmt.setObject(31, "UTC");
                stmt.setObject(32, "d6c47a54-9d88-4c4a-8054-ace76764ed0d");
                stmt.setObject(33, "Nils Gotam");
                stmt.setObject(34, "2014-04-03 09:30:00.000");
                stmt.setObject(35, "UTC");
                stmt.setObject(36, "b085040c-7aa4-4e96-8c8f-420b2c99c920");
                stmt.setObject(37, "Frans Hevel");
                stmt.setObject(38, "2014-07-03 19:30:00.000");
                stmt.setObject(39, "UTC");
                stmt.setObject(40, "b7a43e91-9fb9-4fe9-ab6f-ea51ab0f94e4");
                stmt.setObject(41, "Spiros Teofil");
                stmt.setObject(42, "2014-11-01 07:00:00.000");
                stmt.setObject(43, "UTC");
                stmt.setObject(44, "62b9602c-27b8-44ea-adbd-2748f26537af");
                stmt.setObject(45, "Kfir Caj");
                stmt.setObject(46, "2014-07-03 01:30:00.000");
                stmt.setObject(47, "UTC");
                stmt.setObject(48, "dfe21df3-f364-479d-a5e7-04bc5d85ad2b");
                stmt.setObject(49, "Dwight Gresham");
                stmt.setObject(50, "2014-08-01 10:30:00.000");
                stmt.setObject(51, "UTC");
                stmt.setObject(52, "75a1ebf1-cae7-4a50-8743-32d97500f2cf");
                stmt.setObject(53, "Broen Olujimi");
                stmt.setObject(54, "2014-10-03 13:45:00.000");
                stmt.setObject(55, "UTC");
                stmt.setObject(56, "f9b65c74-9f91-4cfd-9248-94a53af82866");
                stmt.setObject(57, "Rüstem Hebel");
                stmt.setObject(58, "2014-08-01 12:45:00.000");
                stmt.setObject(59, "UTC");
                stmt.setObject(60, "02ad6b15-54b0-4491-bf0f-d781b0a2c4f5");
                stmt.addBatch();
                stmt.executeBatch();
            }
            try (Statement stmt01 = conn.createStatement()) {
                try (ResultSet rs = stmt01.executeQuery("SELECT count(*) FROM `users`")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), 15);
                }
            }

            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO `users_tmp` SELECT * FROM `users` WHERE id = ?")) {
                stmt.setInt(1, 1);
                stmt.addBatch();
                stmt.setInt(1, 2);
                stmt.addBatch();
                stmt.setInt(1, 3);
                stmt.addBatch();
                stmt.executeBatch();
            }

            StringBuilder sb = new StringBuilder();
            try (Statement stmt01 = conn.createStatement()) {
                try (ResultSet rs = stmt01.executeQuery("SELECT id, name FROM `users_tmp`")) {
                    while (rs.next()) {
                        sb.append(rs.getInt(1)).append(",").append(rs.getString(2)).append(";");
                    }
                }
            }


            try (Statement stmt01 = conn.createStatement()) {
                try (ResultSet rs = stmt01.executeQuery("SELECT count(*) FROM `users_tmp`")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), 3,"Users in users_tmp: " + sb);
                }
            }

            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO `users_tmp01` SELECT * FROM `users` WHERE id = ?")) {
                stmt.setInt(1, 1);
                stmt.addBatch();
                stmt.executeBatch();
            }

            try (Statement stmt01 = conn.createStatement()) {
                try (ResultSet rs = stmt01.executeQuery("SELECT count(*) FROM `users_tmp01`")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), 1);
                }
            }

            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO `users_tmp02` (`name`, `last_login`, `password`, `id`) VALUES (?, `parseDateTimeBestEffort`(?, ?), ?, ?)")) {
                for (int i=0; i < 10; i++) {
                    stmt.setObject(1, "Plato Yeshua");
                    stmt.setObject(2, "2014-04-01 08:30:00.000");
                    stmt.setObject(3, "UTC");
                    stmt.setObject(4, "4be68cda-6fd5-4ba7-944e-2b475600bda5");
                    stmt.setObject(5, i);
                    stmt.addBatch();
                }
                stmt.executeBatch();
            }

            try (Statement stmt01 = conn.createStatement()) {
                try (ResultSet rs = stmt01.executeQuery("SELECT count(*) FROM `users_tmp02`")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), 10);
                }
            }
        }
    }

    @Test(groups = { "integration" })
    void testStatementSplit() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS `with_complex_id` (`v?``1` Int32, " +
                        "\"v?\"\"2\" Int32,`v?\\`3` Int32, \"v?\\\"4\" Int32) ENGINE MergeTree ORDER BY ();");
                stmt.execute("CREATE TABLE IF NOT EXISTS `test_stmt_split2` (v1 Int32, v2 String) ENGINE MergeTree ORDER BY (); ");
                stmt.execute("INSERT INTO `test_stmt_split2` VALUES (1, 'abc'), (2, '?'), (3, '?')");
            }
            String insertQuery = "-- line comment1 ?\n"
                    + "# line comment2 ?\n"
                    + "#! line comment3 ?\n"
                    + "/* block comment ? \n */"
                    + "INSERT INTO `with_complex_id`(`v?``1`, \"v?\"\"2\",`v?\\`3`, \"v?\\\"4\") VALUES (?, ?, ?, ?);";
            try (PreparedStatement stmt = conn.prepareStatement(insertQuery)) {
                stmt.setInt(1, 1);
                stmt.setInt(2, 2);
                stmt.setInt(3, 3);
                stmt.setInt(4, 4);
                stmt.execute();
            }
            String selectQuery = "-- line comment ?\n"
                    + "/* block comment ? \n */"
                    + "SELECT `v?``1`, \"v?\"\"2\",`v?\\`3`, \"v?\\\"4\", 'test '' string1 ?', 'test \\' string2 ?', 'test string3 ?\\\\' FROM `with_complex_id` WHERE `v?``1` = ? AND \"v?\"\"2\" = ? AND `v?\\`3` = ? AND \"v?\\\"4\" = ?";
            try (PreparedStatement stmt = conn.prepareStatement(selectQuery)) {
                stmt.setInt(1, 1);
                stmt.setInt(2, 2);
                stmt.setInt(3, 3);
                stmt.setInt(4, 4);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), 1);
                    assertEquals(rs.getInt(2), 2);
                    assertEquals(rs.getInt(3), 3);
                    assertEquals(rs.getInt(4), 4);
                    assertEquals(rs.getString(5), "test ' string1 ?");
                    assertEquals(rs.getString(6), "test ' string2 ?");
                    assertEquals(rs.getString(7), "test string3 ?\\");
                }
            }

            try (PreparedStatement stmt = conn.prepareStatement("SELECT v1 FROM `test_stmt_split2` WHERE v1 > ? AND v2 = '?'")) {
                stmt.setInt(1, 2);
                try (ResultSet rs = stmt.executeQuery()) {
                    int count = 0;
                    while (rs.next()) {
                        count++;
                        assertEquals(rs.getInt(1), 3);
                    }

                    Assert.assertEquals(count, 1);
                }
            }

        }
    }

    @Test(groups = {"integration"})
    void testClearParameters() throws Exception {
        final String sql = "insert into `test_issue_2299` (`id`, `name`, `age`) values (?, ?, ?)";


        try (Connection conn = getJdbcConnection();) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS `test_issue_2299` (`id` Nullable(String), `name` Nullable(String), `age` Int32) ENGINE Memory;");
            }

            try (PreparedStatementImpl ps = (PreparedStatementImpl) conn.prepareStatement(sql)) {

                Assert.assertEquals(ps.getParametersCount(), 3);

                ps.setString(1, "testId");
                ps.setString(2, "testName");
                ps.setInt(3, 18);
                ps.execute();

                ps.clearParameters();
                Assert.assertEquals(ps.getParametersCount(), 3);

                ps.setString(1, "testId2");
                ps.setString(2, "testName2");
                ps.setInt(3, 19);
                ps.execute();
            }
        }
    }

    @DataProvider
    Object[][] testBatchInsertWithRowBinary_dp() {
        return new Object[][]{
                {"INSERT  INTO \n `%s` \nVALUES (?, ?, abs(?), ?)", PreparedStatementImpl.class}, // only string possible (because of abs(?))
                {"INSERT  INTO\n `%s` \nVALUES (?, ?, ?, ?)", WriterStatementImpl.class}, // row binary writer
                {" INSERT INTO %s (ts, v1, v2, v3) VALUES (?, ?, ?, ?)", WriterStatementImpl.class}, // only string supported now
                {"INSERT INTO %s SELECT ?, ?, ?, ?", PreparedStatementImpl.class}, // only string possible (because of SELECT)
        };
    }

    @Test(dataProvider = "testBatchInsertWithRowBinary_dp")
    void testBatchInsertWithRowBinary(String sql, Class implClass) throws Exception {
        String table = "test_batch";
        long seed = System.currentTimeMillis();
        Random rnd = new Random(seed);
        System.out.println("testBatchInsert seed" + seed);
        Properties properties = new Properties();
        properties.put(DriverProperties.BETA_ROW_BINARY_WRITER.getKey(), "true");
        try (Connection conn = getJdbcConnection(properties)) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS " + table +
                        " ( ts DateTime, v1 Int32, v2 Float32, v3 Int32) Engine MergeTree ORDER BY ()");
            }

            final int nBatches = 10;
            try (PreparedStatement stmt = conn.prepareStatement(String.format(sql, table))) {
                Assert.assertEquals(stmt.getClass(), implClass);
                for (int bI = 0; bI < nBatches; bI++) {
                    stmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
                    stmt.setInt(2, rnd.nextInt());
                    stmt.setFloat(3, rnd.nextFloat());
                    stmt.setInt(4, rnd.nextInt());
                    stmt.addBatch();
                }

                int[] result = stmt.executeBatch();
                for (int r : result) {
                    Assert.assertTrue(r == 1 || r == PreparedStatement.SUCCESS_NO_INFO);
                }
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);) {

                int count = 0;
                while (rs.next()) {
                    Timestamp ts = rs.getTimestamp(1);
                    assertNotNull(ts);
                    assertTrue(rs.getInt(2) != 0);
                    assertTrue(rs.getFloat(3) != 0.0f);
                    assertTrue(rs.getInt(4) != 0);
                    count++;
                }
                assertEquals(count, nBatches);

                stmt.execute("TRUNCATE " + table);
            }
        }
    }

    @DataProvider
    Object[][] testBatchInsertTextStatement_dp() {
        return new Object[][]{
                {"INSERT  INTO \n `%s` \nVALUES (?, ?, ?, ?)"}, // simple
                {" INSERT INTO %s (ts, v1, v2, v3) VALUES (?, ?, ?, ?)"},
        };
    }

    @Test(dataProvider = "testBatchInsertTextStatement_dp")
    void testBatchInsertTextStatement(String sql) throws Exception {
        String table = "test_batch_text";
        long seed = System.currentTimeMillis();
        Random rnd = new Random(seed);
        try (Connection conn = getJdbcConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS " + table +
                        " ( ts DateTime DEFAULT now(), v1 Int32, v2 Float32, v3 Int32) Engine MergeTree ORDER BY ()");
            }

            final int nBatches = 10;
            try (PreparedStatement stmt = conn.prepareStatement(String.format(sql, table))) {
                Assert.assertEquals(stmt.getClass(), PreparedStatementImpl.class);
                for (int bI = 0; bI < nBatches; bI++) {
                    stmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
                    stmt.setInt(2, rnd.nextInt());
                    stmt.setFloat(3, rnd.nextFloat());
                    stmt.setInt(4, rnd.nextInt());
                    stmt.addBatch();
                }

                int[] result = stmt.executeBatch();
                for (int r : result) {
                    Assert.assertEquals(r, 1);
                }
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);) {

                int count = 0;
                while (rs.next()) {
                    Timestamp ts = rs.getTimestamp(1);
                    assertNotNull(ts);
                    assertTrue(rs.getInt(2) != 0);
                    assertTrue(rs.getFloat(3) != 0.0f);
                    assertTrue(rs.getInt(4) != 0);
                    count++;
                }
                assertEquals(count, nBatches);

                stmt.execute("TRUNCATE " + table);
            }

        }
    }

    @Test(groups = {"integration"})
    void testBatchInsertNoValuesReuse() throws Exception {
        String table = "test_pstmt_batch_novalues_reuse";
        String sql = "INSERT INTO %s (v1, v2) VALUES (?, ?)";
        long seed = System.currentTimeMillis();
        Random rnd = new Random(seed);
        try (Connection conn = getJdbcConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS " + table +
                        " (v1 Int32, v2 Int32) Engine MergeTree ORDER BY tuple()");
            }

            final int nBatches = 10;
            try (PreparedStatement stmt = conn.prepareStatement(String.format(sql, table))) {
                Assert.assertEquals(stmt.getClass(), PreparedStatementImpl.class);
                // add a batch with invalid values
                stmt.setString(1, "invalid");
                stmt.setInt(2, rnd.nextInt());
                stmt.addBatch();
                assertThrows(SQLException.class, stmt::executeBatch);
                // should fail due to the previous batch data.
                assertThrows(SQLException.class, stmt::executeBatch);
                // clear previous batch data
                stmt.clearBatch();

                for (int step = 0; step < 2; step++) {
                    for (int bI = 0; bI < (nBatches >> 1); bI++) {
                        stmt.setInt(1, rnd.nextInt());
                        stmt.setInt(2, rnd.nextInt());
                        stmt.addBatch();
                    }

                    // reuse the same statement
                    int[] result = stmt.executeBatch();
                    for (int r : result) {
                        Assert.assertEquals(r, 1);
                    }
                }
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);) {

                int count = 0;
                while (rs.next()) {
                    assertTrue(rs.getInt(1) != 0);
                    assertTrue(rs.getInt(2) != 0);
                    count++;
                }
                assertEquals(count, nBatches);
            }
        }
    }

    @Test()
    void testBatchInsertValuesReuse() throws Exception {
        String table = "test_pstmt_batch_values_reuse";
        String sql = "INSERT INTO %s (v1, v2) VALUES (1, ?)";
        long seed = System.currentTimeMillis();
        Random rnd = new Random(seed);
        try (Connection conn = getJdbcConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS " + table +
                        " (v1 Int32, v2 Int32) Engine MergeTree ORDER BY tuple()");
            }

            final int nBatches = 10;
            try (PreparedStatement stmt = conn.prepareStatement(String.format(sql, table))) {
                Assert.assertEquals(stmt.getClass(), PreparedStatementImpl.class);
                // add a batch with invalid values
                stmt.setString(1, "invalid");
                stmt.addBatch();
                assertThrows(SQLException.class, stmt::executeBatch);
                // should fail due to the previous batch data.
                assertThrows(SQLException.class, stmt::executeBatch);
                // clear previous batch data
                stmt.clearBatch();

                for (int step = 0; step < 2; step++) {
                    for (int bI = 0; bI < (nBatches >> 1); bI++) {
                        stmt.setInt(1, rnd.nextInt());
                        stmt.addBatch();
                    }

                    // reuse the same statement
                    int[] result = stmt.executeBatch();
                    for (int r : result) {
                        Assert.assertEquals(r, 1);
                    }
                }
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);) {

                int count = 0;
                while (rs.next()) {
                    assertTrue(rs.getInt(1) != 0);
                    assertTrue(rs.getInt(2) != 0);
                    count++;
                }
                assertEquals(count, nBatches);
            }
        }
    }

    @Test(groups = {"integration"})
    void testWriteUUID() throws Exception {
        String sql = "insert into `test_issue_2327` (`id`, `uuid`) values (?, ?)";
        try (Connection conn = getJdbcConnection();
             PreparedStatementImpl ps = (PreparedStatementImpl) conn.prepareStatement(sql)) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS `test_issue_2327` (`id` Nullable(String), `uuid` UUID) ENGINE Memory;");
            }
            UUID uuid = UUID.randomUUID();
            ps.setString(1, "testId01");
            ps.setObject(2, uuid);
            ps.execute();

            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT count(*) FROM `test_issue_2327`");
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
            }

            final String selectSQL = "SELECT * FROM `test_issue_2327` WHERE " +
                    "`" + getDatabase() + "`.`test_issue_2327`.`uuid` IN (CAST(? AS UUID))";
            try (PreparedStatement stmt = conn.prepareStatement(selectSQL)) {
                stmt.setString(1, uuid.toString());
                try (ResultSet rs = stmt.executeQuery()) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(rs.getString(1), "testId01");
                    Assert.assertEquals(rs.getString(2), uuid.toString());
                }
            }
        }

    }

    @Test(groups = {"integration"})
    void testWriteCollection() throws Exception {
        String sql = "insert into `test_issue_2329` (`id`, `name`, `age`, `arr`) values (?, ?, ?, ?)";
        try (Connection conn = getJdbcConnection();
             PreparedStatementImpl ps = (PreparedStatementImpl) conn.prepareStatement(sql)) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS `test_issue_2329` (`id` Nullable(String), `name` Nullable(String), `age` Int32, `arr` Array(String)) ENGINE Memory;");
            }

            Assert.assertEquals(ps.getParametersCount(), 4);
            Collection<String> arr = new ArrayList<String>();
            ps.setString(1, "testId01");
            ps.setString(2, "testName");
            ps.setInt(3, 18);
            ps.setObject(4, arr);
            ps.execute();

            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT count(*) FROM `test_issue_2329`");
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
            }
        }

    }

    @Test
    void testMethodsNotAllowedToBeCalled() throws Exception {
        /* Story About Broken API
         * There is a Statement interface. It is designed to operate with single statements.
         * So there are method like execute(String) and addBatch(String).
         * Some statements may be repeated over and over again. And they should be constructed
         * over and over again. PreparedStatement was created to solve the issue by accepting
         * an SQL statement as constructor parameter and making its method work in context of
         * one, prepared SQL statement.
         * But someone missed their OOP classes and done this:
         *   "interface PreparedStatement extends Statement"
         * and
         *  declared some method from Statement interface not to be called on PreparedStatement
         * instances.
         * That is how today we have a great confusion and have to check it in all implementations.
         */
        String sql = "SELECT number FROM system.numbers WHERE number = ?";
        try (Connection conn = getJdbcConnection();
             PreparedStatementImpl ps = (PreparedStatementImpl) conn.prepareStatement(sql)) {

            Assert.assertThrows(SQLException.class, () -> ps.addBatch(sql));
            Assert.assertThrows(SQLException.class, () -> ps.executeQuery(sql));
            Assert.assertThrows(SQLException.class, () -> ps.execute(sql));
            Assert.assertThrows(SQLException.class, () -> ps.execute(sql, new int[]{0}));
            Assert.assertThrows(SQLException.class, () -> ps.execute(sql, new String[]{""}));
            Assert.assertThrows(SQLException.class, () -> ps.executeUpdate(sql));
            Assert.assertThrows(SQLException.class, () -> ps.executeUpdate(sql, new int[]{0}));
            Assert.assertThrows(SQLException.class, () -> ps.executeUpdate(sql, new String[]{""}));
            Assert.assertThrows(SQLException.class, () -> ps.executeLargeUpdate(sql));
            Assert.assertThrows(SQLException.class, () -> ps.executeLargeUpdate(sql, new int[]{0}));
            Assert.assertThrows(SQLException.class, () -> ps.executeLargeUpdate(sql, new String[]{""}));
        }
    }

    @Test(dataProvider = "testReplaceQuestionMark_dataProvider")
    public void testReplaceQuestionMark(String sql, String result) {
        assertEquals(PreparedStatementImpl.replaceQuestionMarks(sql, "NULL"), result);
    }

    @DataProvider(name = "testReplaceQuestionMark_dataProvider")
    public static Object[][] testReplaceQuestionMark_dataProvider() {
        return new Object[][] {
                {"", ""},
                {"     ", "     "},
                {"SELECT * FROM t WHERE a = '?'", "SELECT * FROM t WHERE a = '?'"},
                {"SELECT `v2?` FROM t WHERE `v1?` = ?", "SELECT `v2?` FROM t WHERE `v1?` = NULL"},
                {"INSERT INTO \"t2?\" VALUES (?, ?, 'some_?', ?)", "INSERT INTO \"t2?\" VALUES (NULL, NULL, 'some_?', NULL)"}
        };
    }

    @Test(groups = { "integration" })
    public void testJdbcEscapeSyntax() throws Exception {
        if (ClickHouseVersion.of(getServerVersion()).check("(,23.8]")) {
            return; // there is no `timestamp` function TODO: fix in JDBC
        }
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT {d '2021-11-01'} AS D, {ts '2021-08-01 12:34:56'} AS TS, " +
                    "toInt32({fn ABS(-1)}) AS FNABS, {fn CONCAT('Hello', 'World')} AS FNCONCAT, {fn UCASE('hello')} AS FNUPPER, " +
                    "{fn LCASE('HELLO')} AS FNLOWER, {fn LTRIM('  Hello  ')} AS FNLTRIM, {fn RTRIM('  Hello  ')} AS FNRTRIM, " +
                    "toInt32({fn LENGTH('Hello')}) AS FNLENGTH, toInt32({fn POSITION('Hello', 'l')}) AS FNPOSITION, toInt32({fn MOD(10, 3)}) AS FNMOD, " +
                    "{fn SQRT(9)} AS FNSQRT, {fn SUBSTRING('Hello', 3, 2)} AS FNSUBSTRING")) {
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getDate(1), Date.valueOf(LocalDate.of(2021, 11, 1)));
                    //assertEquals(rs.getTimestamp(2), java.sql.Timestamp.valueOf(LocalDateTime.of(2021, 11, 1, 12, 34, 56)));
                    assertEquals(rs.getInt(3), 1);
                    assertEquals(rs.getInt("FNABS"), 1);
                    assertEquals(rs.getString(4), "HelloWorld");
                    assertEquals(rs.getString("FNCONCAT"), "HelloWorld");
                    assertEquals(rs.getString(5), "HELLO");
                    assertEquals(rs.getString("FNUPPER"), "HELLO");
                    assertEquals(rs.getString(6), "hello");
                    assertEquals(rs.getString("FNLOWER"), "hello");
                    assertEquals(rs.getString(7), "Hello  ");
                    assertEquals(rs.getString("FNLTRIM"), "Hello  ");
                    assertEquals(rs.getString(8), "  Hello");
                    assertEquals(rs.getString("FNRTRIM"), "  Hello");
                    assertEquals(rs.getInt(9), 5);
                    assertEquals(rs.getInt("FNLENGTH"), 5);
                    assertEquals(rs.getInt(10), 3);
                    assertEquals(rs.getInt("FNPOSITION"), 3);
                    assertEquals(rs.getInt(11), 1);
                    assertEquals(rs.getInt("FNMOD"), 1);
                    assertEquals(rs.getDouble(12), 3);
                    assertEquals(rs.getDouble("FNSQRT"), 3);
                    assertEquals(rs.getString(13), "ll");
                    assertEquals(rs.getString("FNSUBSTRING"), "ll");
                    assertThrows(SQLException.class, () -> rs.getString(14));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = {"integration "})
    public void testStatementsWithDatabaseInTableIdentifier() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            final String db1Name = conn.getSchema() + "_db1";
            final String table1Name = "table1";
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE DATABASE IF NOT EXISTS " + db1Name);
                stmt.execute("DROP TABLE IF EXISTS " + db1Name + "." + table1Name);
                stmt.execute("CREATE TABLE " + db1Name + "." + table1Name +
                        "(v1 Int32, v2 Int32) Engine MergeTree ORDER BY ()");
            }

            String[] tableIdentifier = new String[]{
                    db1Name + "." + table1Name,
                    "`" + db1Name + "`.`" + table1Name + "`",
                    "\"" + db1Name + "\".\"" + table1Name + "\""
            };

            for (int i = 0; i < tableIdentifier.length; i++) {
                String tableId = tableIdentifier[i];
                final String insertStmt = "INSERT INTO " + tableId + " VALUES (?, ?)";
                try (PreparedStatement stmt = conn.prepareStatement(insertStmt)) {
                    stmt.setInt(1, i + 10);
                    stmt.setInt(2, i + 20);
                    assertEquals(stmt.executeUpdate(), 1);
                }
            }
        }
    }

    @Test(groups = {"integration "})
    public void testNullValues() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            final String table = "test_null_values";
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + table);
                stmt.execute("CREATE TABLE " + table +
                        "(v1 Int32, v2 Nullable(Int32)) Engine MergeTree ORDER BY ()");
            }

            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + table + " VALUES (?, ?)")) {
                stmt.setInt(1, 10);
                // do not set second value
                expectThrows(SQLException.class, stmt::executeUpdate);
                stmt.setInt(1, 20);
                stmt.setObject(2, null);
                assertEquals(stmt.executeUpdate(), 1);
            }

            try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {

                int count = 0;
                while(rs.next()) {
                    count++;
                    assertNull(rs.getObject(2));
                }

                assertEquals(count, 1);
            }
        }
    }

    @Test
    public void testParamWithCast() throws Exception {
        final String sql = " SELECT ?::integer, '?::integer', 123, ?:: UUID, ?";
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, "1000");
                UUID uuid = UUID.randomUUID();
                stmt.setString(1, "1000");
                stmt.setString(2, uuid.toString());
                stmt.setInt(3, 3003001);

                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    Assert.assertEquals(rs.getInt(1), 1000);
                    Assert.assertEquals(rs.getString(2), "?::integer");
                    Assert.assertEquals(rs.getInt(3), 123);
                    Assert.assertEquals(rs.getString(4), uuid.toString());
                    Assert.assertEquals(rs.getInt(5), 3003001);
                }
            }
        }
    }

    @Test(groups = {"integration"})
    public void testSelectWithTableAliasAsKeyword() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            String[] keywords = {
                    "ALL", "AND", "ANY", "AS", "ASC", "BY", "CREATE", "DATABASE", "DELETE", "DESC", "DISTINCT", "DROP", "EXISTS", "FROM", "GRANT", "GROUP", "HAVING", "INSERT", "INTO", "LIMIT", "NOT", "NULL", "ON", "ORDER", "REVOKE", "SELECT", "SET", "TABLE", "TO", "UPDATE", "VALUES", "VIEW", "WHILE", "WITH", "WHERE"
            };

            for (String keyword : keywords) {
                final String table = keyword;
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("DROP TABLE IF EXISTS " + table);
                    stmt.execute(
"CREATE TABLE " + table + " (v1 Int32, v2 String) Engine MergeTree ORDER BY ()");
                    stmt.execute("INSERT INTO `" + table + "` VALUES (1000, 'test')");
                }

                try (PreparedStatement stmt = conn.prepareStatement("SELECT v1, v2 FROM " + table + " AS " + keyword + " WHERE v1 = ? AND v2 = ?")) {
                    stmt.setInt(1, 1000);
                    stmt.setString(2, "test");
                    stmt.execute();
                    try (ResultSet rs = stmt.getResultSet()) {
                        assertTrue(rs.next());
                        Assert.assertEquals(rs.getInt(1), 1000);
                        Assert.assertEquals(rs.getString(2), "test");
                    }
                } catch (Exception e) {
                    fail("failed at keyword " + keyword, e);
                }
            }
        }
    }

    @Test(groups = {"integration"})
    public void testCTEWithUnboundCol() throws Exception {

        try (Connection conn = getJdbcConnection()) {
            String cte = "with ? as text, numz as (select text, number from system.numbers limit 10) select * from numz";
            try (PreparedStatement stmt = conn.prepareStatement(cte)) {
                stmt.setString(1, "1000");

                ResultSet rs = stmt.executeQuery();
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "1000");
                assertEquals(rs.getString(2), "0");
            }
        }
    }

    @Test(groups = {"integration"})
    public void testWithInClause() throws Exception {

        try (Connection conn = getJdbcConnection()) {
            final String q1 = "select number from system.numbers where number in (?) limit 10";
            try (PreparedStatement stmt = conn.prepareStatement(q1)) {
                Long[] filter =  new Long[]{2L, 4L, 6L};
                stmt.setArray(1, conn.createArrayOf("Int64", filter));
                ResultSet rs = stmt.executeQuery();

                for (Long filterValue : filter) {
                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), filterValue);
                }
                Assert.assertFalse(rs.next());
            }

            final String q2 = "with t as (select arrayJoin([1, 2, 3]) as a )  select * from t where a in(?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(q2)) {
                Long[] filter =  new Long[]{2L, 3L};

                stmt.setInt(1, 2);
                stmt.setInt(2, 3);
                ResultSet rs = stmt.executeQuery();

                for (Long filterValue : filter) {
                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), filterValue);
                }
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = {"integration"}, dataProvider = "testTypeCastsDP")
    public void testTypeCastsWithoutArgument(Object value, SQLType targetType, ClickHouseDataType expectedType) throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("select ?, toTypeName(?)")) {
               stmt.setObject(1, value, targetType);
               stmt.setObject(2, value, targetType);

               try (ResultSet rs = stmt.executeQuery()) {
                   rs.next();
                   assertEquals(rs.getString(2), expectedType.getName());
                   switch (expectedType) {
                       case IPv6:
                           // do not check because auto-converted to IPv4
                           break;
                       default:
                           assertEquals(rs.getString(1), String.valueOf(value));
                   }
               }
            }
        }
    }

    @DataProvider(name = "testTypeCastsDP")
    public static Object[][] testTypeCastsDP() {
        return new Object[][] {
                {100, ClickHouseDataType.Int8, ClickHouseDataType.Int8},
                {100L, ClickHouseDataType.Int16, ClickHouseDataType.Int16},
                {100L, ClickHouseDataType.Int32, ClickHouseDataType.Int32},
                {100L, ClickHouseDataType.Int64, ClickHouseDataType.Int64},
                 {100L, ClickHouseDataType.UInt8, ClickHouseDataType.UInt8},
                {100L, ClickHouseDataType.UInt16, ClickHouseDataType.UInt16},
                {100L, ClickHouseDataType.UInt32, ClickHouseDataType.UInt32},
                {100L, ClickHouseDataType.UInt64, ClickHouseDataType.UInt64},
                {"ed0c77a3-2e4b-4954-98ee-22a4fdad9565", ClickHouseDataType.UUID, ClickHouseDataType.UUID},
                {"0:0:0:0:0:ffff:5ab0:4b61", ClickHouseDataType.IPv6, ClickHouseDataType.IPv6},
                {"116.253.40.133", ClickHouseDataType.IPv4, ClickHouseDataType.IPv4},
                {100, JDBCType.TINYINT, ClickHouseDataType.Int8}
        };
    }

    @Test(groups = {"integration"}, dataProvider = "testJDBCTypeCastDP")
    public void testJDBCTypeCast(Object value, int targetType, ClickHouseDataType expectedType) throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("select ?, toTypeName(?)")) {
                stmt.setObject(1, value, targetType);
                stmt.setObject(2, value, targetType);

                try (ResultSet rs = stmt.executeQuery()) {
                    rs.next();
                    assertEquals(rs.getString(2), expectedType.getName());
                    switch (expectedType) {
                        case IPv4:
                            assertEquals(rs.getString(1), "/" + value);
                            break;
                        case IPv6:
                            // do not check
                            break;
                        default:
                            assertEquals(rs.getString(1), String.valueOf(value));
                    }
                }
            }
        }
    }

    @DataProvider(name = "testJDBCTypeCastDP")
    public static Object[][] testJDBCTypeCastDP() {
        return new Object[][] {
                {100, JDBCType.TINYINT.getVendorTypeNumber().intValue(), ClickHouseDataType.Int8}
        };
    }

    @Test(groups = {"integration"})
    public void testTypesInvalidForCast() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("select ?, toTypeName(?)")) {
                for (ClickHouseDataType type : JdbcUtils.INVALID_TARGET_TYPES) {
                    expectThrows(SQLException.class, ()->stmt.setObject(1, "", type));
                }

                expectThrows(SQLException.class, ()->stmt.setObject(1, "", JDBCType.OTHER.getVendorTypeNumber()));
                expectThrows(SQLException.class, ()->stmt.setObject(1, "", ClickHouseDataType.DateTime64));
            }
        }
    }

    @Test(groups = {"integration"}, dataProvider = "testTypeCastWithScaleOrLengthDP")
    public void testTypeCastWithScaleOrLength(Object value, SQLType targetType, Integer scaleOrLength, String expectedValue,
                                              String expectedType) throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("select ?, toTypeName(?)")) {
                stmt.setObject(1, value, targetType, scaleOrLength);
                stmt.setObject(2, value, targetType, scaleOrLength);
                try (ResultSet rs = stmt.executeQuery()) {
                    rs.next();
                    assertEquals(rs.getString(1), expectedValue);
                    assertEquals(rs.getString(2), expectedType);
                }
            }
        }
    }

    @DataProvider(name = "testTypeCastWithScaleOrLengthDP")
    public static Object[][] testTypeCastWithScaleOrLengthDP() {
        return new Object[][] {
                {0.123456789, ClickHouseDataType.Decimal64, 3, "0.123", "Decimal(18, 3)"},
                {"hello", ClickHouseDataType.FixedString, 5, "hello", "FixedString(5)"},
                {"2017-10-02 10:20:30.333333", ClickHouseDataType.DateTime64, 3, "2017-10-02 10:20:30.333", "DateTime64(3)"}
        };
    }

    @Test(groups = {"integration"})
    public void testCheckOfParametersAreSet() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("select ? as v1, ? as v2, ? as v3")) {
                stmt.setString(1, "Test");
                stmt.setObject(2, null);
                stmt.setString(3, null);

                try (ResultSet rs = stmt.executeQuery()) {
                    rs.next();
                    Assert.assertEquals(rs.getString(1), "Test");
                    Assert.assertEquals(rs.getInt(2), 0);
                    Assert.assertTrue(rs.wasNull());
                    Assert.assertNull(rs.getString(2));
                    Assert.assertTrue(rs.wasNull());
                }

                stmt.clearParameters();
                stmt.setString(1, "Test");
                stmt.setObject(2, null);
                Assert.expectThrows(SQLException.class, stmt::executeQuery);
            }

            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES (?, ?, ?)")) {
                stmt.setString(1, "Test");

                Assert.expectThrows(SQLException.class, stmt::executeUpdate);
            }
        }
    }

    @Test
    public void testParameterCount() throws Exception {
        try (Connection conn = getJdbcConnection();) {
            try (PreparedStatement stmt = conn.prepareStatement("select ?, ? as v1, ? as v2")) {
                Assert.assertEquals(stmt.getMetaData().getColumnCount(), 3);
            }
            try (PreparedStatement stmt = conn.prepareStatement("WITH toDateTime(?) AS target_time SELECT * FROM table")) {
                Assert.assertEquals(stmt.getMetaData().getColumnCount(), 1);
            }
        }
    }

    @Test
    public void testEncodingArray() throws Exception {
        try (Connection conn = getJdbcConnection();) {
            try (PreparedStatementImpl stmt = (PreparedStatementImpl) conn.prepareStatement("SELECT ?")) {

                {
                    Object[] array1 = new Object[]{1, 2, 3};
                    ClickHouseColumn col1 = ClickHouseColumn.of("v", "Array(Int8)");
                    assertEquals(stmt.encodeArray(array1, col1.getArrayNestedLevel(), col1.getArrayBaseColumn().getDataType()),
                            "[1,2,3]");
                }                {
                    Object[] array1 = new Object[]{1, 2, 3};
                    Object[] array2 = new Object[]{4, 5, 6};
                    Object[] array3 = new Object[]{array1, array2};
                    ClickHouseColumn col1 = ClickHouseColumn.of("v", "Array(Array(Int8))");
                    assertEquals(stmt.encodeArray(array3, col1.getArrayNestedLevel(), col1.getArrayBaseColumn().getDataType()),
                            "[[1,2,3],[4,5,6]]");
                }
                {
                    Object[] array1 = new Object[]{1, 2, 3};
                    Object[] array2 = new Object[]{4, null, 6};
                    Object[] array3 = new Object[]{null, array1, array2};
                    ClickHouseColumn col1 = ClickHouseColumn.of("v", "Array(Array(Int8))");
                    assertEquals(stmt.encodeArray(array3, col1.getArrayNestedLevel(), col1.getArrayBaseColumn().getDataType()),
                            "[[],[1,2,3],[4,NULL,6]]");
                }
                {
                    Object[] array1 = new Object[]{1, 2, 3};
                    Object[] array2 = new Object[]{4, null, 6};
                    Object[] array3 = new Object[]{null, array1, array2};
                    Object[] array4 = new Object[]{7, null, 9};
                    Object[] array5 = new Object[]{10, null, 12};
                    Object[] array6 = new Object[]{null, array4, array5};

                    Object[] array7 = new Object[]{null, array3, array6};
                    ClickHouseColumn col1 = ClickHouseColumn.of("v", "Array(Array(Array(Int8)))");
                    assertEquals(stmt.encodeArray(array7, col1.getArrayNestedLevel(), col1.getArrayBaseColumn().getDataType()),
                            "[[],[[],[1,2,3],[4,NULL,6]],[[],[7,NULL,9],[10,NULL,12]]]");
                }


                {
                    Object[] array1 = new Object[]{1, 2, 3};
                    Object[] array2 = new Object[]{4, 5, 6};
                    Object[] array3 = new Object[]{array1, array2};
                    ClickHouseColumn col1 = ClickHouseColumn.of("v", "Array(Tuple(Int8, Int8, Int8))");
                    assertEquals(stmt.encodeArray(array3, col1.getArrayNestedLevel(), col1.getArrayBaseColumn().getDataType()),
                            "[(1,2,3),(4,5,6)]");
                }

                {
                    Object[] array1 = new Object[]{1, 2, 3};
                    Object[] array2 = new Object[]{4, 5, 6};
                    Object[] array3 = new Object[]{null, array1, array2, new Object[0]};
                    Object[] array4 = new Object[]{7, 8, 9};
                    Object[] array5 = new Object[]{10, 11, 12};
                    Object[] array6 = new Object[]{null, array4, array5};

                    Object[] array7 = new Object[]{null, array3, array6, new Object[0]};
                    ClickHouseColumn col1 = ClickHouseColumn.of("v", "Array(Array(Tuple(Int8, Int8, Int8)))");
                    assertEquals(stmt.encodeArray(array7, col1.getArrayNestedLevel(), col1.getArrayBaseColumn().getDataType()),
                            "[[],[NULL,(1,2,3),(4,5,6),()],[NULL,(7,8,9),(10,11,12)],[]]");
                }
            }
        }
    }
}
