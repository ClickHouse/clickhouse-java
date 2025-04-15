package com.clickhouse.jdbc;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.sql.Array;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


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

    @Ignore("Not supported yet")
    @Test(groups = { "integration" })
    public void testSetBytes() throws Exception {
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


    @Test(groups = "integration")
    void testWithClause() throws Exception {
        int count = 0;
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("with data as (SELECT number FROM numbers(100)) select * from data ")) {
                stmt.execute();
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    count++;
                }
            }
        }
        assertEquals(count, 100);
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

            try (Statement stmt01 = conn.createStatement()) {
                try (ResultSet rs = stmt01.executeQuery("SELECT count(*) FROM `users_tmp`")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), 3);
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
                stmt.execute("CREATE TABLE `with_complex_id` (`v?``1` Int32, \"v?\"\"2\" Int32,`v?\\`3` Int32, \"v?\\\"4\" Int32) ENGINE Memory;");
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
        }
    }
}
