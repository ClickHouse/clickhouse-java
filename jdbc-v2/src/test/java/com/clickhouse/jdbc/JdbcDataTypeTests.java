package com.clickhouse.jdbc;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.DataTypeUtils;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.sql.SQLUtils;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseVersion;
import com.clickhouse.data.Tuple;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test(groups = { "integration" })
public class JdbcDataTypeTests extends JdbcIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(JdbcDataTypeTests.class);

    @BeforeClass(groups = { "integration" })
    public static void setUp() throws SQLException {
        Driver.load();
    }

    private int insertData(String sql) throws SQLException {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                return stmt.executeUpdate(sql);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testIntegerTypes() throws SQLException {
        runQuery("CREATE TABLE test_integers (order Int8, "
                + "int8 Int8, int16 Int16, int32 Int32, int64 Int64, int128 Int128, int256 Int256, "
                + "uint8 UInt8, uint16 UInt16, uint32 UInt32, uint64 UInt64, uint128 UInt128, uint256 UInt256"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert minimum values
        insertData("INSERT INTO test_integers VALUES ( 1, "
                + "-128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728, -57896044618658097711785492504343953926634992332820282019728792003956564819968, "
                + "0, 0, 0, 0, 0, 0"
                + ")");

        // Insert maximum values
        insertData("INSERT INTO test_integers VALUES ( 2, "
                + "127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727, 57896044618658097711785492504343953926634992332820282019728792003956564819967, "
                + "255, 65535, 4294967295, 18446744073709551615, 340282366920938463463374607431768211455, 115792089237316195423570985008687907853269984665640564039457584007913129639935"
                + ")");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        int int8 = rand.nextInt(256) - 128;
        int int16 = rand.nextInt(65536) - 32768;
        int int32 = rand.nextInt();
        long int64 = rand.nextLong();
        BigInteger int128 = new BigInteger(127, rand);
        BigInteger int256 = new BigInteger(255, rand);
        Short uint8 = Integer.valueOf(rand.nextInt(256)).shortValue();
        int uint16 = rand.nextInt(65536);
        long uint32 = rand.nextInt() & 0xFFFFFFFFL;
        BigInteger uint64 = BigInteger.valueOf(rand.nextLong(Long.MAX_VALUE));
        BigInteger uint128 = new BigInteger(128, rand);
        BigInteger uint256 = new BigInteger(256, rand);

        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_integers VALUES ( 3, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                stmt.setInt(1, int8);
                stmt.setInt(2, int16);
                stmt.setInt(3, int32);
                stmt.setLong(4, int64);
                stmt.setBigDecimal(5, new BigDecimal(int128));
                stmt.setBigDecimal(6, new BigDecimal(int256));
                stmt.setInt(7, uint8);
                stmt.setInt(8, uint16);
                stmt.setLong(9, uint32);
                stmt.setBigDecimal(10, new BigDecimal(uint64));
                stmt.setBigDecimal(11, new BigDecimal(uint128));
                stmt.setBigDecimal(12, new BigDecimal(uint256));
                stmt.executeUpdate();
            }
        }

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_integers ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getByte("int8"), Byte.MIN_VALUE);
                    assertEquals(rs.getShort("int16"), Short.MIN_VALUE);
                    assertEquals(rs.getInt("int32"), Integer.MIN_VALUE);
                    assertEquals(rs.getLong("int64"), Long.MIN_VALUE);
                    assertEquals(rs.getBigDecimal("int128"), new BigDecimal("-170141183460469231731687303715884105728"));
                    assertEquals(rs.getBigDecimal("int256"), new BigDecimal("-57896044618658097711785492504343953926634992332820282019728792003956564819968"));
                    assertEquals(rs.getShort("uint8"), 0);
                    assertEquals(rs.getInt("uint16"), 0);
                    assertEquals(rs.getLong("uint32"), 0);
                    assertEquals(rs.getBigDecimal("uint64"), new BigDecimal("0"));
                    assertEquals(rs.getBigDecimal("uint128"), new BigDecimal("0"));
                    assertEquals(rs.getBigDecimal("uint256"), new BigDecimal("0"));

                    assertTrue(rs.next());
                    assertEquals(rs.getByte("int8"), Byte.MAX_VALUE);
                    assertEquals(rs.getShort("int16"), Short.MAX_VALUE);
                    assertEquals(rs.getInt("int32"), Integer.MAX_VALUE);
                    assertEquals(rs.getLong("int64"), Long.MAX_VALUE);
                    assertEquals(rs.getBigDecimal("int128"), new BigDecimal("170141183460469231731687303715884105727"));
                    assertEquals(rs.getBigDecimal("int256"), new BigDecimal("57896044618658097711785492504343953926634992332820282019728792003956564819967"));
                    assertEquals(rs.getShort("uint8"), 255);
                    assertEquals(rs.getInt("uint16"), 65535);
                    assertEquals(rs.getLong("uint32"), 4294967295L);
                    assertEquals(rs.getBigDecimal("uint64"), new BigDecimal("18446744073709551615"));
                    assertEquals(rs.getBigDecimal("uint128"), new BigDecimal("340282366920938463463374607431768211455"));
                    assertEquals(rs.getBigDecimal("uint256"), new BigDecimal("115792089237316195423570985008687907853269984665640564039457584007913129639935"));

                    assertTrue(rs.next());
                    assertEquals(rs.getByte("int8"), int8);
                    assertEquals(rs.getShort("int16"), int16);
                    assertEquals(rs.getInt("int32"), int32);
                    assertEquals(rs.getLong("int64"), int64);
                    assertEquals(rs.getBigDecimal("int128"), new BigDecimal(int128));
                    assertEquals(rs.getBigDecimal("int256"), new BigDecimal(int256));
                    assertEquals(rs.getShort("uint8"), uint8);
                    assertEquals(rs.getInt("uint16"), uint16);
                    assertEquals(rs.getLong("uint32"), uint32);
                    assertEquals(rs.getBigDecimal("uint64"), new BigDecimal(uint64));
                    assertEquals(rs.getBigDecimal("uint128"), new BigDecimal(uint128));
                    assertEquals(rs.getBigDecimal("uint256"), new BigDecimal(uint256));

                    assertFalse(rs.next());
                }
            }
        }

        // Check the with getObject
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_integers ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getObject("int8"), Byte.MIN_VALUE);
                    assertEquals(rs.getObject("int16"), Short.MIN_VALUE);
                    assertEquals(rs.getObject("int32"), Integer.MIN_VALUE);
                    assertEquals(rs.getObject("int64"), Long.MIN_VALUE);
                    assertEquals(rs.getObject("int128"), new BigInteger("-170141183460469231731687303715884105728"));
                    assertEquals(rs.getObject("int256"), new BigInteger("-57896044618658097711785492504343953926634992332820282019728792003956564819968"));
                    assertEquals(rs.getObject("uint8"), Short.valueOf("0"));
                    assertEquals(rs.getObject("uint16"), 0);
                    assertEquals(rs.getObject("uint32"), 0L);
                    assertEquals(rs.getObject("uint64"), new BigInteger("0"));
                    assertEquals(rs.getObject("uint128"), new BigInteger("0"));
                    assertEquals(rs.getObject("uint256"), new BigInteger("0"));

                    assertTrue(rs.next());
                    assertEquals(rs.getObject("int8"), Byte.MAX_VALUE);
                    assertEquals(rs.getObject("int16"), Short.MAX_VALUE);
                    assertEquals(rs.getObject("int32"), Integer.MAX_VALUE);
                    assertEquals(rs.getObject("int64"), Long.MAX_VALUE);
                    assertEquals(rs.getObject("int128"), new BigInteger("170141183460469231731687303715884105727"));
                    assertEquals(rs.getObject("int256"), new BigInteger("57896044618658097711785492504343953926634992332820282019728792003956564819967"));
                    assertEquals(rs.getObject("uint8"), Short.valueOf("255"));
                    assertEquals(rs.getObject("uint16"), 65535);
                    assertEquals(rs.getObject("uint32"), 4294967295L);
                    assertEquals(rs.getObject("uint64"), new BigInteger("18446744073709551615"));
                    assertEquals(rs.getObject("uint128"), new BigInteger("340282366920938463463374607431768211455"));
                    assertEquals(rs.getObject("uint256"), new BigInteger("115792089237316195423570985008687907853269984665640564039457584007913129639935"));

                    assertTrue(rs.next());
                    assertEquals(rs.getObject("int8"), (byte)int8);
                    assertEquals(rs.getObject("int16"), (short)int16);
                    assertEquals(rs.getObject("int32"), int32);
                    assertEquals(rs.getObject("int64"), int64);
                    assertEquals(rs.getObject("int128"), int128);
                    assertEquals(rs.getObject("int256"), int256);
                    assertEquals(rs.getObject("uint8"), uint8);
                    assertEquals(rs.getObject("uint16"), uint16);
                    assertEquals(rs.getObject("uint32"), uint32);
                    assertEquals(rs.getObject("uint64"), uint64);
                    assertEquals(rs.getObject("uint128"), uint128);
                    assertEquals(rs.getObject("uint256"), uint256);

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testUnsignedIntegerTypes() throws Exception {
        Random rand = new Random();
        runQuery("CREATE TABLE test_unsigned_integers (order Int8, "
                + "uint8 Nullable(UInt8), "
                + "uint16 Nullable(UInt16), "
                + "uint32 Nullable(UInt32), "
                + "uint64 Nullable(UInt64), "
                + "uint128 Nullable(UInt128), "
                + "uint256 Nullable(UInt256)"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert null values
        insertData("INSERT INTO test_unsigned_integers VALUES ( 1, "
                + "NULL, NULL, NULL, NULL, NULL, NULL)");

        // Insert minimum values
        insertData("INSERT INTO test_unsigned_integers VALUES ( 2, "
                + "0, 0, 0, 0, 0, 0)");

        // Insert random values
        int uint8 = rand.nextInt(256);
        int uint16 = rand.nextInt(65536);
        long uint32 = rand.nextLong() & 0xFFFFFFFFL;
        long uint64 = rand.nextLong() & 0xFFFFFFFFFFFFL;
        BigInteger uint128 = new BigInteger(38, rand);
        BigInteger uint256 = new BigInteger(77, rand);
        insertData("INSERT INTO test_unsigned_integers VALUES ( 3, "
                + uint8 + ", " + uint16 + ", " + uint32 + ", " + uint64 + ", " + uint128 + ", " + uint256 + ")");

        try (Connection conn = getJdbcConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT uint8, uint16, uint32, uint64, uint128, uint256 FROM test_unsigned_integers ORDER BY order")) {

            List<Class<?>> expectedTypes = Arrays.asList(
                    Short.class, Integer.class, Long.class, BigInteger.class, BigInteger.class, BigInteger.class);
            List<Class<?>> actualTypes = new ArrayList<>();
            ResultSetMetaData rsmd = rs.getMetaData();
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                actualTypes.add(Class.forName(rsmd.getColumnClassName(i + 1)));
            }
            assertEquals(actualTypes, expectedTypes);


            assertTrue(rs.next());
            assertEquals(rs.getObject("uint8"), null);
            assertEquals(rs.getObject("uint16"), null);
            assertEquals(rs.getObject("uint32"), null);
            assertEquals(rs.getObject("uint64"), null);
            assertEquals(rs.getObject("uint128"), null);
            assertEquals(rs.getObject("uint256"), null);

            assertTrue(rs.next());
            assertEquals((Short) rs.getObject("uint8"), (byte) 0);
            assertEquals((Integer) rs.getObject("uint16"), (short) 0);
            assertEquals((Long) rs.getObject("uint32"), 0);
            assertEquals(rs.getObject("uint64"), BigInteger.ZERO);
            assertEquals(rs.getObject("uint128"), BigInteger.ZERO);
            assertEquals(rs.getObject("uint256"), BigInteger.ZERO);

            assertTrue(rs.next());
            assertEquals(((Short) rs.getObject("uint8")).intValue(), uint8);
            assertEquals((Integer) rs.getObject("uint16"), uint16);
            assertEquals((Long) rs.getObject("uint32"), uint32);
            assertEquals(rs.getObject("uint64"), BigInteger.valueOf(uint64));
            assertEquals(rs.getObject("uint128"), uint128);
            assertEquals(rs.getObject("uint256"), uint256);

            assertFalse(rs.next());
        }
    }


    @Test(groups = { "integration" })
    public void testUUIDTypes() throws Exception {
        Random rand = new Random();
        runQuery("CREATE TABLE test_uuids (order Int8, "
                + "uuid Nullable(UUID) "
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert null values
        insertData("INSERT INTO test_uuids VALUES ( 1, NULL)");

        // Insert random values
        UUID uuid = UUID.randomUUID();
        insertData("INSERT INTO test_uuids VALUES ( 2, "
                + "'" + uuid + "')");

        try (Connection conn = getJdbcConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT uuid  FROM test_uuids ORDER BY order")) {

            assertTrue(rs.next());
            assertNull(rs.getObject("uuid"));


            assertTrue(rs.next());
            assertEquals(rs.getObject("uuid"), uuid);

            assertFalse(rs.next());
        }
    }


    @Test(groups = {"integration"})
    public void testArrayOfUUID() throws Exception {
        try (Connection connection = getJdbcConnection();
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT '2d1f626d-eb07-4c81-be3d-ac1173f0d018'::UUID f_elem, ['2d1f626d-eb07-4c81-be3d-ac1173f0d018']::Array(UUID) arr")) {

            Assert.assertTrue(rs.next());
            UUID fElem = (UUID) rs.getObject(1);
            Array colValue = rs.getArray(2);
            Object[] arr = (Object[]) colValue.getArray();
            Assert.assertEquals(fElem, arr[0]);

            ResultSet arrRs = colValue.getResultSet();
            arrRs.next();
            Assert.assertEquals(fElem, arrRs.getObject(2));
        }
    }

    @Test(groups = { "integration" })
    public void testDecimalTypes() throws SQLException {
        runQuery("CREATE TABLE test_decimals (order Int8, "
                + "dec Decimal(9, 2), dec32 Decimal32(4), dec64 Decimal64(8), dec128 Decimal128(18), dec256 Decimal256(18)"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert minimum values
        insertData("INSERT INTO test_decimals VALUES ( 1, -9999999.99, -99999.9999, -9999999999.99999999, -99999999999999999999.999999999999999999, " +
                "-9999999999999999999999999999999999999999999999999999999999.999999999999999999)");

        // Insert maximum values
        insertData("INSERT INTO test_decimals VALUES ( 2, 9999999.99, 99999.9999, 9999999999.99999999, 99999999999999999999.999999999999999999, " +
                "9999999999999999999999999999999999999999999999999999999999.999999999999999999)");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        BigDecimal dec = new BigDecimal(new BigInteger(7, rand) + "." + rand.nextInt(10,100));//P - S; 9 - 2
        BigDecimal dec32 = new BigDecimal(new BigInteger(5, rand) + "." + rand.nextInt(1000, 10000));
        BigDecimal dec64 = new BigDecimal(new BigInteger(18, rand) + "." + rand.nextInt(10000000, 100000000));
        BigDecimal dec128 = new BigDecimal(new BigInteger(20, rand) + "." + rand.nextLong(100000000000000000L, 1000000000000000000L));
        BigDecimal dec256 = new BigDecimal(new BigInteger(58, rand) + "." + rand.nextLong(100000000000000000L, 1000000000000000000L));

        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_decimals VALUES ( 3, ?, ?, ?, ?, ?)")) {
                stmt.setBigDecimal(1, dec);
                stmt.setBigDecimal(2, dec32);
                stmt.setBigDecimal(3, dec64);
                stmt.setBigDecimal(4, dec128);
                stmt.setBigDecimal(5, dec256);
                stmt.executeUpdate();
            }
        }


        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_decimals ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getBigDecimal("dec"), new BigDecimal("-9999999.99"));
                    assertEquals(rs.getBigDecimal("dec32"), new BigDecimal("-99999.9999"));
                    assertEquals(rs.getBigDecimal("dec64"), new BigDecimal("-9999999999.99999999"));
                    assertEquals(rs.getBigDecimal("dec128"), new BigDecimal("-99999999999999999999.999999999999999999"));
                    assertEquals(rs.getBigDecimal("dec256"), new BigDecimal("-9999999999999999999999999999999999999999999999999999999999.999999999999999999"));

                    assertTrue(rs.next());
                    assertEquals(rs.getBigDecimal("dec"), new BigDecimal("9999999.99"));
                    assertEquals(rs.getBigDecimal("dec32"), new BigDecimal("99999.9999"));
                    assertEquals(rs.getBigDecimal("dec64"), new BigDecimal("9999999999.99999999"));
                    assertEquals(rs.getBigDecimal("dec128"), new BigDecimal("99999999999999999999.999999999999999999"));
                    assertEquals(rs.getBigDecimal("dec256"), new BigDecimal("9999999999999999999999999999999999999999999999999999999999.999999999999999999"));

                    assertTrue(rs.next());
                    assertEquals(rs.getBigDecimal("dec"), dec);
                    assertEquals(rs.getBigDecimal("dec32"), dec32);
                    assertEquals(rs.getBigDecimal("dec64"), dec64);
                    assertEquals(rs.getBigDecimal("dec128"), dec128);
                    assertEquals(rs.getBigDecimal("dec256"), dec256);

                    assertFalse(rs.next());
                }
            }
        }

        // Check the results with getObject
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_decimals ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getObject("dec"), new BigDecimal("-9999999.99"));
                    assertEquals(rs.getObject("dec32"), new BigDecimal("-99999.9999"));
                    assertEquals(rs.getObject("dec64"), new BigDecimal("-9999999999.99999999"));
                    assertEquals(rs.getObject("dec128"), new BigDecimal("-99999999999999999999.999999999999999999"));
                    assertEquals(rs.getObject("dec256"), new BigDecimal("-9999999999999999999999999999999999999999999999999999999999.999999999999999999"));

                    assertTrue(rs.next());
                    assertEquals(rs.getObject("dec"), new BigDecimal("9999999.99"));
                    assertEquals(rs.getObject("dec32"), new BigDecimal("99999.9999"));
                    assertEquals(rs.getObject("dec64"), new BigDecimal("9999999999.99999999"));
                    assertEquals(rs.getObject("dec128"), new BigDecimal("99999999999999999999.999999999999999999"));
                    assertEquals(rs.getObject("dec256"), new BigDecimal("9999999999999999999999999999999999999999999999999999999999.999999999999999999"));

                    assertTrue(rs.next());
                    assertEquals(rs.getObject("dec"), dec);
                    assertEquals(rs.getObject("dec32"), dec32);
                    assertEquals(rs.getObject("dec64"), dec64);
                    assertEquals(rs.getObject("dec128"), dec128);
                    assertEquals(rs.getObject("dec256"), dec256);

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testDateTimeTypes() throws SQLException {
        runQuery("CREATE TABLE test_datetimes (order Int8, " +
                "dateTime DateTime, dateTime32 DateTime32, " +
                "dateTime643 DateTime64(3), dateTime646 DateTime64(6), dateTime649 DateTime64(9)"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert minimum values
        insertData("INSERT INTO test_datetimes VALUES ( 1, " +
                "'1970-01-01 00:00:00', '1970-01-01 00:00:00', " +
                "'1970-01-01 00:00:00.000', '1970-01-01 00:00:00.000000', '1970-01-01 00:00:00.000000000' )");

        // Insert maximum values
        insertData("INSERT INTO test_datetimes VALUES ( 2," +
                "'2106-02-07 06:28:15', '2106-02-07 06:28:15', " +
                "'2261-12-31 23:59:59.999', '2261-12-31 23:59:59.999999', '2261-12-31 23:59:59.999999999' )");

        // Insert random (valid) values
        final ZoneId zoneId = ZoneId.of("America/Los_Angeles");
        final LocalDateTime now = LocalDateTime.now(zoneId);
        final java.sql.Timestamp dateTime = Timestamp.valueOf(now);
        dateTime.setNanos(0);
        final java.sql.Timestamp dateTime32 = Timestamp.valueOf(now);
        dateTime32.setNanos(0);
        final java.sql.Timestamp dateTime643 = Timestamp.valueOf(LocalDateTime.now(ZoneId.of("America/Los_Angeles")));
        dateTime643.setNanos(333000000);
        final java.sql.Timestamp dateTime646 = Timestamp.valueOf(LocalDateTime.now(ZoneId.of("America/Los_Angeles")));
        dateTime646.setNanos(333333000);
        final java.sql.Timestamp dateTime649 = Timestamp.valueOf(LocalDateTime.now(ZoneId.of("America/Los_Angeles")));
        dateTime649.setNanos(333333333);

        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_datetimes VALUES ( 4, ?, ?, ?, ?, ?)")) {
                stmt.setTimestamp(1, dateTime);
                stmt.setTimestamp(2, dateTime32);
                stmt.setTimestamp(3, dateTime643);
                stmt.setTimestamp(4, dateTime646);
                stmt.setTimestamp(5, dateTime649);
                stmt.executeUpdate();
            }
        }

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_datetimes ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getTimestamp("dateTime").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getTimestamp("dateTime32").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getTimestamp("dateTime643").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getTimestamp("dateTime646").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getTimestamp("dateTime649").toString(), "1970-01-01 00:00:00.0");

                    assertTrue(rs.next());
                    assertEquals(rs.getTimestamp("dateTime").toString(), "2106-02-07 06:28:15.0");
                    assertEquals(rs.getTimestamp("dateTime32").toString(), "2106-02-07 06:28:15.0");
                    assertEquals(rs.getTimestamp("dateTime643").toString(), "2261-12-31 23:59:59.999");
                    assertEquals(rs.getTimestamp("dateTime646").toString(), "2261-12-31 23:59:59.999999");
                    assertEquals(rs.getTimestamp("dateTime649").toString(), "2261-12-31 23:59:59.999999999");

                    assertTrue(rs.next());
                    assertEquals(rs.getTimestamp("dateTime").toString(), Timestamp.valueOf(dateTime.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getTimestamp("dateTime32").toString(), Timestamp.valueOf(dateTime32.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getTimestamp("dateTime643").toString(), Timestamp.valueOf(dateTime643.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getTimestamp("dateTime646").toString(), Timestamp.valueOf(dateTime646.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getTimestamp("dateTime649").toString(), Timestamp.valueOf(dateTime649.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());

                    assertEquals(rs.getTimestamp("dateTime", new GregorianCalendar(TimeZone.getTimeZone("UTC"))).toString(), dateTime.toString());
                    assertEquals(rs.getTimestamp("dateTime32", new GregorianCalendar(TimeZone.getTimeZone("UTC"))).toString(), dateTime32.toString());
                    assertEquals(rs.getTimestamp("dateTime643", new GregorianCalendar(TimeZone.getTimeZone("UTC"))).toString(), dateTime643.toString());
                    assertEquals(rs.getTimestamp("dateTime646", new GregorianCalendar(TimeZone.getTimeZone("UTC"))).toString(), dateTime646.toString());
                    assertEquals(rs.getTimestamp("dateTime649", new GregorianCalendar(TimeZone.getTimeZone("UTC"))).toString(), dateTime649.toString());

                    assertFalse(rs.next());
                }
            }
        }

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_datetimes ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getObject("dateTime").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getObject("dateTime32").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getObject("dateTime643").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getObject("dateTime646").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getObject("dateTime649").toString(), "1970-01-01 00:00:00.0");

                    assertTrue(rs.next());

                    assertEquals(rs.getObject("dateTime").toString(), "2106-02-07 06:28:15.0");
                    assertEquals(rs.getObject("dateTime32").toString(), "2106-02-07 06:28:15.0");
                    assertEquals(rs.getObject("dateTime643").toString(), "2261-12-31 23:59:59.999");
                    assertEquals(rs.getObject("dateTime646").toString(), "2261-12-31 23:59:59.999999");
                    assertEquals(rs.getObject("dateTime649").toString(), "2261-12-31 23:59:59.999999999");

                    assertTrue(rs.next());

                    assertEquals(rs.getObject("dateTime").toString(), Timestamp.valueOf(dateTime.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getObject("dateTime32").toString(), Timestamp.valueOf(dateTime32.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getObject("dateTime643").toString(), Timestamp.valueOf(dateTime643.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getObject("dateTime646").toString(), Timestamp.valueOf(dateTime646.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getObject("dateTime649").toString(), Timestamp.valueOf(dateTime649.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());

                    assertFalse(rs.next());
                }
            }
        }

        try (Connection conn = getJdbcConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM test_datetimes ORDER BY order"))
        {
            assertTrue(rs.next());

            assertEquals(rs.getString("dateTime"), "1970-01-01 00:00:00");
            assertEquals(rs.getString("dateTime32"), "1970-01-01 00:00:00");
            assertEquals(rs.getString("dateTime643"), "1970-01-01 00:00:00");
            assertEquals(rs.getString("dateTime646"), "1970-01-01 00:00:00");
            assertEquals(rs.getString("dateTime649"), "1970-01-01 00:00:00");

            assertTrue(rs.next());
            assertEquals(rs.getString("dateTime"), "2106-02-07 06:28:15");
            assertEquals(rs.getString("dateTime32"), "2106-02-07 06:28:15");
            assertEquals(rs.getString("dateTime643"), "2261-12-31 23:59:59.999");
            assertEquals(rs.getString("dateTime646"), "2261-12-31 23:59:59.999999");
            assertEquals(rs.getString("dateTime649"), "2261-12-31 23:59:59.999999999");

            ZoneId tzServer = ZoneId.of(((ConnectionImpl) conn).getClient().getServerTimeZone());
            assertTrue(rs.next());
            assertEquals(
                rs.getString("dateTime"),
                DataTypeUtils.DATETIME_FORMATTER.format(
                    Instant.ofEpochMilli(dateTime.getTime()).atZone(tzServer)));
            assertEquals(
                rs.getString("dateTime32"),
                DataTypeUtils.DATETIME_FORMATTER.format(
                    Instant.ofEpochMilli(dateTime32.getTime()).atZone(tzServer)));
            assertEquals(
                rs.getString("dateTime643"),
                DataTypeUtils.DATETIME_WITH_NANOS_FORMATTER.format(dateTime643.toInstant().atZone(tzServer)));
            assertEquals(
                rs.getString("dateTime646"),
                    DataTypeUtils.DATETIME_WITH_NANOS_FORMATTER.format(dateTime646.toInstant().atZone(tzServer)));
            assertEquals(
                rs.getString("dateTime649"),
                DataTypeUtils.DATETIME_WITH_NANOS_FORMATTER.format(dateTime649.toInstant().atZone(tzServer)));

            assertFalse(rs.next());
        }
    }

    @Test(groups = { "integration" })
    public void testDateTypes() throws SQLException {
        runQuery("CREATE TABLE test_dates (order Int8, "
                + "date Date, date32 Date32"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert minimum values
        insertData("INSERT INTO test_dates VALUES ( 1, '1970-01-01', '1970-01-01')");

        // Insert maximum values
        insertData("INSERT INTO test_dates VALUES ( 2, '2149-06-06', '2299-12-31')");

        // Insert random (valid) values
        final ZoneId zoneId = ZoneId.of("America/Los_Angeles");
        final LocalDateTime now = LocalDateTime.now(zoneId);
        final Date date = Date.valueOf(now.toLocalDate());
        final Date date32 = Date.valueOf(now.toLocalDate());

        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_dates VALUES ( 3, ?, ?)")) {
                stmt.setDate(1, date);
                stmt.setDate(2, date32);
                stmt.executeUpdate();
            }
        }

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_dates ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getDate("date"), Date.valueOf("1970-01-01"));
                    assertEquals(rs.getDate("date32"), Date.valueOf("1970-01-01"));

                    assertTrue(rs.next());
                    assertEquals(rs.getDate("date"), Date.valueOf("2149-06-06"));
                    assertEquals(rs.getDate("date32"), Date.valueOf("2299-12-31"));

                    assertTrue(rs.next());
                    assertEquals(rs.getDate("date").toString(), date.toString());
                    assertEquals(rs.getDate("date32").toString(), date32.toString());

                    assertFalse(rs.next());
                }
            }
        }

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_dates ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getObject("date"), Date.valueOf("1970-01-01"));
                    assertEquals(rs.getObject("date32"), Date.valueOf("1970-01-01"));

                    assertTrue(rs.next());
                    assertEquals(rs.getObject("date"), Date.valueOf("2149-06-06"));
                    assertEquals(rs.getObject("date32"), Date.valueOf("2299-12-31"));

                    assertTrue(rs.next());
                    assertEquals(rs.getObject("date").toString(), date.toString());
                    assertEquals(rs.getObject("date32").toString(), date32.toString());

                    assertFalse(rs.next());
                }
            }
        }

        try (Connection conn = getJdbcConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM test_dates ORDER BY order"))
        {
            assertTrue(rs.next());
            assertEquals(rs.getString("date"), "1970-01-01");
            assertEquals(rs.getString("date32"), "1970-01-01");

            assertTrue(rs.next());
            assertEquals(rs.getString("date"), "2149-06-06");
            assertEquals(rs.getString("date32"), "2299-12-31");

            ZoneId tzServer = ZoneId.of(((ConnectionImpl) conn).getClient().getServerTimeZone());
            assertTrue(rs.next());
            assertEquals(
                    rs.getString("date"),
                    Instant.ofEpochMilli(date.getTime()).atZone(tzServer).toLocalDate().toString());
            assertEquals(
                    rs.getString("date32"),
                    Instant.ofEpochMilli(date32.getTime()).atZone(tzServer).toLocalDate().toString());

            assertFalse(rs.next());
        }
    }


    @Test(groups = { "integration" })
    public void testTimeTypes() throws SQLException {
        if (ClickHouseVersion.of(getServerVersion()).check("(,25.5]")) {
            return; // Time64 introduced in 25.6
        }
        Properties createProperties = new Properties();
        createProperties.put(ClientConfigProperties.serverSetting("allow_experimental_time_time64_type"), "1");
        runQuery("CREATE TABLE test_time64 (order Int8, "
                + "time Time, time64 Time64(9) "
                + ") ENGINE = MergeTree ORDER BY ()",
                createProperties);

        runQuery("INSERT INTO test_time64 (order, time, time64) VALUES " +
                "   (1, '-999:59:59', '-999:59:59.999999999'), " +
                "   (2, '999:59:59', '999:59:59.999999999')");

        // Check the results
        try (Statement stmt = getJdbcConnection().createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_time64")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt("order"), 1);

                // Negative values
                // Negative value cannot be returned as Time without being truncated
                assertTrue(rs.getTime("time").getTime() < 0);
                assertTrue(rs.getTime("time64").getTime() < 0);
                LocalDateTime negativeTime = rs.getObject("time", LocalDateTime.class);
                assertEquals(negativeTime.toEpochSecond(ZoneOffset.UTC), -(TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59));
                LocalDateTime negativeTime64 = rs.getObject("time64", LocalDateTime.class);
                assertEquals(negativeTime64.toEpochSecond(ZoneOffset.UTC), -(TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59), "value " + negativeTime64);
                assertEquals(negativeTime64.getNano(), 999_999_999); // nanoseconds are stored separately and only positive values accepted

                // Positive values
                assertTrue(rs.next());
                assertEquals(rs.getInt("order"), 2);
                LocalDateTime positiveTime = rs.getObject("time", LocalDateTime.class);
                assertEquals(positiveTime.toEpochSecond(ZoneOffset.UTC), (TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59));
                LocalDateTime positiveTime64 = rs.getObject("time64", LocalDateTime.class);
                assertEquals(positiveTime64.toEpochSecond(ZoneOffset.UTC), (TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59));
                assertEquals(positiveTime64.getNano(), 999_999_999);

                // Time is stored as UTC (server timezone)
                assertEquals(rs.getTime("time", Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime(),
                        (TimeUnit.HOURS.toMillis(999) + TimeUnit.MINUTES.toMillis(59) + TimeUnit.SECONDS.toMillis(59)));

                // java.sql.Time max resolution is milliseconds
                assertEquals(rs.getTime("time64", Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime(),
                        (TimeUnit.HOURS.toMillis(999) + TimeUnit.MINUTES.toMillis(59) + TimeUnit.SECONDS.toMillis(59) + 999));

                assertEquals(rs.getTime("time"), rs.getObject("time", Time.class));
                assertEquals(rs.getTime("time64"), rs.getObject("time64", Time.class));

                // time has no date part and cannot be converted to Date or Timestamp
                for (String col : Arrays.asList("time", "time64")) {
                    assertThrows(SQLException.class, () -> rs.getDate(col));
                    assertThrows(SQLException.class, () -> rs.getTimestamp(col));
                    assertThrows(SQLException.class, () -> rs.getObject(col, Timestamp.class));
                    assertThrows(SQLException.class, () -> rs.getObject(col, Date.class));
                }
                assertFalse(rs.next());
            }
        }
    }

    @Test(groups = { "integration" })
    public void testStringTypes() throws SQLException {
        runQuery("CREATE TABLE test_strings (order Int8, "
                + "str String, fixed FixedString(6), "
                + "enum Enum8('a' = 6, 'b' = 7, 'c' = 8), enum8 Enum8('a' = 1, 'b' = 2, 'c' = 3), enum16 Enum16('a' = 1, 'b' = 2, 'c' = 3), "
                + "uuid UUID, escaped String "
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);

        String str = "string" + rand.nextInt(1000);
        String fixed = "fixed" + rand.nextInt(10);
        String enum8 = "a";
        String enum16 = "b";
        String uuid = UUID.randomUUID().toString();
        String escaped = "\\xA3\\xA3\\x12\\xA0\\xDF\\x13\\x4E\\x8C\\x87\\x74\\xD4\\x53\\xDB\\xFC\\x34\\x95";

        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_strings VALUES ( 1, ?, ?, ?, ?, ?, ?, ? )")) {
                stmt.setString(1, str);
                stmt.setString(2, fixed);
                stmt.setString(3, enum8);
                stmt.setString(4, enum8);
                stmt.setString(5, enum16);
                stmt.setString(6, uuid);
                stmt.setString(7, escaped);
                stmt.executeUpdate();
            }
        }

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_strings ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString("str"), str);
                    assertEquals(rs.getString("fixed"), fixed);
                    assertEquals(rs.getString("enum"), "a");
                    assertEquals(rs.getInt("enum"), 6);
                    assertEquals(rs.getString("enum8"), "a");
                    assertEquals(rs.getInt("enum8"), 1);
                    assertEquals(rs.getString("enum16"), "b");
                    assertEquals(rs.getInt("enum16"), 2);
                    assertEquals(rs.getString("uuid"), uuid);
                    assertEquals(rs.getString("escaped"), escaped);
                    assertFalse(rs.next());
                }
            }
        }

        // Check the results with getObject
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_strings ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getObject("str"), str);
                    assertEquals(rs.getObject("fixed"), fixed);
                    assertEquals(rs.getObject("enum"), "a");
                    assertEquals(rs.getObject("enum8"), "a");
                    assertEquals(rs.getObject("enum16"), "b");
                    assertEquals(rs.getObject("uuid"), UUID.fromString(uuid));
                    assertEquals(rs.getObject("escaped"), escaped);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testIpAddressTypes() throws SQLException, UnknownHostException {
        runQuery("CREATE TABLE test_ips (order Int8, "
                + "ipv4_ip IPv4, ipv4_name IPv4, ipv6 IPv6, ipv4_as_ipv6 IPv6"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);

        InetAddress ipv4AddressByIp = Inet4Address.getByName("90.176.75.97");
        InetAddress ipv4AddressByName = Inet4Address.getByName("www.example.com");
        InetAddress ipv6Address = Inet6Address.getByName("2001:adb8:85a3:1:2:8a2e:370:7334");
        InetAddress ipv4AsIpv6 = Inet4Address.getByName("90.176.75.97");

        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_ips VALUES ( 1, ?, ?, ?, ? )")) {
                stmt.setObject(1, ipv4AddressByIp);
                stmt.setObject(2, ipv4AddressByName);
                stmt.setObject(3, ipv6Address);
                stmt.setObject(4, ipv4AsIpv6);
                stmt.executeUpdate();
            }
        }

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_ips ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getObject("ipv4_ip"), ipv4AddressByIp);
                    assertEquals(rs.getObject("ipv4_ip", Inet6Address.class).getHostAddress(), "0:0:0:0:0:ffff:5ab0:4b61");
                    assertEquals(rs.getString("ipv4_ip"), ipv4AddressByIp.getHostAddress());
                    assertEquals(rs.getObject("ipv4_name"), ipv4AddressByName);
                    assertEquals(rs.getObject("ipv6"), ipv6Address);
                    assertEquals(rs.getString("ipv6"), ipv6Address.getHostAddress());
                    assertEquals(rs.getObject("ipv4_as_ipv6"), ipv4AsIpv6);
                    assertEquals(rs.getObject("ipv4_as_ipv6", Inet4Address.class), ipv4AsIpv6);
                    assertFalse(rs.next());
                }
            }
        }
    }


    @Test(groups = {"integration"})
    public void testArrayOfIpAddress() throws Exception {
        try (Connection connection = getJdbcConnection();
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT ['90.176.75.97'::IPv4] addrs1, ['2001:adb8:85a3:1:2:8a2e:370:7334'::IPv6] addrs2, ['2001:adb8:85a3:1:2:8a2e:370:7334'::IPv6, null] addrs3")) {

            InetAddress ipv4AddressByIp = Inet4Address.getByName("90.176.75.97");
            InetAddress ipv6Address = Inet6Address.getByName("2001:adb8:85a3:1:2:8a2e:370:7334");

            Assert.assertTrue(rs.next());
            {
                // IPv4
                Array addrs1 = rs.getArray(1);
                Object[] arr = (Object[]) addrs1.getArray();
                Assert.assertEquals(ipv4AddressByIp, arr[0]);

                ResultSet arrRs = addrs1.getResultSet();
                arrRs.next();
                Assert.assertEquals(ipv4AddressByIp, arrRs.getObject(2));
            }

            {
                // IPv6
                Array addrs2 = rs.getArray(2);
                Object[] arr = (Object[]) addrs2.getArray();
                Assert.assertEquals(ipv6Address, arr[0]);

                ResultSet arrRs = addrs2.getResultSet();
                arrRs.next();
                Assert.assertEquals(ipv6Address, arrRs.getObject(2));
            }

            {
                // IPv6
                Array addrs3 = rs.getArray(3);
                Assert.assertEquals(addrs3.getBaseTypeName(), "Nullable(IPv6)");
                Object[] arr = (Object[]) addrs3.getArray();
                Assert.assertEquals(ipv6Address, arr[0]);

                ResultSet arrRs = addrs3.getResultSet();
                arrRs.next();
                Assert.assertEquals(ipv6Address, arrRs.getObject(2));
                arrRs.next();
                Assert.assertNull(arrRs.getObject(2));
            }
        }
    }


    @Test(groups = { "integration" })
    public void testFloatTypes() throws SQLException {
        runQuery("CREATE TABLE test_floats (order Int8, "
                + "float32 Float32, float64 Float64"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert minimum values
        insertData("INSERT INTO test_floats VALUES ( 1, -3.4028233E38, -1.7976931348623157E308 )");

        // Insert maximum values
        insertData("INSERT INTO test_floats VALUES ( 2, 3.4028233E38, 1.7976931348623157E308 )");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        Float float32 = rand.nextFloat();
        Double float64 = rand.nextDouble();

        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_floats VALUES ( 3, ?, ? )")) {
                stmt.setFloat(1, float32);
                stmt.setDouble(2, float64);
                stmt.executeUpdate();
            }
        }

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_floats ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getFloat("float32"), -3.402823E38f);
                    assertEquals(rs.getDouble("float64"), Double.valueOf(-1.7976931348623157E308));

                    assertTrue(rs.next());
                    assertEquals(rs.getFloat("float32"), Float.valueOf(3.402823E38f));
                    assertEquals(rs.getDouble("float64"), Double.valueOf(1.7976931348623157E308));

                    assertTrue(rs.next());
                    assertEquals(rs.getFloat("float32"), float32);
                    assertEquals(rs.getDouble("float64"), float64);

                    assertFalse(rs.next());
                }
            }
        }

        // Check the results with getObject
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_floats ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getObject("float32"), -3.402823E38f);
                    assertEquals(rs.getObject("float64"), Double.valueOf(-1.7976931348623157E308));

                    assertTrue(rs.next());
                    assertEquals(rs.getObject("float32"), 3.402823E38f);
                    assertEquals(rs.getObject("float64"), Double.valueOf(1.7976931348623157E308));

                    assertTrue(rs.next());

                    DecimalFormat df = new DecimalFormat("#.######");
                    assertEquals(df.format(rs.getObject("float32")), df.format(float32));
                    assertEquals(rs.getObject("float64"), float64);

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testBooleanTypes() throws SQLException {
        runQuery("CREATE TABLE test_booleans (order Int8, "
                + "bool Boolean"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        boolean bool = rand.nextBoolean();

        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_booleans VALUES ( 1, ? )")) {
                stmt.setBoolean(1, bool);
                stmt.executeUpdate();
            }
        }

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_booleans ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getBoolean("bool"), bool);

                    assertFalse(rs.next());
                }
            }
        }

        // Check the results with getObject
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_booleans ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getObject("bool"), bool);

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testArrayTypes() throws SQLException {
        runQuery("CREATE TABLE test_arrays (order Int8, "
                + "array Array(Int8), arraystr Array(String), "
                + "arraytuple Array(Tuple(Int8, String)), "
                + "arraydate Array(Date)"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        Integer[] array = new Integer[10];
        for (int i = 0; i < array.length; i++) {
            array[i] = rand.nextInt(256) - 128;
        }

        String[] arraystr = new String[10];
        for (int i = 0; i < arraystr.length; i++) {
            arraystr[i] = "string" + rand.nextInt(1000);
        }

        Tuple[] arraytuple = new Tuple[10];
        for (int i = 0; i < arraytuple.length; i++) {
            arraytuple[i] = new Tuple(rand.nextInt(256) - 128, "string" + rand.nextInt(1000));
        }

        Date[] arraydate = new Date[10];
        for (int i = 0; i < arraydate.length; i++) {
            arraydate[i] = Date.valueOf(LocalDate.now().plusDays(rand.nextInt(100)));
        }

        // Insert using `Connection#createArrayOf`
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_arrays VALUES ( 1, ?, ?, ?, ?)")) {
                stmt.setArray(1, conn.createArrayOf("Int8", array));
                stmt.setArray(2, conn.createArrayOf("String", arraystr));
                stmt.setArray(3, conn.createArrayOf("Tuple(Int8, String)", arraytuple));
                stmt.setArray(3, conn.createArrayOf("Tuple(Int8, String)", arraytuple));
                stmt.setArray(4, conn.createArrayOf("Date", arraydate));
                stmt.executeUpdate();
            }
        }

        // Insert using common java objects
        final String INSERT_SQL = "INSERT INTO test_arrays VALUES ( 2, ?, ?, ?, ?)";
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(INSERT_SQL)) {
                stmt.setObject(1, array);
                stmt.setObject(2, arraystr);
                stmt.setObject(3, arraytuple);
                stmt.setObject(4, arraydate);
                stmt.executeUpdate();
            }
        }

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_arrays ORDER BY order")) {
                    assertTrue(rs.next());
                    {
                        Object[] arrayResult = (Object[]) rs.getArray("array").getArray();
                        assertEquals(arrayResult.length, array.length);
                        for (int i = 0; i < array.length; i++) {
                            assertEquals(String.valueOf(arrayResult[i]), String.valueOf(array[i]));
                        }

                        Object[] arraystrResult = (Object[]) rs.getArray("arraystr").getArray();
                        assertEquals(arraystrResult.length, arraystr.length);
                        for (int i = 0; i < arraystr.length; i++) {
                            assertEquals(arraystrResult[i], arraystr[i]);
                        }
                        Object[] arraytupleResult = (Object[]) rs.getArray("arraytuple").getArray();
                        assertEquals(arraytupleResult.length, arraytuple.length);
                        for (int i = 0; i < arraytuple.length; i++) {
                            Tuple tuple = arraytuple[i];
                            Tuple tupleResult = new Tuple(((Object[]) arraytupleResult[i]));
                            assertEquals(String.valueOf(tupleResult.getValue(0)), String.valueOf(tuple.getValue(0)));
                            assertEquals(String.valueOf(tupleResult.getValue(1)), String.valueOf(tuple.getValue(1)));
                        }

                        Object[] arraydateResult = (Object[]) rs.getArray("arraydate").getArray();
                        assertEquals(arraydateResult.length, arraydate.length);
                        for (int i = 0; i < arraydate.length; i++) {
                            assertEquals(String.valueOf(arraydateResult[i]), String.valueOf(arraydate[i]));
                        }
                    }
                    assertTrue(rs.next());
                    {
                        Object[] arrayResult = (Object[]) ((Array) rs.getObject("array")).getArray();
                        assertEquals(arrayResult.length, array.length);
                        for (int i = 0; i < array.length; i++) {
                            assertEquals(String.valueOf(arrayResult[i]), String.valueOf(array[i]));
                        }

                        Object[] arraystrResult = (Object[]) ((Array) rs.getObject("arraystr")).getArray();
                        assertEquals(arraystrResult.length, arraystr.length);
                        for (int i = 0; i < arraystr.length; i++) {
                            assertEquals(arraystrResult[i], arraystr[i]);
                        }
                        Object[] arraytupleResult = (Object[]) ((Array) rs.getObject("arraytuple")).getArray();
                        assertEquals(arraytupleResult.length, arraytuple.length);
                        for (int i = 0; i < arraytuple.length; i++) {
                            Tuple tuple = arraytuple[i];
                            Tuple tupleResult = new Tuple(((Object[]) arraytupleResult[i]));
                            assertEquals(String.valueOf(tupleResult.getValue(0)), String.valueOf(tuple.getValue(0)));
                            assertEquals(String.valueOf(tupleResult.getValue(1)), String.valueOf(tuple.getValue(1)));
                        }

                        Object[] arraydateResult = (Object[]) ((Array) rs.getObject("arraydate")).getArray();
                        assertEquals(arraydateResult.length, arraydate.length);
                        for (int i = 0; i < arraydate.length; i++) {
                            assertEquals(arraydateResult[i], arraydate[i]);
                        }
                    }
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testNestedArrays() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?::Array(Array(Int32)) as value")) {
                Integer[][] srcArray = new Integer[][] {
                        {1, 2, 3},
                        {4, 5, 6}
                };
                Array array = conn.createArrayOf("Int32", srcArray);
                stmt.setArray(1, array);

                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    Array arrayHolder = (Array) rs.getObject(1);
                    Object[] dbArray = (Object[]) arrayHolder.getArray();
                    for (int i = 0; i < dbArray.length; i++) {
                        Object[] nestedArray = (Object[]) dbArray[i];
                        for (int j = 0; j < nestedArray.length; j++) {
                            assertEquals((Integer) nestedArray[j], (Integer)srcArray[i][j]);
                        }
                    }
                }

                Integer[] simpleArray = new Integer[] {1, 2, 3};
                Array array1 = conn.createArrayOf("Int32", simpleArray);
                Array array2 = conn.createArrayOf("Int32", simpleArray);

                Array[] multiLevelArray = new Array[] {array1, array2};
                Array array3 = conn.createArrayOf("Int32", multiLevelArray);
                stmt.setArray(1, array3);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    Array arrayHolder = (Array) rs.getObject(1);
                    Object[] dbArray = (Object[]) arrayHolder.getArray();
                    for (int i = 0; i < dbArray.length; i++) {
                        Object[] nestedArray = (Object[]) dbArray[i];
                        for (int j = 0; j < nestedArray.length; j++) {
                            assertEquals((Integer) nestedArray[j], (Integer)simpleArray[j]);
                        }
                    }
                }
            }
        }
    }

    /**
     * Test for https://github.com/ClickHouse/clickhouse-java/issues/2723
     * getString() on nested arrays was failing with NullPointerException due to re-entrancy bug
     * in DataTypeConverter when converting nested arrays to string representation.
     */
    @Test(groups = { "integration" })
    public void testNestedArrayToString() throws SQLException {
        // Test 1: Simple nested array - getString on Array(Array(Int32))
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT [[1, 2, 3], [4, 5, 6]] as nested_array")) {
                    assertTrue(rs.next());
                    // This was throwing NullPointerException before the fix
                    String result = rs.getString("nested_array");
                    assertEquals(result, "[[1, 2, 3], [4, 5, 6]]");
                }
            }
        }

        // Test 2: Query similar to issue #2723 with splitByChar returning array
        // The original issue was that getString() on an array column inside a CASE/WHEN
        // would cause NPE. This test verifies that getString() works correctly on arrays.
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                String query = "SELECT " +
                        "splitByChar('_', 'field1_field2_field3') as split_result, " +
                        "CASE " +
                        "    WHEN " +
                        "         splitByChar('_', 'field1_field2_field3')[1] IN ('field1', 'field2') " +
                        "         AND match( " +
                        "             splitByChar('_', 'field1_field2_field3')[2], " +
                        "             '(field1|field2|field3)' " +
                        "         ) " +
                        "        THEN 'Matched' " +
                        "    ELSE 'NotMatched' " +
                        "END AS action_to_do";
                try (ResultSet rs = stmt.executeQuery(query)) {
                    assertTrue(rs.next());
                    // The key test is that getString() doesn't throw NPE on array column
                    String splitResult = rs.getString("split_result");
                    assertEquals(splitResult, "['field1', 'field2', 'field3']");
                    String actionResult = rs.getString("action_to_do");
                    assertEquals(actionResult, "Matched");
                }
            }
        }

        // Test 3: Deeply nested arrays - Array(Array(Array(String)))
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT [[['a', 'b'], ['c']], [['d', 'e', 'f']]] as deep_nested")) {
                    assertTrue(rs.next());
                    String result = rs.getString("deep_nested");
                    assertEquals(result, "[[['a', 'b'], ['c']], [['d', 'e', 'f']]]");
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testMapTypes() throws SQLException {
        runQuery("CREATE TABLE test_maps (order Int8, "
                + "map Map(String, Int8), mapstr Map(String, String)"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        int mapSize = rand.nextInt(100);
        Map<String, Integer> integerMap = new java.util.HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            integerMap.put("key" + i, rand.nextInt(256) - 128);
        }

        Map<String, String> stringMap = new java.util.HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            stringMap.put("key" + i, "string" + rand.nextInt(1000));
        }

        // Insert random (valid) values
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_maps VALUES ( 1, ?, ? )")) {
                stmt.setObject(1, integerMap);
                stmt.setObject(2, stringMap);
                stmt.executeUpdate();
            }
        }

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_maps ORDER BY order")) {
                    assertTrue(rs.next());
                    Map<Object, Object> mapResult = (Map<Object, Object>) rs.getObject("map");
                    assertEquals(mapResult.size(), mapSize);
                    for (String key: integerMap.keySet()) {
                        assertEquals(String.valueOf(mapResult.get(key)), String.valueOf(integerMap.get(key)));
                    }

                    Map<Object, Object> mapstrResult = (Map<Object, Object>) rs.getObject("mapstr");
                    assertEquals(mapstrResult.size(), mapSize);
                    for (String key: stringMap.keySet()) {
                        assertEquals(String.valueOf(mapstrResult.get(key)), String.valueOf(stringMap.get(key)));
                    }
                }
            }
        }
    }


    @Test(groups = { "integration" })
    public void testMapTypesWithArrayValues() throws SQLException {
        runQuery("DROP TABLE test_maps;");
        runQuery("CREATE TABLE test_maps (order Int8, "
                + "map Map(String, Array(Int32)), "
                + "map2 Map(String, Array(Int32))"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        int mapSize = 3;
        Map<String, int[]> integerMap = new java.util.HashMap<>(mapSize);
        Map<String, Integer[]> integerMap2 = new java.util.HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            int[] array = new int[10];
            Integer[] array2 = new Integer[10];
            for (int j = 0; j < array.length; j++) {
                array[j] = array2[j] = rand.nextInt(1000);

            }
            integerMap.put("key" + i, array);
            integerMap2.put("key" + i, array2);
        }

        // Insert random (valid) values
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_maps VALUES ( 1, ?, ?)")) {
                stmt.setObject(1, integerMap);
                stmt.setObject(2, integerMap2);
                stmt.executeUpdate();
            }
        }

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_maps ORDER BY order")) {
                    assertTrue(rs.next());
                    Map<Object, Object> mapResult = (Map<Object, Object>) rs.getObject("map");
                    assertEquals(mapResult.size(), mapSize);
                    for (String key: integerMap.keySet()) {
                        Object[] arrayResult = ((List<?>) mapResult.get(key)).toArray();
                        int[] array = integerMap.get(key);
                        assertEquals(arrayResult.length, array.length);
                        for (int i = 0; i < array.length; i++) {
                            assertEquals(String.valueOf(arrayResult[i]), String.valueOf(array[i]));
                        }
                    }
                }
            }
        }
    }

    @Test(groups = {"integration"})
    public void testArrayOfMaps() throws Exception {
        try (Connection connection = getJdbcConnection();
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT [map('a', 1, 'b', 2)::Map(String, Int32)] arr1")) {

            Assert.assertTrue(rs.next());
            {
                // Array(Map(String, Int32))
                Array arrMap1 = rs.getArray(1);
                Assert.assertEquals(arrMap1.getBaseTypeName(), "Map(String, Int32)");
                Object[] arr = (Object[]) arrMap1.getArray();
                @SuppressWarnings("unchecked")
                Map<String, Integer> map1 = (Map<String, Integer>) arr[0];
                Assert.assertEquals(map1.get("a"), Integer.valueOf(1));
                Assert.assertEquals(map1.get("b"), Integer.valueOf(2));

                ResultSet arrRs = arrMap1.getResultSet();
                arrRs.next();
                @SuppressWarnings("unchecked")
                Map<String, Integer> rsMap1 = (Map<String, Integer>) arrRs.getObject(2);
                Assert.assertEquals(rsMap1.get("a"), Integer.valueOf(1));
                Assert.assertEquals(rsMap1.get("b"), Integer.valueOf(2));
            }
        }
    }

    @Test(groups = { "integration" })
    public void testNullableTypesSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_nullable (order Int8, "
                + "int8 Nullable(Int8), int16 Nullable(Int16), int32 Nullable(Int32), int64 Nullable(Int64), int128 Nullable(Int128), int256 Nullable(Int256), "
                + "uint8 Nullable(UInt8), uint16 Nullable(UInt16), uint32 Nullable(UInt32), uint64 Nullable(UInt64), uint128 Nullable(UInt128), uint256 Nullable(UInt256), "
                + "dec Nullable(Decimal(9, 2)), dec32 Nullable(Decimal32(4)), dec64 Nullable(Decimal64(8)), dec128 Nullable(Decimal128(18)), dec256 Nullable(Decimal256(18)), "
                + "date Nullable(Date), date32 Nullable(Date32), "
                + "dateTime Nullable(DateTime), dateTime32 Nullable(DateTime32), "
                + "dateTime643 Nullable(DateTime64(3)), dateTime646 Nullable(DateTime64(6)), dateTime649 Nullable(DateTime64(9)), "
                + "str Nullable(String), fixed Nullable(FixedString(6)), "
                + "enum Nullable(Enum8('a' = 6, 'b' = 7, 'c' = 8)), enum8 Nullable(Enum8('a' = 1, 'b' = 2, 'c' = 3)), enum16 Nullable(Enum16('a' = 1, 'b' = 2, 'c' = 3)), "
                + "uuid Nullable(UUID), ipv4 Nullable(IPv4), ipv6 Nullable(IPv6), "
                + "float32 Nullable(Float32), float64 Nullable(Float64), "
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert null values
        insertData("INSERT INTO test_nullable VALUES ( 1, "
                + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
                + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
                + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
                + "NULL, NULL, NULL, NULL)");

        //Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_nullable ORDER BY order")) {
                    assertTrue(rs.next());
                    for (int i = 2; i <= 34; i++) {
                        assertTrue(rs.getObject(i) == null);
                    }

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testLowCardinalityTypeSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_low_cardinality (order Int8, "
                + "lowcardinality LowCardinality(String)"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        String lowcardinality = "string" + rand.nextInt(1000);

        insertData(String.format("INSERT INTO test_low_cardinality VALUES ( 1, '%s' )",
                lowcardinality));

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_low_cardinality ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString("lowcardinality"), lowcardinality);
                    assertEquals(rs.getObject("lowcardinality"), lowcardinality);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSimpleAggregateFunction() throws SQLException {
        runQuery("CREATE TABLE test_aggregate (order Int8," +
                " int8 Int8," +
                " val SimpleAggregateFunction(any, Nullable(Int8))" +
                ") ENGINE = MergeTree ORDER BY ()");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        int int8 = rand.nextInt(256) - 128;

        insertData(String.format("INSERT INTO test_aggregate VALUES ( 1, %d, null )", int8));
        insertData(String.format("INSERT INTO test_aggregate VALUES ( 2, %d, null )", int8));
        insertData(String.format("INSERT INTO test_aggregate VALUES ( 3, %d, null )", int8));

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT sum(int8) FROM test_aggregate")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), int8 * 3);
                    assertEquals(rs.getObject(1), (long) (int8 * 3));
                }
                try (ResultSet rs = stmt.executeQuery("SELECT any(val) FROM test_aggregate")) {
                    assertTrue(rs.next());
                    assertNull(rs.getObject(1));
                    assertTrue(rs.wasNull());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testNestedTypeSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_nested (order Int8, "
                + "nested Nested (int8 Int8, int16 Int16, int32 Int32, int64 Int64, int128 Int128, int256 Int256)"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        int int8 = rand.nextInt(256) - 128;
        int int16 = rand.nextInt(65536) - 32768;
        int int32 = rand.nextInt();
        long int64 = rand.nextLong();
        BigInteger int128 = new BigInteger(127, rand);
        BigInteger int256 = new BigInteger(255, rand);

        String sql = String.format("INSERT INTO test_nested VALUES ( 1, [%s], [%s], [%s], [%s], [%s], [%s])",
                int8, int16, int32, int64, int128, int256);
        log.info("SQL: {}", sql);
        insertData(sql);

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_nested ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(String.valueOf(((Object[])rs.getArray("nested.int8").getArray())[0]), String.valueOf(int8));
                    assertEquals(String.valueOf(((Object[])rs.getArray("nested.int16").getArray())[0]), String.valueOf(int16));
                    assertEquals(String.valueOf(((Object[])rs.getArray("nested.int32").getArray())[0]), String.valueOf(int32));
                    assertEquals(String.valueOf(((Object[])rs.getArray("nested.int64").getArray())[0]), String.valueOf(int64));
                    assertEquals(String.valueOf(((Object[])rs.getArray("nested.int128").getArray())[0]), String.valueOf(int128));
                    assertEquals(String.valueOf(((Object[])rs.getArray("nested.int256").getArray())[0]), String.valueOf(int256));

                    assertFalse(rs.next());
                }
            }
        }

    }

    @Test(groups = { "integration" })
    public void testNestedTypeNonFlatten() throws SQLException {
        if (earlierThan(25,1)){
            return;
        }
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET flatten_nested = 0");
                stmt.execute("CREATE TABLE test_nested_not_flatten (order Int8, "
                        + "nested Nested (int8 Int8, int16 Int16, int32 Int32, int64 Int64, int128 Int128, int256 Int256)"
                        + ") ENGINE = MergeTree ORDER BY () SETTINGS flatten_nested = 0");
                // Insert random (valid) values
                long seed = System.currentTimeMillis();
                Random rand = new Random(seed);
                log.info("Random seed was: {}", seed);

                int int8 = rand.nextInt(256) - 128;
                int int16 = rand.nextInt(65536) - 32768;
                int int32 = rand.nextInt();
                long int64 = rand.nextLong();
                BigInteger int128 = new BigInteger(127, rand);
                BigInteger int256 = new BigInteger(255, rand);


                String nsql = String.format("INSERT INTO test_nested_not_flatten VALUES ( 1, [(%s,%s,%s,%s,%s,%s)])",
                        int8, int16, int32, int64, int128, int256);
                log.info("SQL: {}", nsql);
                stmt.executeUpdate(nsql);

                // Check the results

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_nested_not_flatten ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals((Object[])((Object[])((java.sql.Array) rs.getObject("nested")).getArray())[0],
                            new Object[] {(byte) int8, (short) int16, int32, int64, int128, int256});

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testTupleTypeSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_tuple (order Int8, "
                + "tuple Tuple(int8 Int8, int16 Int16, int32 Int32, int64 Int64, int128 Int128, int256 Int256)"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        int int8 = rand.nextInt(256) - 128;
        int int16 = rand.nextInt(65536) - 32768;
        int int32 = rand.nextInt();
        long int64 = rand.nextLong();
        BigInteger int128 = new BigInteger(127, rand);
        BigInteger int256 = new BigInteger(255, rand);

        String sql = String.format("INSERT INTO test_tuple VALUES ( 1, (%s, %s, %s, %s, %s, %s))",
                int8, int16, int32, int64, int128, int256);
        insertData(sql);

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_tuple ORDER BY order")) {
                    assertTrue(rs.next());
                    Object[] tuple = (Object[]) rs.getObject(2);
                    assertEquals(String.valueOf(tuple[0]), String.valueOf(int8));
                    assertEquals(String.valueOf(tuple[1]), String.valueOf(int16));
                    assertEquals(String.valueOf(tuple[2]), String.valueOf(int32));
                    assertEquals(String.valueOf(tuple[3]), String.valueOf(int64));
                    assertEquals(String.valueOf(tuple[4]), String.valueOf(int128));
                    assertEquals(String.valueOf(tuple[5]), String.valueOf(int256));
                    assertFalse(rs.next());
                }
            }
        }
    }



    @Test(groups = { "integration" })
    public void testJSONWritingAsString() throws SQLException {
        if (ClickHouseVersion.of(getServerVersion()).check("(,24.8]")) {
            return; // JSON was introduced in 24.10
        }

        Properties createProperties = new Properties();
        createProperties.put(ClientConfigProperties.serverSetting("allow_experimental_json_type"), "1");
        runQuery("CREATE TABLE test_json (order Int8, "
                + "json JSON"
                + ") ENGINE = MergeTree ORDER BY ()", createProperties);

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        double key1 =  rand.nextDouble();
        int key2 =  rand.nextInt();
        final String json = "{\"key1\": \"" + key1 + "\", \"key2\": " + key2 + ", \"key3\": [1000, \"value3\", 400000]}";
        final String serverJson = "{\"key1\":\"" + key1 + "\",\"key2\":\"" + key2 + "\",\"key3\":[\"1000\",\"value3\",\"400000\"]}";
        insertData(String.format("INSERT INTO test_json VALUES ( 1, '%s' )", json));

        // Check the results
        Properties props = new Properties();
        props.setProperty(
                ClientConfigProperties.serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING),
                "1");
        props.setProperty(ClientConfigProperties.serverSetting("output_format_json_quote_64bit_integers"), "1");
        try (Connection conn = getJdbcConnection(props)) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_json ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString("json"), serverJson);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testReadingJSONBinary() throws SQLException {
        if (ClickHouseVersion.of(getServerVersion()).check("(,24.8]")) {
            return; // JSON was introduced in 24.10
        }

        Properties properties = new Properties();
        properties.put(ClientConfigProperties.serverSetting("allow_experimental_json_type"), "1");
        try (Connection conn = getJdbcConnection(properties);
             Statement stmt = conn.createStatement()) {

            final String json = "{\"count\": 1000, \"event\": { \"name\": \"start\", \"value\": 0.10} }";
            String sql = String.format("SELECT %1$s::JSON(), %1$s::JSON(count Int16)", SQLUtils.enquoteLiteral(json));
            try (ResultSet rs = stmt.executeQuery(sql)) {
                rs.next();

                Map<String, Object> val1 = (Map<String, Object>) rs.getObject(1);
                assertEquals(val1.get("count"), 1000L);
                Map<String, Object> val2 = (Map<String, Object>) rs.getObject(2);
                assertEquals(val2.get("count"), (short)1000);
            }
        }
    }


        @Test(groups = { "integration" }, enabled = false)
    public void testGeometricTypesSimpleStatement() throws SQLException {
        // TODO: add LineString and MultiLineString support
        runQuery("CREATE TABLE test_geometric (order Int8, "
                + "point Point, ring Ring, linestring LineString, multilinestring MultiLineString, polygon Polygon, multipolygon MultiPolygon"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        String point = "(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + ")";
        String ring = "[(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + "),(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + "),(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + ")]";
        String linestring = "[(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + "),(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + "),(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + ")]";
        String multilinestring = "[[(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + "),(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + ")],[(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + "),(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + ")]]";
        String polygon = "[[(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + ")],[(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + "),(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + ")]]";
        String multipolygon = "[[[(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + ")],[(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + "),(" + rand.nextInt(1000) + "," + rand.nextInt(1000) + ")]]]";

        insertData(String.format("INSERT INTO test_geometric VALUES ( 1, %s, %s, %s, %s, %s, %s )",
                point, ring, linestring, multilinestring, polygon, multipolygon));

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_geometric ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString("point"), point);
                    assertEquals(rs.getString("linestring"), linestring);
                    assertEquals(rs.getString("polygon"), polygon);
                    assertEquals(rs.getString("multilinestring"), multilinestring);
                    assertEquals(rs.getString("multipolygon"), multipolygon);
                    assertEquals(rs.getString("ring"), ring);

                    assertFalse(rs.next());
                }
            }
        }
    }


    @Test(groups = { "integration" })
    public void testDynamicTypesSimpleStatement() throws SQLException {
        if (earlierThan(24, 8)) {
            return;
        }

        Properties properties = new Properties();
        properties.setProperty(ClientConfigProperties.serverSetting("allow_experimental_dynamic_type"), "1");
        runQuery("CREATE TABLE test_dynamic (order Int8, "
                + "dynamic Dynamic"
                + ") ENGINE = MergeTree ORDER BY ()",
                properties);

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        String dynamic = "string" + rand.nextInt(1000);
        int dynamic2 = rand.nextInt(256) - 128;
        double dynamic3 = rand.nextDouble();

        String sql = String.format("INSERT INTO test_dynamic VALUES ( 1, '%s' )", dynamic);
        insertData(sql);

        sql = String.format("INSERT INTO test_dynamic VALUES ( 2, %d )", dynamic2);
        insertData(sql);

        sql = String.format("INSERT INTO test_dynamic VALUES ( 3, %s )", dynamic3);
        insertData(sql);

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_dynamic ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString("dynamic"), dynamic);

                    assertTrue(rs.next());
                    assertEquals(rs.getInt("dynamic"), dynamic2);

                    assertTrue(rs.next());
                    assertEquals(rs.getDouble("dynamic"), dynamic3);

                    assertFalse(rs.next());
                }
            }
        }
    }


    @Test(groups = { "integration" })
    public void testTypeConversions() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT 1, 'true', '1.0', " +
                        "toDate('2024-12-01'), toDateTime('2024-12-01 12:34:56'), toDateTime64('2024-12-01 12:34:56.789', 3), toDateTime64('2024-12-01 12:34:56.789789', 6), toDateTime64('2024-12-01 12:34:56.789789789', 9)")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), 1);
                    assertEquals(String.valueOf(rs.getObject(1)), "1");
                    assertEquals(rs.getObject(1, Integer.class), 1);
                    assertEquals(rs.getObject(1, Long.class), 1L);
                    assertEquals(String.valueOf(rs.getObject(1, new HashMap<String, Class<?>>(){{put(JDBCType.INTEGER.getName(), Integer.class);}})), "1");

                    assertTrue(rs.getBoolean(2));
                    assertEquals(String.valueOf(rs.getObject(2)), "true");
                    assertEquals(rs.getObject(2, Boolean.class), true);
                    assertEquals(String.valueOf(rs.getObject(2, new HashMap<String, Class<?>>(){{put(JDBCType.BOOLEAN.getName(), Boolean.class);}})), "true");

                    assertEquals(rs.getFloat(3), 1.0f);
                    assertEquals(String.valueOf(rs.getObject(3)), "1.0");
                    assertEquals(rs.getObject(3, Float.class), 1.0f);
                    assertEquals(rs.getObject(3, Double.class), 1.0);
                    assertEquals(String.valueOf(rs.getObject(3, new HashMap<String, Class<?>>(){{put(JDBCType.FLOAT.getName(), Float.class);}})), "1.0");

                    assertEquals(rs.getDate(4), Date.valueOf("2024-12-01"));
                    assertTrue(rs.getObject(4) instanceof Date);
                    assertEquals(rs.getObject(4), Date.valueOf("2024-12-01"));
                    assertEquals(rs.getString(4), "2024-12-01");//Underlying object is ZonedDateTime
                    assertEquals(rs.getObject(4, LocalDate.class), LocalDate.of(2024, 12, 1));
                    assertThrows(SQLException.class, () -> rs.getObject(4, ZonedDateTime.class)); // Date cannot be presented as time
                    assertEquals(String.valueOf(rs.getObject(4, new HashMap<String, Class<?>>(){{put(JDBCType.DATE.getName(), LocalDate.class);}})), "2024-12-01");

                    assertEquals(rs.getTimestamp(5).toString(), "2024-12-01 12:34:56.0");
                    assertTrue(rs.getObject(5) instanceof Timestamp);
                    assertEquals(rs.getObject(5), Timestamp.valueOf("2024-12-01 12:34:56"));
                    assertEquals(rs.getString(5), "2024-12-01 12:34:56");
                    assertEquals(rs.getObject(5, LocalDateTime.class), LocalDateTime.of(2024, 12, 1, 12, 34, 56));
                    assertEquals(rs.getObject(5, ZonedDateTime.class), ZonedDateTime.of(2024, 12, 1, 12, 34, 56, 0, ZoneId.of("UTC")));
                    assertEquals(String.valueOf(rs.getObject(5, new HashMap<String, Class<?>>(){{put(JDBCType.TIMESTAMP.getName(), LocalDateTime.class);}})), "2024-12-01T12:34:56");

                    assertEquals(rs.getTimestamp(6).toString(), "2024-12-01 12:34:56.789");
                    assertTrue(rs.getObject(6) instanceof Timestamp);
                    assertEquals(rs.getObject(6), Timestamp.valueOf("2024-12-01 12:34:56.789"));
                    assertEquals(rs.getString(6), "2024-12-01 12:34:56.789");
                    assertEquals(rs.getObject(6, LocalDateTime.class), LocalDateTime.of(2024, 12, 1, 12, 34, 56, 789000000));
                    assertEquals(String.valueOf(rs.getObject(6, new HashMap<String, Class<?>>(){{put(JDBCType.TIMESTAMP.getName(), LocalDateTime.class);}})), "2024-12-01T12:34:56.789");

                    assertEquals(rs.getTimestamp(7).toString(), "2024-12-01 12:34:56.789789");
                    assertTrue(rs.getObject(7) instanceof Timestamp);
                    assertEquals(rs.getObject(7), Timestamp.valueOf("2024-12-01 12:34:56.789789"));
                    assertEquals(rs.getString(7), "2024-12-01 12:34:56.789789");
                    assertEquals(rs.getObject(7, LocalDateTime.class), LocalDateTime.of(2024, 12, 1, 12, 34, 56, 789789000));
                    assertEquals(String.valueOf(rs.getObject(7, new HashMap<String, Class<?>>(){{put(JDBCType.TIMESTAMP.getName(), OffsetDateTime.class);}})), "2024-12-01T12:34:56.789789Z");

                    assertEquals(rs.getTimestamp(8).toString(), "2024-12-01 12:34:56.789789789");
                    assertTrue(rs.getObject(8) instanceof Timestamp);
                    assertEquals(rs.getObject(8), Timestamp.valueOf("2024-12-01 12:34:56.789789789"));
                    assertEquals(rs.getString(8), "2024-12-01 12:34:56.789789789");
                    assertEquals(rs.getObject(8, LocalDateTime.class), LocalDateTime.of(2024, 12, 1, 12, 34, 56, 789789789));
                    assertEquals(String.valueOf(rs.getObject(8, new HashMap<String, Class<?>>(){{put(JDBCType.TIMESTAMP.getName(), ZonedDateTime.class);}})), "2024-12-01T12:34:56.789789789Z[UTC]");
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testVariantTypesSimpleStatement() throws SQLException {
        if (earlierThan(24, 8)) {
            return;
        }

        Properties properties = new Properties();
        properties.setProperty(ClientConfigProperties.serverSetting("allow_experimental_variant_type"), "1");
        runQuery("CREATE TABLE test_variant (order Int8, "
                        + "v Variant(String, Int32)"
                        + ") ENGINE = MergeTree ORDER BY ()",
                properties);

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        String variant1 = "string" + rand.nextInt(1000);
        int variant2 = rand.nextInt(256) - 128;

        String sql = String.format("INSERT INTO test_variant VALUES ( 1, '%s' )", variant1);
        insertData(sql);

        sql = String.format("INSERT INTO test_variant VALUES ( 2, %d )", variant2);
        insertData(sql);


        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_variant ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString("v"), variant1);
                    assertTrue(rs.getObject("v") instanceof String);

                    assertTrue(rs.next());
                    assertEquals(rs.getInt("v"), variant2);
                    assertTrue(rs.getObject("v") instanceof Number);

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGeoPoint1() throws Exception {
        final Double[][] spatialArrayData = new Double[][] {
                {4.837388, 52.38795},
                {4.951513, 52.354582},
                {4.961987, 52.371763},
                {4.870017, 52.334932},
                {4.89813, 52.357238},
                {4.852437, 52.370315},
                {4.901712, 52.369567},
                {4.874112, 52.339823},
                {4.856942, 52.339122},
                {4.870253, 52.360353},
        };

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT \n");
        sql.append("\tcast(arrayJoin([");
        for (int i = 0; i < spatialArrayData.length; i++) {
            sql.append("(" + spatialArrayData[i][0] + ", " + spatialArrayData[i][1] + ")").append(',');
        }
        sql.setLength(sql.length() - 1);
        sql.append("])");
        sql.append("as Point) as Point");


        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql.toString())) {

                ResultSetMetaData metaData = rs.getMetaData();
                assertEquals(metaData.getColumnCount(), 1);
                assertEquals(metaData.getColumnTypeName(1), ClickHouseDataType.Point.name());
                assertEquals(metaData.getColumnType(1), Types.ARRAY);

                int rowCount = 0;
                while (rs.next()) {
                    Object asObject = rs.getObject(1);
                    assertTrue(asObject instanceof double[]);
                    Array asArray = rs.getArray(1);
                    assertEquals(asArray.getArray(), spatialArrayData[rowCount]);
                    assertEquals(asObject, asArray.getArray());
                    rowCount++;
                }
                assertTrue(rowCount > 0);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGeoPoint() throws Exception {
        final double[] row = new double[] {
            10.123456789,
            11.123456789
        };

        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            String table = "test_geo_point";
            stmt.executeUpdate("DROP TABLE IF EXISTS " + table);
            stmt.executeUpdate("CREATE TABLE " + table + " (geom Point) ENGINE = MergeTree ORDER BY ()");

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + table + " VALUES (?)")) {
                Double[] rowObj = Arrays.stream(row).boxed().toArray(Double[]::new);
                pstmt.setObject(1, conn.createStruct("Tuple(Float64, Float64)", rowObj));
                pstmt.executeUpdate();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {
                int geomColumn = 1;
                ResultSetMetaData rsMd = rs.getMetaData();
                assertEquals(rsMd.getColumnTypeName(geomColumn), ClickHouseDataType.Point.name());
                assertEquals(rsMd.getColumnType(geomColumn), Types.ARRAY);

                rs.next();
                assertTrue(rs.isLast());
                Object asObject = rs.getObject(geomColumn);
                assertTrue(asObject instanceof double[]);
                Array asArray = rs.getArray(geomColumn);
                assertEquals(asArray.getArray(),  row);
                assertEquals(asArray.getBaseTypeName(), ClickHouseDataType.Point.name());
                assertEquals(asArray.getBaseType(), Types.ARRAY);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGeoRing() throws Exception {
        final Double[][] row = new Double[][] {
                {10.123456789, 11.123456789},
                {12.123456789, 13.123456789},
                {14.123456789, 15.123456789},
                {10.123456789, 11.123456789},
        };

        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            final String table = "test_geo_ring";
            stmt.executeUpdate("DROP TABLE IF EXISTS " + table);
            stmt.executeUpdate("CREATE TABLE " + table + " (geom Ring) ENGINE = MergeTree ORDER BY ()");

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + table + " VALUES (?)")) {
                pstmt.setObject(1, conn.createArrayOf("Array(Point)", row));
                pstmt.executeUpdate();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {
                int geomColumn = 1;
                ResultSetMetaData rsMd = rs.getMetaData();
                assertEquals(rsMd.getColumnTypeName(geomColumn), ClickHouseDataType.Ring.name());
                assertEquals(rsMd.getColumnType(geomColumn), Types.ARRAY);

                rs.next();
                assertTrue(rs.isLast());
                Object asObject = rs.getObject(geomColumn);
                assertTrue(asObject instanceof double[][]);
                Array asArray = rs.getArray(geomColumn);
                assertEquals(asArray.getArray(),  row);
                assertEquals(asArray.getBaseTypeName(), ClickHouseDataType.Ring.name());
                assertEquals(asArray.getBaseType(), Types.ARRAY);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGeoLineString() throws Exception {
        final Double[][] row = new Double[][] {
                {10.123456789, 11.123456789},
                {12.123456789, 13.123456789},
                {14.123456789, 15.123456789},
                {10.123456789, 11.123456789},
        };

        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            final String table = "test_geo_line_string";
            stmt.executeUpdate("DROP TABLE IF EXISTS " + table);
            stmt.executeUpdate("CREATE TABLE " + table +" (geom LineString) ENGINE = MergeTree ORDER BY ()");

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + table + " VALUES (?)")) {
                pstmt.setObject(1, conn.createArrayOf("Array(Point)", row));
                pstmt.executeUpdate();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {
                int geomColumn = 1;
                ResultSetMetaData rsMd = rs.getMetaData();
                assertEquals(rsMd.getColumnTypeName(geomColumn), ClickHouseDataType.LineString.name());
                assertEquals(rsMd.getColumnType(geomColumn), Types.ARRAY);

                rs.next();
                assertTrue(rs.isLast());
                Object asObject = rs.getObject(geomColumn);
                assertTrue(asObject instanceof double[][]);
                Array asArray = rs.getArray(geomColumn);
                assertEquals(asArray.getArray(),  row);
                assertEquals(asArray.getBaseTypeName(), ClickHouseDataType.LineString.name());
                assertEquals(asArray.getBaseType(), Types.ARRAY);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGeoMultiLineString() throws Exception {
        final Double[][][] row = new Double[][][] {
                { // LineString 1
                        {10.123456789, 11.123456789},
                        {12.123456789, 13.123456789},
                        {14.123456789, 15.123456789},
                        {10.123456789, 11.123456789},
                },
                {
                        {16.123456789, 17.123456789},
                        {18.123456789, 19.123456789},
                        {20.123456789, 21.123456789},
                        {16.123456789, 17.123456789},
                }
        };

        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            final String table = "test_geo_multi_line_string";
            stmt.executeUpdate("DROP TABLE IF EXISTS " + table);
            stmt.executeUpdate("CREATE TABLE " + table +" (geom MultiLineString) ENGINE = MergeTree ORDER BY ()");

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + table + " VALUES (?)")) {
                pstmt.setObject(1, conn.createArrayOf("Array(Array(Point))", row));
                pstmt.executeUpdate();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {
                int geomColumn = 1;
                ResultSetMetaData rsMd = rs.getMetaData();
                assertEquals(rsMd.getColumnTypeName(geomColumn), ClickHouseDataType.MultiLineString.name());
                assertEquals(rsMd.getColumnType(geomColumn), Types.ARRAY);

                rs.next();
                assertTrue(rs.isLast());
                Object asObject = rs.getObject(geomColumn);
                assertTrue(asObject instanceof double[][][]);
                Array asArray = rs.getArray(geomColumn);
                assertEquals(asArray.getArray(),  row);
                assertEquals(asArray.getBaseTypeName(), ClickHouseDataType.MultiLineString.name());
                assertEquals(asArray.getBaseType(), Types.ARRAY);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGeoPolygon() throws Exception {
        final Double[][][] row = new Double[][][] {
                { // Ring 1
                        {10.123456789, 11.123456789},
                        {12.123456789, 13.123456789},
                        {14.123456789, 15.123456789},
                        {10.123456789, 11.123456789},
                },
                { // Ring 2
                        {16.123456789, 17.123456789},
                        {18.123456789, 19.123456789},
                        {20.123456789, 21.123456789},
                        {16.123456789, 17.123456789},
                }
        };

        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            final String table = "test_geo_polygon";
            stmt.executeUpdate("DROP TABLE IF EXISTS " + table);
            stmt.executeUpdate("CREATE TABLE " + table +" (geom Polygon) ENGINE = MergeTree ORDER BY ()");

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + table + " VALUES (?)")) {
                pstmt.setObject(1, conn.createArrayOf("Array(Array(Point))", row));
                pstmt.executeUpdate();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {
                int geomColumn = 1;
                ResultSetMetaData rsMd = rs.getMetaData();
                assertEquals(rsMd.getColumnTypeName(geomColumn), ClickHouseDataType.Polygon.name());
                assertEquals(rsMd.getColumnType(geomColumn), Types.ARRAY);

                rs.next();
                assertTrue(rs.isLast());
                Object asObject = rs.getObject(geomColumn);
                assertTrue(asObject instanceof double[][][]);
                Array asArray = rs.getArray(geomColumn);
                assertEquals(asArray.getArray(),  row);
                assertEquals(asArray.getBaseTypeName(), ClickHouseDataType.Polygon.name());
                assertEquals(asArray.getBaseType(), Types.ARRAY);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGeoMultiPolygon() throws Exception {
        final Double[][][][] row = new Double[][][][] {
                { // Polygon 1
                    { // Ring 1
                            {10.123456789, 11.123456789},
                            {12.123456789, 13.123456789},
                            {14.123456789, 15.123456789},
                            {10.123456789, 11.123456789},
                    },
                    { // Ring 2
                            {16.123456789, 17.123456789},
                            {18.123456789, 19.123456789},
                            {20.123456789, 21.123456789},
                            {16.123456789, 17.123456789},
                    }
                },
                { // Polygon 2
                        { // Ring 1
                                {-10.123456789, -11.123456789},
                                {-12.123456789, -13.123456789},
                                {-14.123456789, -15.123456789},
                                {-10.123456789, -11.123456789},
                        },
                        { // Ring 2
                                {-16.123456789, -17.123456789},
                                {-18.123456789, -19.123456789},
                                {-20.123456789, -21.123456789},
                                {-16.123456789, -17.123456789},
                        }
                }
        };

        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            final String table = "test_geo_muti_polygon";
            stmt.executeUpdate("DROP TABLE IF EXISTS " + table);
            stmt.executeUpdate("CREATE TABLE " + table +" (geom MultiPolygon) ENGINE = MergeTree ORDER BY ()");

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + table + " VALUES (?)")) {
                pstmt.setObject(1, conn.createArrayOf("Array(Array(Array(Point)))", row));
                pstmt.executeUpdate();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table)) {
                int geomColumn = 1;
                ResultSetMetaData rsMd = rs.getMetaData();
                assertEquals(rsMd.getColumnTypeName(geomColumn), ClickHouseDataType.MultiPolygon.name());
                assertEquals(rsMd.getColumnType(geomColumn), Types.ARRAY);

                rs.next();
                assertTrue(rs.isLast());
                Object asObject = rs.getObject(geomColumn);
                assertTrue(asObject instanceof double[][][][]);
                Array asArray = rs.getArray(geomColumn);
                assertEquals(asArray.getArray(),  row);
                assertEquals(asArray.getBaseTypeName(), ClickHouseDataType.MultiPolygon.name());
                assertEquals(asArray.getBaseType(), Types.ARRAY);
            }
        }
    }

    private static final HashMap<String, Object> EMPTY_JSON = new HashMap<>();

    @Test(groups = { "integration" }, dataProvider = "testJSONReadDP")
    public void testJSONRead(String json, Object expected) throws Exception {
        if (ClickHouseVersion.of(getServerVersion()).check("(,24.8]")) {
            return; // JSON was introduced in 24.10
        }
        Properties createProperties = new Properties();
        createProperties.put(ClientConfigProperties.serverSetting("allow_experimental_json_type"), "1");
        runQuery("DROP TABLE IF EXISTS test_jdbc_json_read");
        runQuery("CREATE TABLE test_jdbc_json_read (data JSON) ENGINE = MergeTree ORDER BY ()", createProperties);

        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            final String sql = "INSERT INTO test_jdbc_json_read (data) VALUES ('%s'), ('{}')";
            stmt.executeUpdate(String.format(sql, json));

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_jdbc_json_read")) {

                assertTrue(rs.next());
                Object jsonObj = rs.getObject(1);
                if (expected == null) {
                    expected = jsonToClientMap(json);
                }
                assertEquals(jsonObj, expected);
                assertTrue(rs.next());
                Object emptyJsonObj = rs.getObject(1);
                assertEquals(emptyJsonObj, EMPTY_JSON);
                assertFalse(rs.next());
            }
        }
    }

    private final ObjectMapper objectMapper = new ObjectMapper()
            .enable(DeserializationFeature.USE_LONG_FOR_INTS);

    private HashMap<String, Object> jsonToClientMap(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<HashMap<String, Object>>() {});
        } catch (IOException e) {
            throw new RuntimeException("Failed to read json to Map<String, Object>", e);
        }
    }

    @DataProvider(name = "testJSONReadDP")
    public Object[][] testJSONReadDP() {
        Map<String, Object> map1 = new HashMap<>();
        map1.put("nested.key", "value");
        Map<String, Object> map2 = new HashMap<>();
        map2.put("nested.numbers",new ArrayList<Long>() {{ add(1L); add(2L); add(3L); }});
        Map<String, Object> map3 = new HashMap<>();
        map3.put("nested.strings", new ArrayList<String>() {{ add("one"); add("two"); add("three"); }});
        Map<String, Object> map4 = new HashMap<>();
        map4.put("array", new ArrayList<HashMap<String,Object>>() {{
            add(new HashMap<String, Object>() {{
                put("nested.key", "value");
            }});
            add(new HashMap<String, Object>() {{
                put("nested.numbers", new ArrayList<Long>() {{
                    add(1L);
                    add(2L);
                    add(3L);
                }});
            }});
        }});
        Map<String, Object> map5 = new HashMap<>();
        map5.put("array", new ArrayList<HashMap<String,Object>>() {{
            add(new HashMap<String, Object>() {{
                put("nested.strings", new ArrayList<String>() {{ add("one"); add("two"); add("three"); }});

            }});
        }});
        Map<String, Object> map6 = new HashMap<>();
        map6.put("level1.level2.level3", "value");

        Map<String, Object> map7 = new HashMap<>();
        map7.put("level1.level2.level3.level4", "value");

        return new Object[][] {
                {"{\"key\": \"value\"}", null}, // Simple object
                {"{\"numbers\":[1, 2, 3]}", null},
                {"{\"strings\":[\"one\", \"two\", \"three\"]}", null},
                {"{\"nested\":{\"key\": \"value\"}}", map1}, // nested objects
                {"{\"nested\":{\"numbers\":[1, 2, 3]}}", map2}, // nested objects
                {"{\"nested\":{\"strings\":[\"one\", \"two\", \"three\"]}}", map3}, // nested objects
                {"{\"array\":[{\"key\": \"value\"},{\"key\": \"value\"}]}", null}, // array of objects
                {"{\"array\":[{\"numbers\":[1, 2, 3]},{\"strings\":[\"one\", \"two\", \"three\"]}]}", null}, // array of objects
                {"{\"array\":[{\"nested\":{\"key\": \"value\"}},{\"nested\":{\"numbers\":[1, 2, 3]}}]}", map4}, // array of objects
                {"{\"array\":[{\"nested\":{\"strings\":[\"one\", \"two\", \"three\"]}}]}", map5}, // array of objects
                {"{\"array\":[{\"nested\":[{\"key\": \"value\"}]}]}", null}, // simple array of objects
                {"{\"level1\": {\"level2\": {\"level3\": \"value\"}}}", map6}, // deep nested objects
                {"{\"level1\": {\"level2\": {\"level3\": {\"level4\": \"value\"}}}}", map7}, // deep nested objects

        };
    }

    /**
     * Tests that both Time and DateTime columns are readable as JDBC TIME type.
     * ClickHouse added Time and Time64 support in version 25.6.
     * On older versions DateTime types were used to emulate TIME.
     * This test ensures compatibility for reading time values from both column types.
     */
    @Test(groups = { "integration" })
    public void testTimeAndDateTimeCompatibleWithJDBCTime() throws Exception {
        boolean hasTimeType = !ClickHouseVersion.of(getServerVersion()).check("(,25.5]");

        Properties createProperties = new Properties();
        if (hasTimeType) {
            createProperties.put(ClientConfigProperties.serverSetting("allow_experimental_time_time64_type"), "1");
        }

        // Create table with DateTime columns (always supported) and Time columns (if available)
        String tableDDL = hasTimeType
                ? "CREATE TABLE test_time_compat (order Int8, "
                  + "time Time, time64 Time64(3), "
                  + "dateTime DateTime('UTC'), dateTime64 DateTime64(3, 'UTC') "
                  + ") ENGINE = MergeTree ORDER BY ()"
                : "CREATE TABLE test_time_compat (order Int8, "
                  + "dateTime DateTime('UTC'), dateTime64 DateTime64(3, 'UTC') "
                  + ") ENGINE = MergeTree ORDER BY ()";

        runQuery(tableDDL, createProperties);

        // Insert values representing times: 12:34:56 and 23:59:59.999
        String insertSQL = hasTimeType
                ? "INSERT INTO test_time_compat (order, time, time64, dateTime, dateTime64) VALUES "
                  + "(1, '12:34:56', '12:34:56.789', '1970-01-01 12:34:56', '1970-01-01 12:34:56.789'), "
                  + "(2, '23:59:59', '23:59:59.999', '1970-01-01 23:59:59', '1970-01-01 23:59:59.999')"
                : "INSERT INTO test_time_compat (order, dateTime, dateTime64) VALUES "
                  + "(1, '1970-01-01 12:34:56', '1970-01-01 12:34:56.789'), "
                  + "(2, '1970-01-01 23:59:59', '1970-01-01 23:59:59.999')";

        runQuery(insertSQL, createProperties);

        // Expected values for each row: [order, year, month, day, hours, minutes, seconds, milliseconds]
        // Note: month is 1-based (1 = January)
        int[][] expectedValues = {
            {1, 1970, 1, 1, 12, 34, 56, 789},
            {2, 1970, 1, 1, 23, 59, 59, 999}
        };

        Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

        // Check that all columns are readable as java.sql.Time and verify date components
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_time_compat ORDER BY order")) {
                    for (int[] expected : expectedValues) {
                        assertTrue(rs.next());
                        assertEquals(rs.getInt("order"), expected[0]);

                        // Test DateTime columns as Time (always available)
                        verifyTimeValue(rs.getTime("dateTime", utcCalendar), expected[4], expected[5], expected[6], 0, utcCalendar);
                        verifyTimeValue(rs.getTime("dateTime64", utcCalendar), expected[4], expected[5], expected[6], expected[7], utcCalendar);

                        // Verify date components for DateTime columns
                        verifyDateValue(rs.getDate("dateTime", utcCalendar), expected[1], expected[2], expected[3], utcCalendar);
                        verifyDateValue(rs.getDate("dateTime64", utcCalendar), expected[1], expected[2], expected[3], utcCalendar);

                        if (hasTimeType) {
                            // Test Time columns as Time
                            verifyTimeValue(rs.getTime("time", utcCalendar), expected[4], expected[5], expected[6], 0, utcCalendar);
                            verifyTimeValue(rs.getTime("time64", utcCalendar), expected[4], expected[5], expected[6], expected[7], utcCalendar);
                        }
                    }
                    assertFalse(rs.next());
                }
            }
        }
    }

    private static void assertNotNull(Object obj) {
        Assert.assertNotNull(obj);
    }

    private static void verifyTimeValue(Time time, int expectedHours, int expectedMinutes, 
                                       int expectedSeconds, int expectedMillis, Calendar calendar) {
        assertNotNull(time);
        calendar.setTime(time);
        assertEquals(calendar.get(Calendar.HOUR_OF_DAY), expectedHours);
        assertEquals(calendar.get(Calendar.MINUTE), expectedMinutes);
        assertEquals(calendar.get(Calendar.SECOND), expectedSeconds);
        if (expectedMillis > 0) {
            assertEquals(time.getTime() % 1000, expectedMillis);
        }
    }

    private static void verifyDateValue(Date date, int expectedYear, int expectedMonth, 
                                       int expectedDay, Calendar calendar) {
        assertNotNull(date);
        calendar.setTime(date);
        assertEquals(calendar.get(Calendar.YEAR), expectedYear);
        assertEquals(calendar.get(Calendar.MONTH) + 1, expectedMonth); // Calendar.MONTH is 0-based, convert to 1-based
        assertEquals(calendar.get(Calendar.DAY_OF_MONTH), expectedDay);
    }
}
