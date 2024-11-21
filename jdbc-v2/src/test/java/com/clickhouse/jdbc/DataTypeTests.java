package com.clickhouse.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DataTypeTests extends JdbcIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(DataTypeTests.class);

    @BeforeTest
    public void setUp() throws SQLException {
        DriverManager.registerDriver(new Driver());
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(getEndpointString(isCloud()));
    }

    private int insertData(String sql) throws SQLException {
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                return stmt.executeUpdate(sql);
            }
        }
    }

    @Test
    public void testIntegerTypesSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_integers (order Int8, "
                + "int8 Int8, int16 Int16, int32 Int32, int64 Int64, int128 Int128, int256 Int256, "
                + "uint8 UInt8, uint16 UInt16, uint32 UInt32, uint64 UInt64, uint128 UInt128, uint256 UInt256"
                + ") ENGINE = Memory");

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
        int uint8 = rand.nextInt(256);
        int uint16 = rand.nextInt(65536);
        long uint32 = rand.nextInt() & 0xFFFFFFFFL;
        BigInteger uint64 = BigInteger.valueOf(rand.nextLong(Long.MAX_VALUE));
        BigInteger uint128 = new BigInteger(128, rand);
        BigInteger uint256 = new BigInteger(256, rand);

        String sql = String.format("INSERT INTO test_integers VALUES ( 3, %d, %d, %d, %d, %s, %s, %d, %d, %d, %d, %s, %s)",
                int8, int16, int32, int64, int128, int256, uint8, uint16, uint32, uint64, uint128, uint256);
        insertData(sql);

        // Check the results
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_integers ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getByte("int8"), -128);
                    assertEquals(rs.getShort("int16"), -32768);
                    assertEquals(rs.getInt("int32"), -2147483648);
                    assertEquals(rs.getLong("int64"), -9223372036854775808L);
                    assertEquals(rs.getBigDecimal("int128"), new BigDecimal("-170141183460469231731687303715884105728"));
                    assertEquals(rs.getBigDecimal("int256"), new BigDecimal("-57896044618658097711785492504343953926634992332820282019728792003956564819968"));
                    assertEquals(rs.getShort("uint8"), 0);
                    assertEquals(rs.getInt("uint16"), 0);
                    assertEquals(rs.getLong("uint32"), 0);
                    assertEquals(rs.getBigDecimal("uint64"), new BigDecimal("0"));
                    assertEquals(rs.getBigDecimal("uint128"), new BigDecimal("0"));
                    assertEquals(rs.getBigDecimal("uint256"), new BigDecimal("0"));

                    assertTrue(rs.next());
                    assertEquals(rs.getByte("int8"), 127);
                    assertEquals(rs.getShort("int16"), 32767);
                    assertEquals(rs.getInt("int32"), 2147483647);
                    assertEquals(rs.getLong("int64"), 9223372036854775807L);
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
    }

    @Test
    public void testDecimalTypesSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_decimals (order Int8, "
                + "dec Decimal(9, 2), dec32 Decimal32(4), dec64 Decimal64(8), dec128 Decimal128(18), dec256 Decimal256(18)"
                + ") ENGINE = Memory");

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

        String sql = String.format("INSERT INTO test_decimals VALUES ( 3, %s, %s, %s, %s, %s)",
                dec, dec32, dec64, dec128, dec256);
        insertData(sql);

        // Check the results
        try (Connection conn = getConnection()) {
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
    }

    @Test
    public void testDateTypesSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_dates (order Int8, "
                + "date Date, date32 Date32, " +
                "dateTime DateTime, dateTime32 DateTime32, " +
                "dateTime643 DateTime64(3), dateTime646 DateTime64(6), dateTime649 DateTime64(9)"
                + ") ENGINE = Memory");

        // Insert minimum values
        insertData("INSERT INTO test_dates VALUES ( 1, '1970-01-01', '1970-01-01', " +
                "'1970-01-01 00:00:00', '1970-01-01 00:00:00', " +
                "'1970-01-01 00:00:00.000', '1970-01-01 00:00:00.000000', '1970-01-01 00:00:00.000000000' )");

        // Insert maximum values
        insertData("INSERT INTO test_dates VALUES ( 2, '2149-06-06', '2299-12-31', " +
                "'2106-02-07 06:28:15', '2106-02-07 06:28:15', " +
                "'2261-12-31 23:59:59.999', '2261-12-31 23:59:59.999999', '2261-12-31 23:59:59.999999999' )");

        // Insert random (valid) values
        long now = System.currentTimeMillis();
        log.info("Random seed was: {}", now);

        Date date = new Date(now);
        Date date32 = new Date(now);
        java.sql.Timestamp dateTime = new java.sql.Timestamp(now);
        dateTime.setNanos(0);
        java.sql.Timestamp dateTime32 = new java.sql.Timestamp(now);
        dateTime32.setNanos(0);
        java.sql.Timestamp dateTime643 = new java.sql.Timestamp(now);
        java.sql.Timestamp dateTime646 = new java.sql.Timestamp(now);
        java.sql.Timestamp dateTime649 = new java.sql.Timestamp(now);

        String sql = String.format("INSERT INTO test_dates VALUES ( 3, '%s', '%s', '%s', '%s', '%s', '%s', '%s' )",
                date, date32, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dateTime), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dateTime32), dateTime643, dateTime646, dateTime649);
        insertData(sql);

        // Check the results
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_dates ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getDate("date"), Date.valueOf("1970-01-01"));
                    assertEquals(rs.getDate("date32"), Date.valueOf("1970-01-01"));
                    assertEquals(rs.getTimestamp("dateTime"), new java.sql.Timestamp(Date.valueOf("1970-01-01").getTime()));
                    assertEquals(rs.getTimestamp("dateTime32"), new java.sql.Timestamp(Date.valueOf("1970-01-01").getTime()));
                    assertEquals(rs.getTimestamp("dateTime643"), new java.sql.Timestamp(Date.valueOf("1970-01-01").getTime()));
                    assertEquals(rs.getTimestamp("dateTime646"), new java.sql.Timestamp(Date.valueOf("1970-01-01").getTime()));
                    assertEquals(rs.getTimestamp("dateTime649"), new java.sql.Timestamp(Date.valueOf("1970-01-01").getTime()));

                    assertTrue(rs.next());
                    assertEquals(rs.getDate("date"), Date.valueOf("2149-06-06"));
                    assertEquals(rs.getDate("date32"), Date.valueOf("2299-12-31"));
                    assertEquals(rs.getTimestamp("dateTime"), java.sql.Timestamp.valueOf("2106-02-07 06:28:15"));
                    assertEquals(rs.getTimestamp("dateTime32"), java.sql.Timestamp.valueOf("2106-02-07 06:28:15"));
                    assertEquals(rs.getTimestamp("dateTime643"), java.sql.Timestamp.valueOf("2261-12-31 23:59:59.999"));
                    assertEquals(rs.getTimestamp("dateTime646"), java.sql.Timestamp.valueOf("2261-12-31 23:59:59.999999"));
                    assertEquals(rs.getTimestamp("dateTime649"), java.sql.Timestamp.valueOf("2261-12-31 23:59:59.999999999"));

                    assertTrue(rs.next());
                    assertEquals(rs.getDate("date").toLocalDate(), date.toLocalDate());
                    assertEquals(rs.getDate("date32").toLocalDate(), date32.toLocalDate());
                    assertEquals(rs.getTimestamp("dateTime"), dateTime);
                    assertEquals(rs.getTimestamp("dateTime32"), dateTime32);
                    assertEquals(rs.getTimestamp("dateTime643"), dateTime643);
                    assertEquals(rs.getTimestamp("dateTime646"), dateTime646);
                    assertEquals(rs.getTimestamp("dateTime649"), dateTime649);

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testStringTypesSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_strings (order Int8, "
                + "str String, fixed FixedString(6), "
                + "enum Enum8('a' = 6, 'b' = 7, 'c' = 8), enum8 Enum8('a' = 1, 'b' = 2, 'c' = 3), enum16 Enum16('a' = 1, 'b' = 2, 'c' = 3), "
                + "uuid UUID, ipv4 IPv4, ipv6 IPv6"
                + ") ENGINE = Memory");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        String str = "string" + rand.nextInt(1000);
        String fixed = "fixed" + rand.nextInt(10);
        String enum8 = "a";
        String enum16 = "b";
        String uuid = UUID.randomUUID().toString();
        String ipv4 = rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256);
        String ipv6 = "2001:adb8:85a3:1:2:8a2e:370:7334";

        String sql = String.format("INSERT INTO test_strings VALUES ( 1, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s' )",
                str, fixed, enum8, enum8, enum16, uuid, ipv4, ipv6);
        insertData(sql);

        // Check the results
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_strings ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString("str"), str);
                    assertEquals(rs.getString("fixed"), fixed);
                    assertEquals(rs.getString("enum"), "6");
                    assertEquals(rs.getString("enum8"), "1");
                    assertEquals(rs.getString("enum16"), "2");
                    assertEquals(rs.getString("uuid"), uuid);
                    assertEquals(rs.getString("ipv4"), "/" + ipv4);
                    assertEquals(rs.getString("ipv6"), "/" + ipv6);

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testFloatTypesSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_floats (order Int8, "
                + "float32 Float32, float64 Float64"
                + ") ENGINE = Memory");

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

        String sql = String.format("INSERT INTO test_floats VALUES ( 3, %s, %s )",
                float32, float64);
        insertData(sql);

        // Check the results
        try (Connection conn = getConnection()) {
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
    }

    @Test
    public void testBooleanTypesSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_booleans (order Int8, "
                + "bool Boolean"
                + ") ENGINE = Memory");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        boolean bool = rand.nextBoolean();

        String sql = String.format("INSERT INTO test_booleans VALUES ( 1, %s )",
                bool);
        insertData(sql);

        // Check the results
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_booleans ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getBoolean("bool"), bool);

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testArrayTypesSimpleStatement () throws SQLException {
        runQuery("CREATE TABLE test_arrays (order Int8, "
                + "array Array(Int8), arraystr Array(String)"
                + ") ENGINE = Memory");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        int[] array = new int[rand.nextInt(10)];
        for (int i = 0; i < array.length; i++) {
            array[i] = rand.nextInt(256) - 128;
        }

        String[] arraystr = new String[rand.nextInt(10)];
        for (int i = 0; i < arraystr.length; i++) {
            arraystr[i] = "string" + rand.nextInt(1000);
        }

        // Insert random (valid) values
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO test_arrays VALUES ( 1, [");
        for (int i = 0; i < array.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(array[i]);
        }
        sb.append("], [");
        for (int i = 0; i < arraystr.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("'");
            sb.append(arraystr[i]);
            sb.append("'");
        }
        sb.append("])");
        String sql = sb.toString();
        insertData(sql);

        // Check the results
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_arrays ORDER BY order")) {
                    assertTrue(rs.next());
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

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testMapTypesSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_maps (order Int8, "
                + "map Map(String, Int8), mapstr Map(String, String)"
                + ") ENGINE = Memory");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        int mapSize = rand.nextInt(10);
        String[] keys = new String[mapSize];
        int[] values = new int[mapSize];
        for (int i = 0; i < mapSize; i++) {
            keys[i] = "key" + i;
            values[i] = rand.nextInt(256) - 128;
        }

        String[] keysstr = new String[mapSize];
        String[] valuesstr = new String[mapSize];
        for (int i = 0; i < mapSize; i++) {
            keysstr[i] = "key" + i;
            valuesstr[i] = "string" + rand.nextInt(1000);
        }

        // Insert random (valid) values
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO test_maps VALUES ( 1, ");
        sb.append("{");
        for (int i = 0; i < mapSize; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("'");
            sb.append(keys[i]);
            sb.append("': ");
            sb.append(values[i]);
        }
        sb.append("}, ");
        sb.append("{");
        for (int i = 0; i < mapSize; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("'");
            sb.append(keysstr[i]);
            sb.append("': '");
            sb.append(valuesstr[i]);
            sb.append("'");
        }
        sb.append("}");
        sb.append(")");
        String sql = sb.toString();
        insertData(sql);

        // Check the results
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_maps ORDER BY order")) {
                    assertTrue(rs.next());
                    Map<Object, Object> mapResult = (Map<Object, Object>) rs.getObject("map");
                    assertEquals(mapResult.size(), mapSize);
                    for (int i = 0; i < mapSize; i++) {
                        assertEquals(String.valueOf(mapResult.get(keys[i])), String.valueOf(values[i]));
                    }

                    Map<Object, Object> mapstrResult = (Map<Object, Object>) rs.getObject("mapstr");
                    assertEquals(mapstrResult.size(), mapSize);
                    for (int i = 0; i < mapSize; i++) {
                        assertEquals(mapstrResult.get(keysstr[i]), valuesstr[i]);
                    }
                }
            }
        }
    }

    @Test
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
                + ") ENGINE = Memory");

        // Insert null values
        insertData("INSERT INTO test_nullable VALUES ( 1, "
                + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
                + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
                + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
                + "NULL, NULL, NULL, NULL)");

        //Check the results
        try (Connection conn = getConnection()) {
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

    @Test
    public void testLowCardinalityTypeSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_low_cardinality (order Int8, "
                + "lowcardinality LowCardinality(String)"
                + ") ENGINE = Memory");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        String lowcardinality = "string" + rand.nextInt(1000);

        insertData(String.format("INSERT INTO test_low_cardinality VALUES ( 1, '%s' )",
                lowcardinality));

        // Check the results
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_low_cardinality ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString("lowcardinality"), lowcardinality);

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testSimpleAggregateFunction() throws SQLException {
        runQuery("CREATE TABLE test_aggregate (order Int8, "
                + "int8 Int8"
                + ") ENGINE = Memory");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        int int8 = rand.nextInt(256) - 128;

        insertData(String.format("INSERT INTO test_aggregate VALUES ( 1, %d )", int8));
        insertData(String.format("INSERT INTO test_aggregate VALUES ( 2, %d )", int8));
        insertData(String.format("INSERT INTO test_aggregate VALUES ( 3, %d )", int8));

        // Check the results
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT sum(int8) FROM test_aggregate")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), int8 * 3);
                }
            }
        }
    }

    @Test
    public void testNestedTypeSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_nested (order Int8, "
                + "nested Nested (int8 Int8, int16 Int16, int32 Int32, int64 Int64, int128 Int128, int256 Int256)"
                + ") ENGINE = Memory");

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
        try (Connection conn = getConnection()) {
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

    @Test
    public void testTupleTypeSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_tuple (order Int8, "
                + "tuple Tuple(int8 Int8, int16 Int16, int32 Int32, int64 Int64, int128 Int128, int256 Int256)"
                + ") ENGINE = Memory");

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
        try (Connection conn = getConnection()) {
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



    @Test (enabled = false)//TODO: This type is experimental right now
    public void testJSONTypeSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_json (order Int8, "
                + "json JSON"
                + ") ENGINE = Memory");

        // Insert random (valid) values
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        log.info("Random seed was: {}", seed);

        String json = "{\"key1\": \"" + rand.nextDouble() + "\", \"key2\": " + rand.nextInt() + ", \"key3\": [\"value3\", 4]}";
        insertData(String.format("INSERT INTO test_json VALUES ( 1, '%s' )", json));

        // Check the results
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_json ORDER BY order")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString("json"), json);

                    assertFalse(rs.next());
                }
            }
        }
    }



    @Test (enabled = false)//TODO: The client doesn't support all of these yet
    public void testGeometricTypesSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_geometric (order Int8, "
                + "point Point, ring Ring, linestring LineString, multilinestring MultiLineString, polygon Polygon, multipolygon MultiPolygon"
                + ") ENGINE = Memory");

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
        try (Connection conn = getConnection()) {
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


    @Test (enabled = false)//TODO: This type is experimental right now
    public void testDynamicTypesSimpleStatement() throws SQLException {
        runQuery("CREATE TABLE test_dynamic (order Int8, "
                + "dynamic Dynamic"
                + ") ENGINE = Memory");

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
        try (Connection conn = getConnection()) {
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
}
