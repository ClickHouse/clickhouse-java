package ru.yandex.clickhouse.integration;

import static org.junit.Assert.assertArrayEquals;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseContainerForTest;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.Utils;

public class ClickHouseDataTypeTest {
    private Connection conn;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setSessionId(UUID.randomUUID().toString());
        ClickHouseDataSource dataSource = ClickHouseContainerForTest.newDataSource(props);
        conn = dataSource.getConnection();
        try (Statement s = conn.createStatement()) {
            s.execute("SET allow_experimental_bigint_types=1");
            s.execute("SET allow_experimental_map_type=1");
        }
    }

    @AfterTest
    public void tearDown() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("SET allow_experimental_bigint_types=0");
        }
    }

    @Test
    public void testSignedIntegers() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS test_signed_integers");
            s.execute("CREATE TABLE IF NOT EXISTS test_signed_integers(i128 Int128, i256 Int256) ENGINE = TinyLog");
            s.execute(
                    "insert into test_signed_integers values(0, 0), (-170141183460469231731687303715884105728,-57896044618658097711785492504343953926634992332820282019728792003956564819968), (170141183460469231731687303715884105727,57896044618658097711785492504343953926634992332820282019728792003956564819967)");
            // check Int128
            try (ResultSet rs = s.executeQuery("select min(i128) as a, max(i128) as b from test_signed_integers")) {
                assertTrue(rs.next());
                assertEquals(new BigInteger("-170141183460469231731687303715884105728"), rs.getObject("a"));
                assertEquals(new BigInteger("170141183460469231731687303715884105727"), rs.getObject("b"));
            }

            // check Int256
            try (ResultSet rs = s.executeQuery("select min(i256) as a, max(i256) as b from test_signed_integers")) {
                assertTrue(rs.next());
                assertEquals(
                        new BigInteger(
                                "-57896044618658097711785492504343953926634992332820282019728792003956564819968"),
                        rs.getObject("a"));
                assertEquals(
                        new BigInteger("57896044618658097711785492504343953926634992332820282019728792003956564819967"),
                        rs.getObject("b"));
            }
        }
    }

    @Test
    public void testUnsignedIntegers() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS test_unsigned_integers");
            s.execute("CREATE TABLE IF NOT EXISTS test_unsigned_integers(i256 UInt256) ENGINE = TinyLog");
            s.execute(
                    "insert into test_unsigned_integers values (1), (0), (115792089237316195423570985008687907853269984665640564039457584007913129639935)");
            // TODO: add check for UInt128 once it's supported

            // check UInt256
            try (ResultSet rs = s.executeQuery("select min(i256) as a, max(i256) as b from test_unsigned_integers")) {
                assertTrue(rs.next());
                assertEquals(BigInteger.ZERO, rs.getObject("a"));
                assertEquals(
                        new BigInteger(
                                "115792089237316195423570985008687907853269984665640564039457584007913129639935"),
                        rs.getObject("b"));
            }
        }
    }

    @Test
    public void testDecimal256() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS test_decimal256");
            s.execute(
                    "CREATE TABLE IF NOT EXISTS test_decimal256(d256 Decimal256(0), d Decimal256(20)) ENGINE = Memory");
            s.execute(
                    "insert into test_decimal256 values (-123456789.123456789, -123456789.123456789), (0, 0), (123456789.123456789, 123456789.123456789)");

            // check min scale
            try (ResultSet rs = s.executeQuery("select min(d256) as a, max(d256) as b from test_decimal256")) {
                assertTrue(rs.next());
                assertEquals(new BigDecimal("-123456789"), rs.getObject("a"));
                assertEquals(new BigDecimal("123456789"), rs.getObject("b"));
            }

            // check max scale
            try (ResultSet rs = s.executeQuery("select d from test_decimal256 order by d")) {
                assertTrue(rs.next());
                assertEquals(new BigDecimal("-123456789.123456789").setScale(20), rs.getObject("d"));

                assertTrue(rs.next());
                assertTrue(rs.next());
                assertEquals(new BigDecimal("123456789.123456789").setScale(20), rs.getObject("d"));
            }
        }
    }

    @Test
    public void testDateTimes() throws Exception {
        Timestamp expected = new Timestamp(1614881594000L);
        expected.setNanos(123456789);

        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS test_datetimes");
            s.execute(
                    "CREATE TABLE IF NOT EXISTS test_datetimes(d DateTime, d32 DateTime32, d64 DateTime64(9)) ENGINE = Memory");
            s.execute(
                    "insert into test_datetimes values ('2021-03-04 18:13:14', '2021-03-04 18:13:14', '2021-03-04 18:13:14.123456789')");

            try (ResultSet rs = s.executeQuery("select * from test_datetimes")) {
                assertTrue(rs.next());
                assertEquals(new Timestamp(1614881594000L), rs.getObject("d"));
                assertEquals(new Timestamp(1614881594000L), rs.getObject("d32"));
                assertEquals(expected, rs.getObject("d64"));
            }

            s.execute("truncate table test_datetimes");
        }

        try (PreparedStatement s = conn.prepareStatement("insert into test_datetimes values(?,?,?)")) {
            s.setString(1, "2021-03-01 18:13:14");
            s.setString(2, "2021-03-01 18:13:14");
            s.setString(3, "2021-03-01 18:13:14.123456789");
            s.execute();

            s.setObject(1, new Timestamp(expected.getTime()));
            s.setObject(2, new Timestamp(expected.getTime()));
            s.setObject(3, expected);
            s.execute();
        }

        try (Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery("select * from test_datetimes order by d")) {
                assertTrue(rs.next());
                Timestamp ts = new Timestamp(expected.getTime() - 3 * 24 * 3600000);
                ts.setNanos(expected.getNanos());
                assertEquals(new Timestamp(ts.getTime() - ts.getNanos() / 1000000), rs.getObject("d"));
                assertEquals(new Timestamp(ts.getTime() - ts.getNanos() / 1000000), rs.getObject("d32"));
                assertEquals(ts, rs.getObject("d64"));

                assertTrue(rs.next());
                assertEquals(new Timestamp(expected.getTime() - expected.getNanos() / 1000000), rs.getObject("d"));
                assertEquals(new Timestamp(expected.getTime() - expected.getNanos() / 1000000), rs.getObject("d32"));
                assertEquals(expected, rs.getObject("d64"));

                long ms = Duration.ofDays(1L).toMillis();
                assertEquals(new Time(expected.getTime() % ms - expected.getNanos() / 1000000), rs.getTime("d"));
                assertEquals(new Time(expected.getTime() % ms - expected.getNanos() / 1000000), rs.getTime("d32"));
                assertEquals(new Time(expected.getTime() % ms), rs.getTime("d64"));
            }
        }
    }

    @Test
    public void testIPs() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS test_ips");
            s.execute("CREATE TABLE IF NOT EXISTS test_ips(ip4 IPv4, ip6 IPv6) ENGINE = Memory");
            s.execute("insert into test_ips values ('0.0.0.0', '::')");

            try (ResultSet rs = s.executeQuery("select * from test_ips")) {
                assertTrue(rs.next());
                assertEquals("0.0.0.0", rs.getObject("ip4"));
                assertEquals("::", rs.getObject("ip6"));
            }

            s.execute("truncate table test_ips");
        }

        try (PreparedStatement s = conn.prepareStatement("insert into test_ips values(?,?)")) {
            s.setString(1, "0.0.0.0");
            s.setString(2, "::");
            s.execute();

            s.setObject(1, 16909060);
            s.setObject(2, "2607:f8b0:4005:805::2004");
            s.execute();
        }

        try (Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery("select * from test_ips order by ip4")) {
                assertTrue(rs.next());
                assertEquals("0.0.0.0", rs.getObject("ip4"));
                assertEquals("::", rs.getObject("ip6"));

                assertTrue(rs.next());
                assertEquals("1.2.3.4", rs.getObject("ip4"));
                assertEquals("2607:f8b0:4005:805::2004", rs.getObject("ip6"));
            }
        }
    }

    private void compareMap(Object actual, Object expected) {
        Map<?, ?> m1 = (Map<?, ?>) actual;
        Map<?, ?> m2 = (Map<?, ?>) expected;
        assertEquals(m1.size(), m2.size());
        for (Map.Entry<?, ?> e : m1.entrySet()) {
            if (e.getValue().getClass().isArray()) {
                assertArrayEquals((Object[]) e.getValue(), (Object[]) m2.get(e.getKey()));
            } else {
                assertEquals(e.getValue(), m2.get(e.getKey()));
            }
        }
    }

    @Test
    public void testMaps() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS test_maps");
            s.execute(
                    "CREATE TABLE IF NOT EXISTS test_maps(ma Map(Integer, Array(String)), mi Map(Integer, Integer), ms Map(String, String)) ENGINE = Memory");
            s.execute("insert into test_maps values ({1:['11','12'],2:['22','23']},{1:11,2:22},{'k1':'v1','k2':'v2'})");

            try (ResultSet rs = s.executeQuery("select * from test_maps")) {
                assertTrue(rs.next());
                compareMap(rs.getObject("ma"),
                        Utils.mapOf(1, new String[] { "11", "12" }, 2, new String[] { "22", "23" }));
                compareMap(rs.getObject("mi"), Utils.mapOf(1, 11, 2, 22));
                compareMap(rs.getObject("ms"), Utils.mapOf("k1", "v1", "k2", "v2"));
            }

            s.execute("truncate table test_maps");
        }

        try (PreparedStatement s = conn.prepareStatement("insert into test_maps values(?,?,?)")) {
            s.setObject(1, Utils.mapOf(1, new String[] { "11", "12" }, 2, new String[] { "22", "23" }));
            s.setObject(2, Utils.mapOf(1, 11, 2, 22));
            s.setObject(3, Utils.mapOf("k1", "v1", "k2", "v2"));
            s.execute();
        }

        try (Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery("select * from test_maps")) {
                assertTrue(rs.next());
                compareMap(rs.getObject("ma"),
                        Utils.mapOf(1, new String[] { "11", "12" }, 2, new String[] { "22", "23" }));
                compareMap(rs.getObject("mi"), Utils.mapOf(1, 11, 2, 22));
                compareMap(rs.getObject("ms"), Utils.mapOf("k1", "v1", "k2", "v2"));
            }
        }
    }
}
