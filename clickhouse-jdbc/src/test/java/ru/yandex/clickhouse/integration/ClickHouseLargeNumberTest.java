package ru.yandex.clickhouse.integration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseContainerForTest;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import ru.yandex.clickhouse.util.ClickHouseVersionNumberUtil;

public class ClickHouseLargeNumberTest {
    private ClickHouseConnection conn;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setSessionId(UUID.randomUUID().toString());
        ClickHouseDataSource dataSource = ClickHouseContainerForTest.newDataSource(props);
        conn = dataSource.getConnection();
        try (Statement s = conn.createStatement()) {
            s.execute("SET allow_experimental_bigint_types=1");
        } catch (ClickHouseException e) {
            conn = null;
        }
    }

    @AfterTest
    public void tearDown() throws Exception {
        if (conn == null) {
            return;
        }

        try (Statement s = conn.createStatement()) {
            s.execute("SET allow_experimental_bigint_types=0");
        }
    }

    @Test
    public void testBigIntSupport() throws SQLException {
        if (conn == null || ClickHouseVersionNumberUtil.compare(conn.getServerVersion(), "21.7") >= 0) {
            return;
        }

        String testSql = "create table if not exists system.test_bigint_support(i Int256) engine=Memory;"
                + "drop table if exists system.test_bigint_support;";
        try (Connection conn = ClickHouseContainerForTest.newDataSource().getConnection();
                Statement s = conn.createStatement()) {
            s.execute("set allow_experimental_bigint_types=0;" + testSql);
            fail("Should fail without enabling bigint support");
        } catch (SQLException e) {
            assertEquals(e.getErrorCode(), 44);
        }

        try (Connection conn = ClickHouseContainerForTest.newDataSource().getConnection();
                Statement s = conn.createStatement()) {
            assertFalse(s.execute("set allow_experimental_bigint_types=1;" + testSql));
        }

        try (ClickHouseConnection conn = ClickHouseContainerForTest.newDataSource().getConnection();
                ClickHouseStatement s = conn.createStatement()) {
            Map<ClickHouseQueryParam, String> params = new EnumMap<>(ClickHouseQueryParam.class);
            params.put(ClickHouseQueryParam.ALLOW_EXPERIMENTAL_BIGINT_TYPES, "1");
            assertNull(s.executeQuery(testSql, params));

            params.put(ClickHouseQueryParam.ALLOW_EXPERIMENTAL_BIGINT_TYPES, "0");
            s.executeQuery(testSql, params);
            fail("Should fail without enabling bigint support");
        } catch (SQLException e) {
            assertEquals(e.getErrorCode(), 44);
        }
    }

    @Test
    public void testSignedIntegers() throws Exception {
        if (conn == null) {
            return;
        }

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
        if (conn == null) {
            return;
        }

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
        if (conn == null) {
            return;
        }

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
}
