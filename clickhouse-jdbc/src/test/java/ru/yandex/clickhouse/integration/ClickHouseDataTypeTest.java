package ru.yandex.clickhouse.integration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseContainerForTest;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.except.ClickHouseException;

public class ClickHouseDataTypeTest {
    private Connection conn;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseDataSource dataSource = ClickHouseContainerForTest.newDataSource();
        conn = dataSource.getConnection();
    }

    @AfterTest
    public void tearDown() throws Exception {
    }

    @Test
    public void testDateTimes() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS test_datetimes");
            s.execute(
                    "CREATE TABLE IF NOT EXISTS test_datetimes(d DateTime, d32 DateTime32, d64 DateTime64(9)) ENGINE = Memory");
        } catch (ClickHouseException e) {
            return;
        }

        Timestamp expected = new Timestamp(1614881594000L);
        expected.setNanos(123456789);

        try (Statement s = conn.createStatement()) {
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
}
