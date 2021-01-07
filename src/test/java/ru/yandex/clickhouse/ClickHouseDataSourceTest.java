package ru.yandex.clickhouse;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.time.Duration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import static java.time.temporal.ChronoUnit.SECONDS;

public class ClickHouseDataSourceTest {
    private static final GenericContainer<?> chServer;

    static {
        String clickhouseVersion = System.getProperty("clickhouseVersion");
        if (clickhouseVersion == null || (clickhouseVersion = clickhouseVersion.trim()).isEmpty()) {
            clickhouseVersion = "latest";
        }

        chServer = new GenericContainer<>("yandex/clickhouse-server:" + clickhouseVersion)
                .waitingFor(Wait.forHttp("/ping").forStatusCode(200).withStartupTimeout(Duration.of(60, SECONDS)));
    }

    @BeforeSuite(groups = { "sit" })
    public void beforeSuite() {
        chServer.start();
    }

    @AfterSuite(groups = { "sit" })
    public void afterSuite() {
        chServer.stop();
    }

    @Test
    public void testConstructor() throws Exception {
        ClickHouseDataSource ds = new ClickHouseDataSource("jdbc:clickhouse://localhost:1234/ppc");
        assertEquals("localhost", ds.getHost());
        assertEquals(1234, ds.getPort());
        assertEquals("ppc", ds.getDatabase());

        ClickHouseDataSource ds2 = new ClickHouseDataSource("jdbc:clickhouse://clh.company.com:5324");
        assertEquals("clh.company.com", ds2.getHost());
        assertEquals(5324, ds2.getPort());
        assertEquals("default", ds2.getDatabase());

        try {
            new ClickHouseDataSource(null);
            fail("ClickHouseDataSource with null argument must fail");
        } catch (IllegalArgumentException ex) {
            // pass, it's ok
        }

        try {
            new ClickHouseDataSource("jdbc:mysql://localhost:2342");
            fail("ClickHouseDataSource with incorrect args must fail");
        } catch (IllegalArgumentException ex) {
            // pass, it's ok
        }

        try {
            new ClickHouseDataSource("jdbc:clickhouse://localhost:wer");
            fail("ClickHouseDataSource with incorrect args must fail");
        } catch (IllegalArgumentException ex) {
            // pass, it's ok
        }
    }

    @Test
    public void testIPv6Constructor() throws Exception {
        ClickHouseDataSource ds = new ClickHouseDataSource("jdbc:clickhouse://[::1]:5324");
        assertEquals(ds.getHost(), "[::1]");
        assertEquals(ds.getPort(), 5324);
        assertEquals(ds.getDatabase(), "default");

        ClickHouseDataSource ds2 = new ClickHouseDataSource("jdbc:clickhouse://[::FFFF:129.144.52.38]:5324");
        assertEquals(ds2.getHost(), "[::FFFF:129.144.52.38]");
        assertEquals(ds2.getPort(), 5324);
        assertEquals(ds2.getDatabase(), "default");
    }

}
