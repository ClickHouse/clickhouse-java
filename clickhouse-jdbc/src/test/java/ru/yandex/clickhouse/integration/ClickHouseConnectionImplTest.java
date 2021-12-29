package ru.yandex.clickhouse.integration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.sql.DataSource;

import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.JdbcIntegrationTest;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ClickHouseConnectionImplTest extends JdbcIntegrationTest {

    @Test(groups = "integration")
    public void testDefaultEmpty() throws Exception {
        assertSuccess(createDataSource(null, null));
    }

    @Test(groups = "integration")
    public void testDefaultUserOnly() throws Exception {
        assertSuccess(createDataSource("default", null));
    }

    @Test(groups = "integration")
    public void testDefaultUserEmptyPassword() throws Exception {
        assertSuccess(createDataSource("default", ""));
    }

    @Test(groups = "integration")
    public void testDefaultUserPass() throws Exception {
        assertFailure(createDataSource("default", "bar"));
    }

    @Test(groups = "integration")
    public void testDefaultPass() throws Exception {
        assertFailure(createDataSource(null, "bar"));
    }

    @Test(groups = "integration")
    public void testFooEmpty() throws Exception {
        assertFailure(createDataSource("foo", null));
    }

    @Test(groups = "integration")
    public void testFooWrongPass() throws Exception {
        assertFailure(createDataSource("foo", "baz"));
    }

    @Test(groups = "integration")
    public void testFooPass() throws Exception {
        assertSuccess(createDataSource("foo", "bar"));
    }

    @Test(groups = "integration")
    public void testFooWrongUser() throws Exception {
        assertFailure(createDataSource("baz", "bar"));
    }

    @Test(groups = "integration")
    public void testOofNoPassword() throws Exception {
        assertSuccess(createDataSource("oof", null));
    }

    @Test(groups = "integration")
    public void testOofWrongPassword() throws Exception {
        assertFailure(createDataSource("oof", "baz"));
    }

    @Test(groups = "integration")
    public void testDefaultDatabase() throws Exception {
        ClickHouseDataSource ds = newDataSource();
        String currentDbQuery = "select currentDatabase()";
        try (Connection conn = ds.getConnection(); Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery(currentDbQuery)) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "default");
                assertFalse(rs.next());
            }

            PreparedStatement p = conn.prepareStatement(currentDbQuery);
            try (ResultSet rs = p.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "default");
                assertFalse(rs.next());
            }
            s.execute("drop database if exists tdb1; drop database if exists tdb2; create database tdb1; create database tdb2");
        }

        ds = newDataSource("tdb2");
        try (Connection conn = ds.getConnection(); Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery(currentDbQuery)) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "tdb2");
                assertFalse(rs.next());
            }

            s.execute("create table tdb2_aaa(a String) engine=Memory; insert into tdb2_aaa values('3')");

            try (ResultSet rs = s.executeQuery("select currentDatabase(), a from tdb2_aaa")) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "tdb2");
                assertEquals(rs.getString(2), "3");
                assertFalse(rs.next());
            }

            s.execute("use tdb1; create table tdb1_aaa(a String) engine=Memory; insert into tdb1_aaa values('1')");

            try (ResultSet rs = s.executeQuery("select currentDatabase(), a from tdb1_aaa")) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "tdb1");
                assertEquals(rs.getString(2), "1");
                assertFalse(rs.next());
            }

            try (ResultSet rs = s.executeQuery("use `tdb2`; select currentDatabase(), a from tdb2_aaa")) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "tdb2");
                assertEquals(rs.getString(2), "3");
                assertFalse(rs.next());
            }

            String sql = "select currentDatabase(), a from tdb2_aaa";
            try (PreparedStatement p = conn.prepareStatement(sql); ResultSet rs = p.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "tdb2");
                assertEquals(rs.getString(2), "3");
                assertFalse(rs.next());
            }
        }
    }

    private static void assertSuccess(DataSource dataSource) throws Exception {
        Connection connection = dataSource.getConnection();
        assertTrue(connection.createStatement().execute("SELECT 1"));
    }

    private static void assertFailure(DataSource dataSource) throws Exception {
        // grrr, no JDK 1.8
        // assertThrows(SQLException.class, () -> dataSource.getConnection());
        try {
            dataSource.getConnection();
            fail();
        } catch (ClickHouseException e) {
            // expected
        }
    }

    private DataSource createDataSource(String user, String password) {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUser(user);
        props.setPassword(password);
        return newDataSource(props);
    }
}
