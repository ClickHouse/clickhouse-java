package ru.yandex.clickhouse.integration;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.sql.DataSource;

import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseContainerForTest;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ClickHouseConnectionImplTest {

    @Test
    public void testDefaultEmpty() throws Exception {
        assertSuccess(createDataSource(null, null));
    }

    @Test
    public void testDefaultUserOnly() throws Exception {
        assertSuccess(createDataSource("default", null));
    }

    @Test
    public void testDefaultUserEmptyPassword() throws Exception {
        assertSuccess(createDataSource("default", ""));
    }

    @Test
    public void testDefaultUserPass() throws Exception {
        assertFailure(createDataSource("default", "bar"));
    }

    @Test
    public void testDefaultPass() throws Exception {
        assertFailure(createDataSource(null, "bar"));
    }

    @Test
    public void testFooEmpty() throws Exception {
        assertFailure(createDataSource("foo", null));
    }

    @Test
    public void testFooWrongPass() throws Exception {
        assertFailure(createDataSource("foo", "baz"));
    }

    @Test
    public void testFooPass() throws Exception {
        assertSuccess(createDataSource("foo", "bar"));
    }

    @Test
    public void testFooWrongUser() throws Exception {
        assertFailure(createDataSource("baz", "bar"));
    }

    @Test
    public void testOofNoPassword() throws Exception {
        assertSuccess(createDataSource("oof", null));
    }

    @Test
    public void testOofWrongPassword() throws Exception {
        assertFailure(createDataSource("oof", "baz"));
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

    private static DataSource createDataSource(String user, String password) {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUser(user);
        props.setPassword(password);
        return ClickHouseContainerForTest.newDataSource(props);
    }
}
