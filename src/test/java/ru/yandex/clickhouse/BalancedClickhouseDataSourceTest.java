package ru.yandex.clickhouse;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class BalancedClickhouseDataSourceTest {

    private BalancedClickhouseDataSource dataSource;
    private BalancedClickhouseDataSource doubleDataSource;

    @Test
    public void testUrlSplit() throws Exception {
        assertEquals(Arrays.asList("jdbc:clickhouse://localhost:1234/ppc"),
                BalancedClickhouseDataSource.splitUrl("jdbc:clickhouse://localhost:1234/ppc"));

        assertEquals(Arrays.asList("jdbc:clickhouse://localhost:1234/ppc",
                "jdbc:clickhouse://another.host.com:4321/ppc"),
                BalancedClickhouseDataSource.splitUrl("jdbc:clickhouse://localhost:1234,another.host.com:4321/ppc"));

        assertEquals(Arrays.asList("jdbc:clickhouse://localhost:1234", "jdbc:clickhouse://another.host.com:4321"),
                BalancedClickhouseDataSource.splitUrl("jdbc:clickhouse://localhost:1234,another.host.com:4321"));

    }


    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        doubleDataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://localhost:8123,localhost:8123", properties);
    }


    @Test
    public void testSingleDatabaseConnection() throws Exception {
        Connection connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");

        connection.createStatement().execute("DROP TABLE IF EXISTS test.insert_test");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.insert_test (i Int32, s String) ENGINE = TinyLog"
        );
        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.insert_test (s, i) VALUES (?, ?)");
        statement.setString(1, "asd");
        statement.setInt(2, 42);
        statement.execute();


        ResultSet rs = connection.createStatement().executeQuery("SELECT * from test.insert_test");
        rs.next();

        assertEquals("asd", rs.getString("s"));
        assertEquals(42, rs.getInt("i"));
    }


    @Test
    public void testDoubleDatabaseConnection() throws Exception {
        Connection connection = doubleDataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
        connection = doubleDataSource.getConnection();
        connection.createStatement().execute("DROP TABLE IF EXISTS test.insert_test");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.insert_test (i Int32, s String) ENGINE = TinyLog"
        );

        connection = doubleDataSource.getConnection();

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.insert_test (s, i) VALUES (?, ?)");
        statement.setString(1, "asd");
        statement.setInt(2, 42);
        statement.execute();

        ResultSet rs = connection.createStatement().executeQuery("SELECT * from test.insert_test");
        rs.next();

        assertEquals("asd", rs.getString("s"));
        assertEquals(42, rs.getInt("i"));

        connection = doubleDataSource.getConnection();

        statement = connection.prepareStatement("INSERT INTO test.insert_test (s, i) VALUES (?, ?)");
        statement.setString(1, "asd");
        statement.setInt(2, 42);
        statement.execute();

        rs = connection.createStatement().executeQuery("SELECT * from test.insert_test");
        rs.next();

        assertEquals("asd", rs.getString("s"));
        assertEquals(42, rs.getInt("i"));

    }


    @Test
    public void testCorrectActualizationDatabaseConnection() throws Exception {
        dataSource.actualize();
        Connection connection = dataSource.getConnection();
    }


    @Test
    public void testDisableConnection() throws Exception {
        BalancedClickhouseDataSource badDatasource = new BalancedClickhouseDataSource("jdbc:clickhouse://not.existed.url:8123", new ClickHouseProperties());
        badDatasource.actualize();
        try {
            Connection connection = badDatasource.getConnection();
            fail();
        } catch (Exception e) {
            // There is no enabled connections
        }
    }


    @Test
    public void testWorkWithEnabledUrl() throws Exception {
        BalancedClickhouseDataSource halfDatasource = new BalancedClickhouseDataSource("jdbc:clickhouse://not.existed.url:8123,localhost:8123", new ClickHouseProperties());

        halfDatasource.actualize();
        Connection connection = halfDatasource.getConnection();

        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
        connection = halfDatasource.getConnection();
        connection.createStatement().execute("DROP TABLE IF EXISTS test.insert_test");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.insert_test (i Int32, s String) ENGINE = TinyLog"
        );

        connection = halfDatasource.getConnection();

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.insert_test (s, i) VALUES (?, ?)");
        statement.setString(1, "asd");
        statement.setInt(2, 42);
        statement.execute();

        ResultSet rs = connection.createStatement().executeQuery("SELECT * from test.insert_test");
        rs.next();

        assertEquals("asd", rs.getString("s"));
        assertEquals(42, rs.getInt("i"));

        connection = halfDatasource.getConnection();

        statement = connection.prepareStatement("INSERT INTO test.insert_test (s, i) VALUES (?, ?)");
        statement.setString(1, "asd");
        statement.setInt(2, 42);
        statement.execute();

        rs = connection.createStatement().executeQuery("SELECT * from test.insert_test");
        rs.next();

        assertEquals("asd", rs.getString("s"));
        assertEquals(42, rs.getInt("i"));
    }

}
