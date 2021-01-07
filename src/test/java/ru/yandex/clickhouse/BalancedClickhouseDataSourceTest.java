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


    @Test
    public void testUrlSplitValidHostName() throws Exception {
        assertEquals(Arrays.asList("jdbc:clickhouse://localhost:1234", "jdbc:clickhouse://_0another-host.com:4321"),
                BalancedClickhouseDataSource.splitUrl("jdbc:clickhouse://localhost:1234,_0another-host.com:4321"));

    }


    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUrlSplitInvalidHostName() throws Exception {
        BalancedClickhouseDataSource.splitUrl("jdbc:clickhouse://localhost:1234,_0ano^ther-host.com:4321");

    }


    @BeforeTest
    public void setUp() throws Exception {
        dataSource = ClickHouseContainerForTest.newBalancedDataSource();
        String address = ClickHouseContainerForTest.getClickHouseHttpAddress();
        doubleDataSource = ClickHouseContainerForTest.newBalancedDataSource(address, address);
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
        BalancedClickhouseDataSource badDatasource = ClickHouseContainerForTest.newBalancedDataSource("not.existed.url:8123");
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
        BalancedClickhouseDataSource halfDatasource = ClickHouseContainerForTest.newBalancedDataSource("not.existed.url:8123", ClickHouseContainerForTest.getClickHouseHttpAddress());

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

    @Test
    public void testConstructWithClickHouseProperties() {
        final ClickHouseProperties properties = new ClickHouseProperties();
        properties.setMaxThreads(3);
        properties.setSocketTimeout(67890);
        properties.setPassword("888888");
        //without connection parameters
        String hostAddr = ClickHouseContainerForTest.getClickHouseHttpAddress();
        String ipAddr   = ClickHouseContainerForTest.getClickHouseHttpAddress(true);
        BalancedClickhouseDataSource dataSource = ClickHouseContainerForTest.newBalancedDataSourceWithSuffix(
            "click", properties, hostAddr, ipAddr);
        ClickHouseProperties dataSourceProperties = dataSource.getProperties();
        assertEquals(dataSourceProperties.getMaxThreads().intValue(), 3);
        assertEquals(dataSourceProperties.getSocketTimeout(), 67890);
        assertEquals(dataSourceProperties.getPassword(), "888888");
        assertEquals(dataSourceProperties.getDatabase(), "click");
        assertEquals(2, dataSource.getAllClickhouseUrls().size());
        assertEquals(dataSource.getAllClickhouseUrls().get(0), "jdbc:clickhouse://" + hostAddr + "/click");
        assertEquals(dataSource.getAllClickhouseUrls().get(1), "jdbc:clickhouse://" + ipAddr + "/click");
        // with connection parameters
        dataSource = ClickHouseContainerForTest.newBalancedDataSourceWithSuffix(
                "click?socket_timeout=12345&user=readonly", properties, hostAddr, ipAddr);
        dataSourceProperties = dataSource.getProperties();
        assertEquals(dataSourceProperties.getMaxThreads().intValue(), 3);
        assertEquals(dataSourceProperties.getSocketTimeout(), 12345);
        assertEquals(dataSourceProperties.getUser(), "readonly");
        assertEquals(dataSourceProperties.getPassword(), "888888");
        assertEquals(dataSourceProperties.getDatabase(), "click");
        assertEquals(2, dataSource.getAllClickhouseUrls().size());
        assertEquals(dataSource.getAllClickhouseUrls().get(0), "jdbc:clickhouse://" + hostAddr + "/click?socket_timeout" +
                "=12345&user=readonly");
        assertEquals(dataSource.getAllClickhouseUrls().get(1), "jdbc:clickhouse://" + ipAddr + "/click?socket_timeout=12345&user=readonly");
    }

}
