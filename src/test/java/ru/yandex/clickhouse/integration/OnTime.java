package ru.yandex.clickhouse.integration;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.*;

/**
 * Here it is assumed the connection to a ClickHouse instance with flights example data it available at localhost:8123
 * For ClickHouse quickstart and example dataset see <a href="https://clickhouse.yandex/tutorial.html">https://clickhouse.yandex/tutorial.html</a>
 */
public class OnTime {

    private ClickHouseDataSource dataSource;
    private Connection connection;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @Test(enabled = false)
    public void simpleSelect() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select FlightDate, AirlineID, FlightNum from ontime limit 10");
        while (rs.next()) {
            System.out.println(rs.getDate("FlightDate") + ";" + rs.getInt("AirlineID") + ";" + rs.getString("FlightNum"));
        }
        statement.close();
    }

    @Test(enabled = false)
    public void mostTrendingDestinationTest() throws SQLException {
        String query =
            "SELECT \n" +
                "    DestCityName, \n" +
                "    sum(Year = 2014) AS c2014, \n" +
                "    sum(Year = 2015) AS c2015, \n" +
                "    c2015 / c2014 AS diff\n" +
                "FROM ontime\n" +
                "WHERE Year IN (2014, 2015)\n" +
                "GROUP BY DestCityName\n" +
                "HAVING (c2014 > 10000) AND (c2015 > 1000) AND (diff > 1)\n" +
                "ORDER BY diff DESC";

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query);
        while (rs.next()) {
            System.out.println(rs.getString("DestCityName") + ";" + rs.getBigDecimal("c2014") + ";" + rs.getBigDecimal("c2015") + ";" + rs.getFloat("diff"));
        }
        statement.close();
    }

    @Test(enabled = false)
    public void batchInsert() throws Exception {

        connection.createStatement().execute("DROP TABLE IF EXISTS test.batch_insert");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test.batch_insert (i Int32, s String) ENGINE = TinyLog"
        );

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.batch_insert (s, i) VALUES (?, ?)");

        statement.setString(1, "string1");
        statement.setInt(2, 21);
        statement.addBatch();

        statement.setString(1, "string2");
        statement.setInt(2, 32);
        statement.addBatch();

        statement.executeBatch();

        ResultSet rs = connection.createStatement().executeQuery("SELECT count() as cnt from test.batch_insert");
        rs.next();

        Assert.assertEquals(rs.getInt("cnt"), 2);

        Assert.assertFalse(rs.next());

    }

    @Test(enabled = false)
    public void batchInsert2() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.batch_insert2");
        connection.createStatement().execute(
            "CREATE TABLE test.batch_insert2 (" +
                "date Date," +
                "date_time DateTime," +
                "string String," +
                "int32 Int32," +
                "float64 Float64" +
                ") ENGINE = MergeTree(date, (date), 8192)"
        );

        Date date = new Date(602110800000L); //1989-01-30
        Timestamp dateTime = new Timestamp(1471008092000L); //2016-08-12 16:21:32
        String string = "testString";
        int int32 = Integer.MAX_VALUE;
        double float64 = 42.21;

        PreparedStatement statement = connection.prepareStatement(
            "INSERT INTO test.batch_insert2 (date, date_time, string, int32, float64) VALUES (?, ?, ?, ?, ?)"
        );

        statement.setDate(1, date);
        statement.setTimestamp(2, dateTime);
        statement.setString(3, string);
        statement.setInt(4, int32);
        statement.setDouble(5, float64);
        statement.addBatch();
        statement.executeBatch();

        ResultSet rs = connection.createStatement().executeQuery("SELECT * from test.batch_insert2");
        Assert.assertTrue(rs.next());

        Assert.assertEquals(rs.getDate("date"), date);
        Assert.assertEquals(rs.getTimestamp("date_time"), dateTime);
        Assert.assertEquals(rs.getString("string"), string);
        Assert.assertEquals(rs.getInt("int32"), int32);
        Assert.assertEquals(rs.getDouble("float64"), float64);

        Assert.assertFalse(rs.next());
    }
}
