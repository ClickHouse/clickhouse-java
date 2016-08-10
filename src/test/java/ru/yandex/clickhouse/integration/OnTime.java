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

        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
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
    }
}
