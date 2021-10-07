package ru.yandex.clickhouse.integration;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.JdbcIntegrationTest;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CSVStreamTest extends JdbcIntegrationTest {
    private ClickHouseConnection connection;

    @BeforeClass(groups = "integration")
    public void setUp() throws Exception {
        connection = newConnection();
    }

    @AfterClass(groups = "integration")
    public void tearDown() throws Exception {
        closeConnection(connection);
    }
    
    @Test(groups = "integration")
    public void simpleInsert() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS csv_stream");
        connection.createStatement().execute(
                "CREATE TABLE csv_stream (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(Charset.forName("UTF-8")));

        connection.createStatement().sendCSVStream(inputStream, dbName + ".csv_stream");

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM csv_stream");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

}
