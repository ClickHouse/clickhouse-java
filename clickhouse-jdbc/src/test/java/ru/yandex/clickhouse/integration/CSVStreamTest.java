package ru.yandex.clickhouse.integration;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseContainerForTest;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringBufferInputStream;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CSVStreamTest {
    private ClickHouseDataSource dataSource;
    private ClickHouseConnection connection;

    @BeforeTest
    public void setUp() throws Exception {
        dataSource = ClickHouseContainerForTest.newDataSource();
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @Test
    public void simpleInsert() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.csv_stream");
        connection.createStatement().execute(
                "CREATE TABLE test.csv_stream (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(Charset.forName("UTF-8")));
        inputStream = new StringBufferInputStream(string);

        connection.createStatement().sendCSVStream(inputStream, "test.csv_stream");

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM test.csv_stream");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

}
