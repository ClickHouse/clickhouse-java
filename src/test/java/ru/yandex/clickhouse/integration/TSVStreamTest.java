package ru.yandex.clickhouse.integration;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TSVStreamTest {

    private ClickHouseDataSource dataSource;
    private ClickHouseConnection connection;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @Test
    public void simpleInsert() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.tsv_stream");
        connection.createStatement().execute(
                "CREATE TABLE test.tsv_stream (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5\t6\n1\t6";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(Charset.forName("UTF-8")));

        connection.createStatement().sendStream(inputStream, "test.tsv_stream");

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM test.tsv_stream");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

}
