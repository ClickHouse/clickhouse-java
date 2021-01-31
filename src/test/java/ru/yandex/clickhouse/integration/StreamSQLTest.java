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

public class StreamSQLTest {
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
    public void simpleCSVInsert() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.csv_stream_sql");
        connection.createStatement().execute(
                "CREATE TABLE test.csv_stream_sql (value Int32, string_value String) ENGINE = Log()"
        );

        String string = "5,6\n1,6";
        InputStream inputStream = new ByteArrayInputStream(string.getBytes(Charset.forName("UTF-8")));

        connection.createStatement().sendStreamSQL(inputStream, "insert into test.csv_stream_sql format CSV");

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM test.csv_stream_sql");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertEquals(rs.getLong("sum"), 6);
        Assert.assertEquals(rs.getLong("uniq"), 1);
    }

    @Test
    public void multiRowTSVInsert() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.tsv_stream_sql");
        connection.createStatement().execute(
                "CREATE TABLE test.tsv_stream_sql (value Int32, string_value String) ENGINE = Log()"
        );


        final int rowsCount = 100000;

        InputStream in = new InputStream() {
            private int si = 0;
            private String s = "";
            private int i = 0;
            private final int count = rowsCount;

            private boolean genNextString() {
                if (i >= count) return false;
                si = 0;
                s = String.format("%d\txxxx%d\n", 1, i);
                i++;
                return true;
            }

            public int read() throws IOException {
                if (si >= s.length()) {
                    if ( ! genNextString() ) {
                        return -1;
                    }
                }
                return s.charAt( si++ );
            }
        };

        connection.createStatement().sendStreamSQL(in, "insert into test.tsv_stream_sql format TSV");

        ResultSet rs = connection.createStatement().executeQuery(
                "SELECT count() AS cnt, sum(value) AS sum, uniqExact(string_value) uniq FROM test.tsv_stream_sql");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), rowsCount);
        Assert.assertEquals(rs.getInt("sum"), rowsCount);
        Assert.assertEquals(rs.getInt("uniq"), rowsCount);
    }

}
