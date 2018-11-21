package ru.yandex.clickhouse.integration;

import java.sql.Connection;
import java.sql.ResultSet;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

public class ClickHouseDatabaseMetadataTest {

    private ClickHouseDataSource dataSource;
    private Connection connection;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @AfterTest
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testMetadata() throws Exception {
        connection.createStatement().executeQuery(
            "DROP TABLE IF EXISTS test.testMetadata");
        connection.createStatement().executeQuery(
            "CREATE TABLE test.testMetadata("
          + "foo Nullable(UInt32), bar UInt64) ENGINE = TinyLog");
        ResultSet columns = connection.getMetaData().getColumns(
            null, "test", "testMetaData", null);
        while (columns.next()) {
            String colName = columns.getString("COLUMN_NAME");
            if ("foo".equals(colName)) {
                Assert.assertEquals(columns.getString("TYPE_NAME"), "UInt32");
            } else if ("bar".equals(columns.getString("COLUMN_NAME"))) {
                Assert.assertEquals(columns.getString("TYPE_NAME"), "UInt64");
            } else {
                throw new IllegalStateException(
                    "Unexpected column name " + colName);
            }
        }
    }

}
