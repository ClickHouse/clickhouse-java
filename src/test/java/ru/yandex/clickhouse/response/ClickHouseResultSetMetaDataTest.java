package ru.yandex.clickhouse.response;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;


public class ClickHouseResultSetMetaDataTest {
    private Connection connection;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        ClickHouseDataSource dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @AfterTest
    public void tearDown() throws Exception {
        connection.close();
    }

    @Test
    public void getInformationFromMetadataTest() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.metadata_test_1");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.metadata_test_1 (i Int32, s String, col1 Nullable(Int32)) ENGINE = TinyLog"
        );

        ClickHouseResultSet rs = connection
                .createStatement()
                .executeQuery("SELECT * FROM test.metadata_test_1")
                .unwrap(ClickHouseResultSet.class);


        ClickHouseResultSetMetaData metaData = new ClickHouseResultSetMetaData(rs);
        Assert.assertEquals(3, metaData.getColumnCount());

        Assert.assertEquals("metadata_test_1", metaData.getTableName(1));
        Assert.assertEquals("metadata_test_1", metaData.getTableName(2));
        Assert.assertEquals("metadata_test_1", metaData.getTableName(3));

        Assert.assertEquals(Types.INTEGER, metaData.getColumnType(1));
        Assert.assertEquals(Types.VARCHAR, metaData.getColumnType(2));
        Assert.assertEquals(Types.INTEGER, metaData.getColumnType(3));

        Assert.assertEquals("test", metaData.getCatalogName(1));
        Assert.assertEquals("test", metaData.getCatalogName(2));
        Assert.assertEquals("test", metaData.getCatalogName(3));

        Assert.assertEquals("Int32", metaData.getColumnTypeName(1));
        Assert.assertEquals("String", metaData.getColumnTypeName(2));
        Assert.assertEquals("Nullable(Int32)", metaData.getColumnTypeName(3));

        Assert.assertEquals(ResultSetMetaData.columnNoNulls, metaData.isNullable(1));
        Assert.assertEquals(ResultSetMetaData.columnNoNulls, metaData.isNullable(2));
        Assert.assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(3));
    }

    @Test
    public void getInformationFromMetadataTest_WithJoinStatement() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.metadata_test_1");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.metadata_test_1 (i Int32, s String, col1 Nullable(Int32)) ENGINE = TinyLog"
        );

        connection.createStatement().execute("DROP TABLE IF EXISTS test.metadata_test_2");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.metadata_test_2 (i Int32, s String, col1 String, col2 Nullable(Int32)) ENGINE = TinyLog"
        );

        connection.createStatement().execute("insert into test.metadata_test_1 values (1,'2',3)");
        connection.createStatement().execute("insert into test.metadata_test_2 values (1,'2','3',4)");

        ClickHouseResultSet rs = connection
                .createStatement()
                .executeQuery(
                        "SELECT " +
                                "c2, " +
                                "n3, " +
                                "commonColumn " +
                                "FROM (" +
                                "    SELECT " +
                                "       i as commonColumn, " +
                                "       s as c2, " +
                                "       col1 " +
                                "    FROM test.metadata_test_1 " +
                                ") ANY LEFT JOIN (" +
                                "SELECT " +
                                "        i as commonColumn, " +
                                "        s, " +
                                "        col1 as n2, " +
                                "        col2 AS n3 " +
                                "FROM test.metadata_test_2)" +
                                "USING commonColumn ")
                .unwrap(ClickHouseResultSet.class);

        ClickHouseResultSetMetaData metaData = rs.getMetaData().unwrap(ClickHouseResultSetMetaData.class);

        Assert.assertEquals(3, metaData.getColumnCount());
        Assert.assertEquals("metadata_test_1", metaData.getTableName(1));
        Assert.assertEquals("metadata_test_2", metaData.getTableName(2));
        Assert.assertEquals("metadata_test_2", metaData.getTableName(3));

        Assert.assertEquals(ResultSetMetaData.columnNoNulls, metaData.isNullable(1));
        Assert.assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(2));
        Assert.assertEquals(ResultSetMetaData.columnNoNulls, metaData.isNullable(3));
    }

    @Test
    public void getInformationFromMetadataTest_WithJoinStatementWithStar() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.metadata_test_1");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.metadata_test_1 (i Int32, s String, col1 Nullable(Int32)) ENGINE = TinyLog"
        );

        connection.createStatement().execute("DROP TABLE IF EXISTS test.metadata_test_2");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.metadata_test_2 (i Int32, s String, col1 String, col2 Nullable(Int32)) ENGINE = TinyLog"
        );

        connection.createStatement().execute("insert into test.metadata_test_1 values (1,'2',3)");
        connection.createStatement().execute("insert into test.metadata_test_2 values (1,'2','3',4)");

        ClickHouseResultSet rs = connection
                .createStatement()
                .executeQuery(
                        "SELECT " +
                                " * "+
                                "FROM (" +
                                "    SELECT " +
                                "       i as commonColumn, " +
                                "       s as c2, " +
                                "       col1 " +
                                "    FROM test.metadata_test_1 " +
                                ") ANY LEFT JOIN (" +
                                "SELECT " +
                                "        i as commonColumn, " +
                                "        s, " +
                                "        col1 as n2, " +
                                "        col2 AS n3 " +
                                "FROM test.metadata_test_2)" +
                                "USING commonColumn ")
                .unwrap(ClickHouseResultSet.class);

        ClickHouseResultSetMetaData metaData = rs.getMetaData().unwrap(ClickHouseResultSetMetaData.class);

        Assert.assertEquals(3, metaData.getColumnCount());
        Assert.assertEquals("metadata_test_1", metaData.getTableName(1));
        Assert.assertEquals("metadata_test_1", metaData.getTableName(2));
        Assert.assertEquals("metadata_test_1", metaData.getTableName(3));

        Assert.assertEquals(ResultSetMetaData.columnNoNulls, metaData.isNullable(1));
        Assert.assertEquals(ResultSetMetaData.columnNoNulls, metaData.isNullable(2));
        Assert.assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(3));
    }
}
