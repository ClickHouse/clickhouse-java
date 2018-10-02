package ru.yandex.clickhouse.integration;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.*;
import java.util.Collections;

public class BatchInserts {
    private ClickHouseDataSource dataSource;
    private Connection connection;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @Test
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

    @Test
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

    @Test
    public void testSimpleInsert() throws Exception{
        connection.createStatement().execute("DROP TABLE IF EXISTS test.insert");
        connection.createStatement().execute(
                "CREATE TABLE test.insert (" +
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
                "INSERT INTO test.insert (date, date_time, string, int32, float64) VALUES (?, ?, ?, ?, ?)"
        );

        statement.setDate(1, date);
        statement.setTimestamp(2, dateTime);
        statement.setString(3, string);
        statement.setInt(4, int32);
        statement.setDouble(5, float64);

        statement.execute();

        ResultSet rs = connection.createStatement().executeQuery("SELECT * from test.insert");
        Assert.assertTrue(rs.next());

        Assert.assertEquals(rs.getDate("date"), date);
        Assert.assertEquals(rs.getTimestamp("date_time"), dateTime);
        Assert.assertEquals(rs.getString("string"), string);
        Assert.assertEquals(rs.getInt("int32"), int32);
        Assert.assertEquals(rs.getDouble("float64"), float64);

        Assert.assertFalse(rs.next());
    }

    @Test
     public void batchInsertNulls() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.batch_insert_nulls");
        connection.createStatement().execute(
                        "CREATE TABLE test.batch_insert_nulls (" +
                                        "date Date," +
                                        "date_time Nullable(DateTime)," +
                                        "string Nullable(String)," +
                                        "int32 Nullable(Int32)," +
                                        "float64 Nullable(Float64)" +
                                        ") ENGINE = MergeTree(date, (date), 8192)"
                        );

        ClickHousePreparedStatement statement = (ClickHousePreparedStatement) connection.prepareStatement(
                "INSERT INTO test.batch_insert_nulls (date, date_time, string, int32, float64) VALUES (?, ?, ?, ?, ?)"
                );

        Date date = new Date(602110800000L); //1989-01-30
        statement.setDate(1, date);
        statement.setObject(2, null, Types.TIMESTAMP);
        statement.setObject(3, null, Types.VARCHAR);
        statement.setObject(4, null, Types.INTEGER);
        statement.setObject(5, null, Types.DOUBLE);
        statement.addBatch();
        statement.executeBatch(Collections.singletonMap(ClickHouseQueryParam.CONNECT_TIMEOUT, "1000"));

        ResultSet rs = connection.createStatement().executeQuery("SELECT date, date_time, string, int32, float64 from test.batch_insert_nulls");
        Assert.assertTrue(rs.next());

        Assert.assertEquals(rs.getDate("date"), date);
        Assert.assertNull(rs.getTimestamp("date_time"));
        Assert.assertNull(rs.getString("string"));
        Assert.assertEquals(rs.getInt("int32"), 0);
        Assert.assertNull(rs.getObject("int32"));
        Assert.assertEquals(rs.getDouble("float64"), 0.0);
        Assert.assertNull(rs.getObject("float64"));

        Assert.assertFalse(rs.next());
        connection.createStatement().execute("DROP TABLE test.batch_insert_nulls");
    }

    @Test
    public void testBatchValuesColumn() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.batch_single_test");
        connection.createStatement().execute(
                "CREATE TABLE test.batch_single_test(date Date, values String) ENGINE = StripeLog"
        );

        PreparedStatement st = connection.prepareStatement("INSERT INTO test.batch_single_test (date, values) VALUES (?, ?)");
        st.setDate(1, new Date(System.currentTimeMillis()));
        st.setString(2, "test");

        st.addBatch();
        st.executeBatch();
    }
}
