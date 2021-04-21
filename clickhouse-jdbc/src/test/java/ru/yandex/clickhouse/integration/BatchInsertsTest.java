package ru.yandex.clickhouse.integration;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseContainerForTest;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.ClickHousePreparedStatementImpl;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

public class BatchInsertsTest {

    private Connection connection;
    private DateFormat dateFormat;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseDataSource dataSource = ClickHouseContainerForTest.newDataSource();
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(TimeZone.getDefault());
    }

    @AfterTest
    public void tearDown() throws Exception {
        connection.createStatement().execute("DROP DATABASE test");
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
    public void testBatchInsert2() throws Exception {
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

        Date date = new Date(dateFormat.parse("1989-01-30").getTime());
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
    public void testBatchInsert3() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.batch_insert3");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.batch_insert3 (i Int32, s String) ENGINE = TinyLog"
        );

        ClickHousePreparedStatementImpl statement = (ClickHousePreparedStatementImpl) connection.prepareStatement("INSERT INTO test.batch_insert3 (s, i) VALUES (?, ?), (?, ?)");
        statement.setString(1, "firstParam");
        statement.setInt(2, 1);
        statement.setString(3, "thirdParam");
        statement.setInt(4, 2);
        statement.addBatch();
        int[] result = statement.executeBatch();
        Assert.assertEquals(result, new int[]{1, 1});

        ResultSet rs = connection.createStatement().executeQuery("SELECT count() as cnt from test.batch_insert3");
        rs.next();

        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertFalse(rs.next());
    }

    @Test
    public void batchInsert4() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.batch_insert4");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.batch_insert4 (i Int32, s String) ENGINE = TinyLog"
        );

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.batch_insert4 (i, s) VALUES (?, 'hello'), (?, ?)");
        statement.setInt(1, 42);
        statement.setInt(2, 43);
        statement.setString(3, "first_param");
        statement.execute();

        ResultSet rs = connection.createStatement().executeQuery("SELECT count() as cnt from test.batch_insert4");
        rs.next();

        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testBatchInsert5() throws Exception {

        connection.createStatement().execute("DROP TABLE IF EXISTS test.batch_insert5");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.batch_insert5 (i Int32, s String) ENGINE = TinyLog"
        );

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.batch_insert5 (i, s) VALUES (?, 'hello'), (?, ?)");
        statement.setInt(1, 42);
        statement.setInt(2, 43);
        statement.setString(3, "first_param");
        statement.addBatch();
        statement.setInt(1, 44);
        statement.setInt(2, 45);
        statement.setString(3, "second_param");
        statement.addBatch();
        statement.executeBatch();

        ResultSet rs = connection.createStatement().executeQuery("SELECT count() as cnt from test.batch_insert5");
        rs.next();

        Assert.assertEquals(rs.getInt("cnt"), 4);
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testSimpleInsert() throws Exception {
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

        Date date = new Date(dateFormat.parse("1989-01-30").getTime());
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

        Assert.assertEquals(rs.getDate("date").getTime(), date.getTime());
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

        Date date = new Date(dateFormat.parse("1989-01-30").getTime());
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

    @Test(expectedExceptions = SQLException.class)
    public void testNullParameters() throws SQLException {
        PreparedStatement st = connection.prepareStatement("INSERT INTO test.batch_single_test (date, values) VALUES (?, ?)");
        st.setString(2, "test");
        st.addBatch();
    }

    @Test
    public void testBatchInsertWithLongQuery() throws SQLException {
        int columnCount = 200;
        try (Statement s = connection.createStatement()) {
            String createColumns = IntStream.range(0, columnCount).mapToObj(
                i -> "`looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongnaaaaameeeeeeee" + i + "` String "
            ).collect(Collectors.joining(","));
            s.execute("DROP TABLE IF EXISTS test.batch_insert_with_long_query");
            s.execute("CREATE TABLE test.batch_insert_with_long_query (" + createColumns + ") ENGINE = Memory");
        }
        
        String values = IntStream.range(0, columnCount).mapToObj(i -> "?").collect(Collectors.joining(","));
        String columns = IntStream.range(0, columnCount).mapToObj(
            i -> "looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongnaaaaameeeeeeee" + i
        ).collect(Collectors.joining(","));
        int index = 1;
        try (PreparedStatement s = connection.prepareStatement("INSERT INTO test.batch_insert_with_long_query (" + columns + ") VALUES (" + values + ")")) {
            for (int i = 0; i < columnCount; i++) {
                s.setString(index++, "12345");
            }
            s.addBatch();
            s.executeBatch();
        }
    }
}
