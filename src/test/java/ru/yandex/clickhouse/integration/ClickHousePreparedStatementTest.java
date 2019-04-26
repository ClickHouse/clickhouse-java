package ru.yandex.clickhouse.integration;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.ClickHousePreparedStatementImpl;
import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.response.ClickHouseResponse;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import static java.util.Collections.singletonList;

public class ClickHousePreparedStatementTest {

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
    public void testArrayTest() throws Exception {

        connection.createStatement().execute("DROP TABLE IF EXISTS test.array_test");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.array_test (i Int32, a Array(Int32)) ENGINE = TinyLog"
        );

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.array_test (i, a) VALUES (?, ?)");

        statement.setInt(1, 1);
        statement.setArray(2, new ClickHouseArray(ClickHouseDataType.Int32, new int[]{1, 2, 3}));
        statement.addBatch();

        statement.setInt(1, 2);
        statement.setArray(2, new ClickHouseArray(ClickHouseDataType.Int32, new int[]{2, 3, 4, 5}));
        statement.addBatch();
        statement.executeBatch();

        ResultSet rs = connection.createStatement().executeQuery("SELECT count() as cnt from test.array_test");
        rs.next();

        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testArrayOfNullable() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.array_of_nullable");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.array_of_nullable (" +
                        "str Nullable(String), " +
                        "int Nullable(Int32), " +
                        "strs Array(Nullable(String)), " +
                        "ints Array(Nullable(Int32))) ENGINE = TinyLog"
        );

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO test.array_of_nullable (str, int, strs, ints) VALUES (?, ?, ?, ?)"
        );

        statement.setObject(1, null);
        statement.setObject(2, null);
        statement.setObject(3, new String[]{"a", null, "c"});
        statement.setArray(4, new ClickHouseArray(ClickHouseDataType.Int32, new Integer[]{1, null, 3}));
        statement.addBatch();
        statement.executeBatch();

        ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM test.array_of_nullable");

        Assert.assertTrue(rs.next());
        Assert.assertNull(rs.getObject("str"));
        Assert.assertNull(rs.getObject("int"));
        Assert.assertEquals(rs.getArray("strs").getArray(), new String[]{"a", null, "c"});
        Assert.assertEquals(rs.getArray("ints").getArray(), new int[]{1, 0, 3});
        Assert.assertFalse(rs.next());

        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUseObjectsInArrays(true);
        ClickHouseDataSource configuredDataSource = new ClickHouseDataSource(dataSource.getUrl(), properties);
        ClickHouseConnection configuredConnection = configuredDataSource.getConnection();

        try {
            rs = configuredConnection.createStatement().executeQuery("SELECT * FROM test.array_of_nullable");
            rs.next();

            Assert.assertEquals(rs.getArray("ints").getArray(), new Integer[]{1, null, 3});
        } finally {
            configuredConnection.close();
        }
    }

    @Test
    public void testArrayFixedStringTest() throws Exception {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.array_fixed_string_test");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.array_fixed_string_test (i Int32, a Array(FixedString(16))) ENGINE = TinyLog"
        );

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.array_fixed_string_test (i, a) VALUES (?, ?)");

        statement.setInt(1, 1);
        statement.setArray(2, new ClickHouseArray(ClickHouseDataType.FixedString, new byte[][]{randomEncodedUUID(), randomEncodedUUID()}));
        statement.addBatch();

        statement.setInt(1, 2);
        statement.setArray(2, new ClickHouseArray(ClickHouseDataType.FixedString, new byte[][]{randomEncodedUUID(), randomEncodedUUID()}));
        statement.addBatch();
        statement.executeBatch();

        ResultSet rs = connection.createStatement().executeQuery("SELECT count() as cnt from test.array_fixed_string_test");
        rs.next();

        Assert.assertEquals(rs.getInt("cnt"), 2);
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testInsertUInt() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.unsigned_insert");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.unsigned_insert (ui32 UInt32, ui64 UInt64) ENGINE = TinyLog"
        );
        PreparedStatement stmt = connection.prepareStatement("insert into test.unsigned_insert (ui32, ui64) values (?, ?)");
        stmt.setObject(1, 4294967286L);
        stmt.setObject(2, new BigInteger("18446744073709551606"));
        stmt.execute();
        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select ui32, ui64 from test.unsigned_insert");
        rs.next();
        Object bigUInt32 = rs.getObject(1);
        Assert.assertTrue(bigUInt32 instanceof Long);
        Assert.assertEquals(((Long)bigUInt32).longValue(), 4294967286L);
        Object bigUInt64 = rs.getObject(2);
        Assert.assertTrue(bigUInt64 instanceof BigInteger);
        Assert.assertEquals(bigUInt64, new BigInteger("18446744073709551606"));
    }

    @Test
    public void testInsertUUID() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.uuid_insert");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.uuid_insert (ui32 UInt32, uuid UUID) ENGINE = TinyLog"
        );
        PreparedStatement stmt = connection.prepareStatement("insert into test.uuid_insert (ui32, uuid) values (?, ?)");
        stmt.setObject(1, Long.valueOf(4294967286L));
        stmt.setObject(2, UUID.fromString("bef35f40-3b03-45b0-b1bd-8ec6593dcaaa"));
        stmt.execute();
        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select ui32, uuid from test.uuid_insert");
        rs.next();
        Object bigUInt32 = rs.getObject(1);
        Assert.assertTrue(bigUInt32 instanceof Long);
        Assert.assertEquals(((Long)bigUInt32).longValue(), 4294967286L);
        Object uuid = rs.getObject(2);
        Assert.assertTrue(uuid instanceof UUID);
        Assert.assertEquals(uuid, UUID.fromString("bef35f40-3b03-45b0-b1bd-8ec6593dcaaa"));
    }

    @Test
    public void testInsertUUIDBatch() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.uuid_insert");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.uuid_insert (ui32 UInt32, uuid UUID) ENGINE = TinyLog"
        );
        PreparedStatement stmt = connection.prepareStatement("insert into test.uuid_insert (ui32, uuid) values (?, ?)");
        stmt.setObject(1, 4294967286L);
        stmt.setObject(2, UUID.fromString("bef35f40-3b03-45b0-b1bd-8ec6593dcaaa"));
        stmt.addBatch();
        stmt.executeBatch();
        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select ui32, uuid from test.uuid_insert");
        rs.next();
        Object bigUInt32 = rs.getObject(1);
        Assert.assertTrue(bigUInt32 instanceof Long);
        Assert.assertEquals(((Long)bigUInt32).longValue(), 4294967286L);
        Object uuid = rs.getObject(2);
        Assert.assertTrue(uuid instanceof UUID);
        Assert.assertEquals(uuid, UUID.fromString("bef35f40-3b03-45b0-b1bd-8ec6593dcaaa"));
    }

    @Test
    public void testInsertNullString() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.null_insert");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.null_insert (val Nullable(String)) ENGINE = TinyLog"
        );

        PreparedStatement stmt = connection.prepareStatement("insert into test.null_insert (val) values (?)");
        stmt.setNull(1, Types.VARCHAR);
        stmt.execute();
        stmt.setNull(1, Types.VARCHAR);
        stmt.addBatch();
        stmt.executeBatch();

        stmt.setString(1, null);
        stmt.execute();
        stmt.setString(1, null);
        stmt.addBatch();
        stmt.executeBatch();

        stmt.setObject(1, null);
        stmt.execute();
        stmt.setObject(1, null);
        stmt.addBatch();
        stmt.executeBatch();

        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select count(*), val from test.null_insert group by val");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 6);
        Assert.assertNull(rs.getString(2));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testSelectNullableTypes() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.select_nullable");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.select_nullable (idx Int32, i Nullable(Int32), ui Nullable(UInt64), f Nullable(Float32), s Nullable(String)) ENGINE = TinyLog"
        );

        PreparedStatement stmt = connection.prepareStatement("insert into test.select_nullable (idx, i, ui, f, s) values (?, ?, ?, ?, ?)");
        stmt.setInt(1, 1);
        stmt.setObject(2, null);
        stmt.setObject(3, null);
        stmt.setObject(4, null);
        stmt.setString(5, null);
        stmt.addBatch();
        stmt.setInt(1, 2);
        stmt.setInt(2, 1);
        stmt.setInt(3, 1);
        stmt.setFloat(4, 1.0f);
        stmt.setString(5, "aaa");
        stmt.addBatch();
        stmt.executeBatch();

        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select i, ui, f, s from test.select_nullable order by idx");
        rs.next();
        Assert.assertEquals(rs.getMetaData().getColumnType(1), Types.INTEGER);
        Assert.assertEquals(rs.getMetaData().getColumnType(2), Types.BIGINT);
        Assert.assertEquals(rs.getMetaData().getColumnType(3), Types.FLOAT);
        Assert.assertEquals(rs.getMetaData().getColumnType(4), Types.VARCHAR);

        Assert.assertNull(rs.getObject(1));
        Assert.assertNull(rs.getObject(2));
        Assert.assertNull(rs.getObject(3));
        Assert.assertNull(rs.getObject(4));

        Assert.assertEquals(rs.getInt(1), 0);
        Assert.assertEquals(rs.getInt(1), 0);
        Assert.assertEquals(rs.getFloat(1), 0.0f);
        Assert.assertEquals(rs.getString(1), null);

        rs.next();
        Assert.assertEquals(rs.getObject(1).getClass(), Integer.class);
        Assert.assertEquals(rs.getObject(2).getClass(), BigInteger.class);
        Assert.assertEquals(rs.getObject(3).getClass(), Float.class);
        Assert.assertEquals(rs.getObject(4).getClass(), String.class);

        Assert.assertEquals(rs.getObject(1), 1);
        Assert.assertEquals(rs.getObject(2), BigInteger.ONE);
        Assert.assertEquals(rs.getObject(3), 1.0f);
        Assert.assertEquals(rs.getObject(4), "aaa");

    }

    @Test
    public void testInsertBatchNullValues() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.prep_nullable_value");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test.prep_nullable_value "
          + "(idx Int32, s Nullable(String), i Nullable(Int32), f Nullable(Float32)) "
          + "ENGINE = TinyLog"
        );
        PreparedStatement stmt = connection.prepareStatement(
            "INSERT INTO test.prep_nullable_value (idx, s, i, f) VALUES "
          + "(1, ?, ?, NULL), (2, NULL, NULL, ?)");
        stmt.setString(1, "foo");
        stmt.setInt(2, 42);
        stmt.setFloat(3, 42.0F);
        stmt.addBatch();
        int[] updateCount = stmt.executeBatch();
        Assert.assertEquals(updateCount.length, 2);

        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT s, i, f FROM test.prep_nullable_value "
          + "ORDER BY idx ASC");
        rs.next();
        Assert.assertEquals(rs.getString(1), "foo");
        Assert.assertEquals(rs.getInt(2), 42);
        Assert.assertNull(rs.getObject(3));
        rs.next();
        Assert.assertNull(rs.getObject(1));
        Assert.assertNull(rs.getObject(2));
        Assert.assertEquals(rs.getFloat(3), 42.0f);
    }

    @Test
    public void testSelectDouble() throws SQLException {
        Statement select = connection.createStatement();
        ResultSet rs = select.executeQuery("select toFloat64(0.1) ");
        rs.next();
        Assert.assertEquals(rs.getMetaData().getColumnType(1), Types.DOUBLE);
        Assert.assertEquals(rs.getObject(1).getClass(), Double.class);
        Assert.assertEquals(rs.getDouble(1), 0.1);
    }

    @Test
    public void testExecuteQueryClickhouseResponse() throws SQLException {
        ClickHousePreparedStatement sth = (ClickHousePreparedStatement) connection.prepareStatement("select ? limit 5");
        sth.setObject(1, 314);
        ClickHouseResponse resp = sth.executeQueryClickhouseResponse();
        Assert.assertEquals(resp.getData(), singletonList(singletonList("314")));
    }

    @Test
    public void clickhouseJdbcFailsBecauseOfCommentInStart() throws Exception {
        String sqlStatement = "/*comment*/ select * from system.numbers limit 3";
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(sqlStatement);
        Assert.assertNotNull(rs);
        for (int i = 0; i < 3; i++) {
            rs.next();
            Assert.assertEquals(rs.getInt(1), i);
        }
    }

    @Test
    public void testTrailingParameter() throws Exception {
        String sqlStatement =
            "SELECT 42 AS foo, 23 AS bar "
          + "ORDER BY foo DESC LIMIT ?, ?";
        PreparedStatement stmt = connection.prepareStatement(sqlStatement);
        stmt.setInt(1, 42);
        stmt.setInt(2, 23);
        ResultSet rs = stmt.executeQuery();
    }

    @Test
    public void testSetTime() throws Exception {
        ClickHousePreparedStatement stmt = (ClickHousePreparedStatement)
            connection.prepareStatement("SELECT toDateTime(?)");
        stmt.setTime(1, Time.valueOf("13:37:42"));
        ResultSet rs = stmt.executeQuery();
        rs.next();
        Assert.assertEquals(rs.getTime(1), Time.valueOf("13:37:42"));
    }

    @Test
    public void testAsSql() throws Exception {
        String unbindedStatement = "SELECT test.example WHERE id IN (?, ?)";
        ClickHousePreparedStatement statement = (ClickHousePreparedStatement)
            connection.prepareStatement(unbindedStatement);
        Assert.assertEquals(statement.asSql(), unbindedStatement);

        statement.setInt(1, 123);
        Assert.assertEquals(statement.asSql(), unbindedStatement);

        statement.setInt(2, 456);
        Assert.assertEquals(statement.asSql(), "SELECT test.example WHERE id IN (123, 456)");
    }

    @Test
    public void testMetadataOnlySelect() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.mymetadata");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test.mymetadata "
          + "(idx Int32, s String) "
          + "ENGINE = TinyLog"
        );
        PreparedStatement insertStmt = connection.prepareStatement(
            "INSERT INTO test.mymetadata (idx, s) VALUES (?, ?)");
        insertStmt.setInt(1, 42);
        insertStmt.setString(2, "foo");
        insertStmt.executeUpdate();
        PreparedStatement metaStmt = connection.prepareStatement(
            "SELECT idx, s FROM test.mymetadata WHERE idx = ?");
        metaStmt.setInt(1, 42);
        ResultSetMetaData metadata = metaStmt.getMetaData();
        Assert.assertEquals(metadata.getColumnCount(), 2);
        Assert.assertEquals(metadata.getColumnName(1), "idx");
        Assert.assertEquals(metadata.getColumnName(2), "s");
    }

    @Test
    public void testMetadataOnlySelectAfterExecution() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.mymetadata");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test.mymetadata "
          + "(idx Int32, s String) "
          + "ENGINE = TinyLog"
        );
        PreparedStatement insertStmt = connection.prepareStatement(
            "INSERT INTO test.mymetadata (idx, s) VALUES (?, ?)");
        insertStmt.setInt(1, 42);
        insertStmt.setString(2, "foo");
        insertStmt.executeUpdate();
        PreparedStatement metaStmt = connection.prepareStatement(
            "SELECT idx, s FROM test.mymetadata WHERE idx = ?");
        metaStmt.setInt(1, 42);
        metaStmt.executeQuery();
        ResultSetMetaData metadata = metaStmt.getMetaData();
        Assert.assertEquals(metadata.getColumnCount(), 2);
        Assert.assertEquals(metadata.getColumnName(1), "idx");
        Assert.assertEquals(metadata.getColumnName(2), "s");
    }

    @Test
    public void testMetadataExecutionAfterMeta() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.mymetadata");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test.mymetadata "
          + "(idx Int32, s String) "
          + "ENGINE = TinyLog"
        );
        PreparedStatement insertStmt = connection.prepareStatement(
            "INSERT INTO test.mymetadata (idx, s) VALUES (?, ?)");
        insertStmt.setInt(1, 42);
        insertStmt.setString(2, "foo");
        insertStmt.executeUpdate();
        PreparedStatement metaStmt = connection.prepareStatement(
            "SELECT idx, s FROM test.mymetadata WHERE idx = ?");
        metaStmt.setInt(1, 42);
        ResultSetMetaData metadata = metaStmt.getMetaData();
        Assert.assertEquals(metadata.getColumnCount(), 2);
        Assert.assertEquals(metadata.getColumnName(1), "idx");
        Assert.assertEquals(metadata.getColumnName(2), "s");

        ResultSet rs = metaStmt.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt(1), 42);
        Assert.assertEquals(rs.getString(2), "foo");
        metadata = metaStmt.getMetaData();
        Assert.assertEquals(metadata.getColumnCount(), 2);
        Assert.assertEquals(metadata.getColumnName(1), "idx");
        Assert.assertEquals(metadata.getColumnName(2), "s");
    }

    @Test
    public void testMetadataOnlyUpdate() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.mymetadata");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test.mymetadata "
          + "(idx Int32, s String) "
          + "ENGINE = TinyLog"
        );
        PreparedStatement insertStmt = connection.prepareStatement(
            "INSERT INTO test.mymetadata (idx, s) VALUES (?, ?)");
        insertStmt.setInt(1, 42);
        insertStmt.setString(2, "foo");
        insertStmt.executeUpdate();
        PreparedStatement metaStmt = connection.prepareStatement(
            "UPDATE test.mymetadata SET s = ? WHERE idx = ?");
        metaStmt.setString(1, "foo");
        metaStmt.setInt(2, 42);
        ResultSetMetaData metadata = metaStmt.getMetaData();
        Assert.assertNull(metadata);
        metaStmt.close();
    }

    @Test
    public void testInsertWithFunctions() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.insertfunctions");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test.insertfunctions "
          + "(id UInt32, foo String, bar String) "
          + "ENGINE = TinyLog");
        PreparedStatement stmt = connection.prepareStatement(
            "INSERT INTO test.insertfunctions(id, foo, bar) VALUES "
          + "(?, lower(reverse(?)), upper(reverse(?)))");
        stmt.setInt(1, 42);
        stmt.setString(2, "Foo");
        stmt.setString(3, "Bar");
        String sql = stmt.unwrap(ClickHousePreparedStatementImpl.class).asSql();
        Assert.assertEquals(
            sql,
            "INSERT INTO test.insertfunctions(id, foo, bar) VALUES "
          + "(42, lower(reverse('Foo')), upper(reverse('Bar')))");
        // make sure that there is no exception
        stmt.execute();
        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT id, foo, bar FROM test.insertfunctions");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 42);
        Assert.assertEquals(rs.getString(2), "oof");
        Assert.assertEquals(rs.getString(3), "RAB");
        rs.close();
    }

    public void testBytes() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.strings_versus_bytes");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test.strings_versus_bytes"
          + "(s String, fs FixedString(8)) "
          + "ENGINE = TinyLog"
        );
        PreparedStatement insertStmt = connection.prepareStatement(
            "INSERT INTO test.strings_versus_bytes (s, fs) VALUES (?, ?)");
        insertStmt.setBytes(1, "foo".getBytes(Charset.forName("UTF-8")));
        insertStmt.setBytes(2, "bar".getBytes(Charset.forName("UTF-8")));
        insertStmt.executeUpdate();
        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT s, fs FROM test.strings_versus_bytes");
        rs.next();
        Assert.assertEquals(rs.getString(1), "foo");
        // TODO: The actual String returned by our ResultSet is rather strange
        // ['b' 'a' 'r' 0 0 0 0 0]
        Assert.assertEquals(rs.getString(2).trim(), "bar");
    }

    @Test
    public void testInsertWithFunctionsAddBatch() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.insertfunctions");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test.insertfunctions "
          + "(id UInt32, foo String, bar String) "
          + "ENGINE = TinyLog");
        PreparedStatement stmt = connection.prepareStatement(
            "INSERT INTO test.insertfunctions(id, foo, bar) VALUES "
          + "(?, lower(reverse(?)), upper(reverse(?)))");
        stmt.setInt(1, 42);
        stmt.setString(2, "Foo");
        stmt.setString(3, "Bar");
        stmt.addBatch();
        stmt.executeBatch();
        // this will _not_ perform the functions, but instead send the parameters
        // as is to the clickhouse server
    }

    @SuppressWarnings("boxing")
    @Test
    public void testMultiLineValues() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.multiline");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test.multiline"
          + "(foo Int32, bar String) "
          + "ENGINE = TinyLog"
        );
        PreparedStatement insertStmt = connection.prepareStatement(
            "INSERT INTO test.multiline\n"
          + "\t(foo, bar)\r\n"
          + "\t\tVALUES\n"
          + "(?, ?) , \n\r"
          + "\t(?,?),(?,?)\n");
        Map<Integer, String> testData = new HashMap<Integer, String>();
        testData.put(23, "baz");
        testData.put(42, "bar");
        testData.put(1337, "oof");
        int i = 0;
        for (Integer k : testData.keySet()) {
            insertStmt.setInt(++i, k.intValue());
            insertStmt.setString(++i, testData.get(k));
        }
        insertStmt.executeUpdate();

        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT * FROM test.multiline ORDER BY foo");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 23);
        Assert.assertEquals(rs.getString(2), "baz");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 42);
        Assert.assertEquals(rs.getString(2), "bar");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 1337);
        Assert.assertEquals(rs.getString(2), "oof");
        Assert.assertFalse(rs.next());
    }

    // Issue 153
    public void testArrayDateTime() throws Exception {
        connection.createStatement().execute(
            "DROP TABLE IF EXISTS test.date_time_array");
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test.date_time_array"
          + "(foo Array(DateTime)) "
          + "ENGINE = TinyLog"
        );
        PreparedStatement stmt = connection.prepareStatement(
            "INSERT INTO test.date_time_array (foo) VALUES (?)");
        stmt.setArray(1, connection.createArrayOf("DateTime",
            new Timestamp[] {
                new Timestamp(1557136800000L),
                new Timestamp(1560698526598L)
            }));
        stmt.execute();

        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT foo FROM test.date_time_array");
        rs.next();
        Timestamp[] result = (Timestamp[]) rs.getArray(1).getArray();
        Assert.assertEquals(result[0].getTime(), 1557136800000L);
        Assert.assertEquals(result[1].getTime(), 1560698526598L);
    }

    private static byte[] randomEncodedUUID() {
        UUID uuid = UUID.randomUUID();
        return ByteBuffer.allocate(16)
            .putLong(uuid.getMostSignificantBits())
            .putLong(uuid.getLeastSignificantBits())
            .array();
    }
}
