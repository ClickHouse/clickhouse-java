package ru.yandex.clickhouse.integration;

import com.google.common.primitives.UnsignedLong;
import com.google.common.primitives.UnsignedLongs;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class RowBinaryStreamTest {

    private ClickHouseDataSource dataSource;
    private ClickHouseConnection connection;

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseProperties properties = new ClickHouseProperties();
        dataSource = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123", properties);
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    private void createTable(String table) throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS " + table);
        connection.createStatement().execute(
            "CREATE TABLE " + table + " (" +
                "date Date, " +
                "dateTime DateTime, " +
                "string String, " +
                "int8 Int8, " +
                "uInt8 UInt8, " +
                "int16 Int16, " +
                "uInt16 UInt16, " +
                "int32 Int32, " +
                "uInt32 UInt32, " +
                "int64 Int64, " +
                "uInt64 UInt64, " +
                "float32 Float32, " +
                "float64 Float64 " +
                ") ENGINE = MergeTree(date, (date), 8192)"
        );
    }

    @Test
    public void multiRowTest() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.big_data");
        connection.createStatement().execute(
            "CREATE TABLE test.big_data (" +
                "value Int32" +
                ") ENGINE = TinyLog()"
        );

        final int count = 1000000;
        final AtomicLong sum = new AtomicLong();

        connection.createStatement().sendRowBinaryStream(
            "INSERT INTO test.big_data (value)",
            new ClickHouseStreamCallback() {
                @Override
                public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                    for (int i = 0; i < count; i++) {
                        stream.writeInt32(i);
                        sum.addAndGet(i);
                    }
                }
            }
        );

        ResultSet rs = connection.createStatement().executeQuery("SELECT count() as cnt, sum(value) as sum FROM test.big_data");

        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), count);
        Assert.assertEquals(rs.getLong("sum"), sum.get());
    }


    @Test
    public void testRowBinaryStream() throws Exception {
        createTable("test.raw_binary");
        ClickHouseStatement statement = connection.createStatement();
        final Date date1 = new Date(1483230102000L); //2017-01-01 03:21:42
        final Date date2 = new Date(1494321702000L); //2017-05-09 12:21:42

        statement.sendRowBinaryStream(
            "INSERT INTO test.raw_binary " +
                "(date, dateTime, string, int8, uInt8, int16, uInt16, int32, uInt32, int64, uInt64, float32, float64)",
            new ClickHouseStreamCallback() {
                @Override
                public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {

                    stream.writeDate(date1);
                    stream.writeDateTime(date1);
                    stream.writeString("string\n1");
                    stream.writeInt8(Byte.MIN_VALUE);
                    stream.writeUInt8(0);
                    stream.writeInt16(Short.MIN_VALUE);
                    stream.writeUInt16(0);
                    stream.writeInt32(Integer.MIN_VALUE);
                    stream.writeUInt32(0);
                    stream.writeInt64(Long.MIN_VALUE);
                    stream.writeUInt64(0);
                    stream.writeFloat32((float) 123.456);
                    stream.writeFloat64(42.21);

                    stream.writeDate(date2);
                    stream.writeDateTime(date2);
                    stream.writeString("a\tbdasd''a");
                    stream.writeInt8(Byte.MAX_VALUE);
                    stream.writeUInt8(255);
                    stream.writeInt16(Short.MAX_VALUE);
                    stream.writeUInt16(42000);
                    stream.writeInt32(Integer.MAX_VALUE);
                    stream.writeUInt32(Integer.MAX_VALUE + 100L);
                    stream.writeInt64(Long.MAX_VALUE);
                    stream.writeUInt64(UnsignedLong.fromLongBits(UnsignedLongs.MAX_VALUE));
                    stream.writeFloat32((float) 21.21);
                    stream.writeFloat64(77.77);
                }
            }
        );

        ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM test.raw_binary ORDER BY date");

        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getString("date"), "2017-01-01");
        Assert.assertEquals(rs.getTimestamp("dateTime").getTime(), date1.getTime());
        Assert.assertEquals(rs.getString("string"), "string\n1");
        Assert.assertEquals(rs.getInt("int8"), Byte.MIN_VALUE);
        Assert.assertEquals(rs.getInt("uInt8"), 0);
        Assert.assertEquals(rs.getInt("int16"), Short.MIN_VALUE);
        Assert.assertEquals(rs.getInt("uInt16"), 0);
        Assert.assertEquals(rs.getInt("int32"), Integer.MIN_VALUE);
        Assert.assertEquals(rs.getInt("uInt32"), 0);
        Assert.assertEquals(rs.getLong("int64"), Long.MIN_VALUE);
        Assert.assertEquals(rs.getLong("uInt64"), 0);
        Assert.assertEquals(rs.getDouble("float32"), 123.456);
        Assert.assertEquals(rs.getDouble("float64"), 42.21);

        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getString("date"), "2017-05-09");
        Assert.assertEquals(rs.getTimestamp("dateTime").getTime(), date2.getTime());
        Assert.assertEquals(rs.getString("string"), "a\tbdasd''a");
        Assert.assertEquals(rs.getInt("int8"), Byte.MAX_VALUE);
        Assert.assertEquals(rs.getInt("uInt8"), 255);
        Assert.assertEquals(rs.getInt("int16"), Short.MAX_VALUE);
        Assert.assertEquals(rs.getInt("uInt16"), 42000);
        Assert.assertEquals(rs.getInt("int32"), Integer.MAX_VALUE);
        Assert.assertEquals(rs.getLong("uInt32"), 2147483747L);
        Assert.assertEquals(rs.getLong("int64"), Long.MAX_VALUE);
        Assert.assertEquals(rs.getString("uInt64"), "18446744073709551615");
        Assert.assertEquals(rs.getDouble("float32"), 21.21);
        Assert.assertEquals(rs.getDouble("float64"), 77.77);

        Assert.assertFalse(rs.next());
    }


    @Test
    public void testTimeZone() throws Exception{
        final ClickHouseStatement statement = connection.createStatement();
        connection.createStatement().execute("DROP TABLE IF EXISTS test.binary_tz");
        connection.createStatement().execute(
            "CREATE TABLE test.binary_tz (date Date, dateTime DateTime) ENGINE = MergeTree(date, (date), 8192)"
        );

        final Date date1 = new Date(1497474018000L);

        statement.sendRowBinaryStream(
            "INSERT INTO test.binary_tz (date, dateTime)",
            new ClickHouseStreamCallback() {
                @Override
                public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                    stream.writeDate(date1);
                    stream.writeDateTime(date1);
                }
            }
        );

        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT countIf(date != toDate(dateTime)) as cnt from test.binary_tz"
        );

        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("cnt"), 0);
    }

}
