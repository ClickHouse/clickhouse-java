package ru.yandex.clickhouse.integration;

import com.google.common.primitives.UnsignedLong;
import com.google.common.primitives.UnsignedLongs;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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
import java.math.BigInteger;
import java.sql.*;
import java.util.UUID;
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
                        "float64 Float64, " +
                        "dateArray Array(Date), " +
                        "dateTimeArray Array(DateTime), " +
                        "stringArray Array(String), " +
                        "int8Array Array(Int8), " +
                        "uInt8Array Array(UInt8), " +
                        "int16Array Array(Int16), " +
                        "uInt16Array Array(UInt16), " +
                        "int32Array Array(Int32), " +
                        "uInt32Array Array(UInt32), " +
                        "int64Array Array(Int64), " +
                        "uInt64Array Array(UInt64), " +
                        "float32Array Array(Float32), " +
                        "float64Array Array(Float64), " +
                        "uuid UUID" +
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

        ResultSet rs = connection.createStatement().executeQuery("SELECT count() AS cnt, sum(value) AS sum FROM test.big_data");

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
        final Date[] dates1 = {new Date(1263945600000L)};
        final Timestamp[] dateTimes1 = {new Timestamp(1483230102000L)};
        final String[] strings1 = {"test", "test2"};
        final byte[] int8s1 = {Byte.MIN_VALUE};
        final int[] uint8s1 = {0};
        final short[] int16s1 = {Short.MIN_VALUE};
        final int[] uint16s1 = {0};
        final int[] int32s1 = {Integer.MIN_VALUE};
        final long[] uint32s1 = {0};
        final long[] int64s1 = {Long.MIN_VALUE};
        final long[] uint64s1 = {0};
        final float[] float32s1 = {Float.MIN_VALUE};
        final double[] float64s1 = {Double.MIN_VALUE};
        final UUID uuid1 = UUID.fromString("123e4567-e89b-12d3-a456-426655440000");
        final UUID uuid2 = UUID.fromString("789e0123-e89b-12d3-a456-426655444444");

        statement.sendRowBinaryStream(
                "INSERT INTO test.raw_binary " +
                        "(date, dateTime, string, int8, uInt8, int16, uInt16, int32, uInt32, int64, uInt64, float32, float64, dateArray, dateTimeArray, stringArray, int8Array, uInt8Array, int16Array, uInt16Array, int32Array, uInt32Array, int64Array, uInt64Array, float32Array, float64Array, uuid)",
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
                        stream.writeDateArray(dates1);
                        stream.writeDateTimeArray(dateTimes1);
                        stream.writeStringArray(strings1);
                        stream.writeInt8Array(int8s1);
                        stream.writeUInt8Array(uint8s1);
                        stream.writeInt16Array(int16s1);
                        stream.writeUInt16Array(uint16s1);
                        stream.writeInt32Array(int32s1);
                        stream.writeUInt32Array(uint32s1);
                        stream.writeInt64Array(int64s1);
                        stream.writeUInt64Array(uint64s1);
                        stream.writeFloat32Array(float32s1);
                        stream.writeFloat64Array(float64s1);
                        stream.writeUUID(uuid1);

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
                        stream.writeDateArray(new Date[]{date2});
                        stream.writeDateTimeArray(new Date[]{date2});
                        stream.writeStringArray(new String[]{});
                        stream.writeInt8Array(new byte[]{});
                        stream.writeUInt8Array(new int[]{});
                        stream.writeInt16Array(new short[]{});
                        stream.writeUInt16Array(new int[]{});
                        stream.writeInt32Array(new int[]{});
                        stream.writeUInt32Array(new long[]{});
                        stream.writeInt64Array(new long[]{});
                        stream.writeUInt64Array(new long[]{});
                        stream.writeFloat32Array(new float[]{});
                        stream.writeFloat64Array(new double[]{});
                        stream.writeUUID(uuid2);
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
        Assert.assertEquals(rs.getObject("uuid"), "123e4567-e89b-12d3-a456-426655440000");

        final Date[] dateArray = (Date[]) rs.getArray("dateArray").getArray();
        Assert.assertEquals(dateArray.length, dates1.length);
        for (int i = 0; i < dateArray.length; i++) {
            // expected is Date at start of the day in local timezone
            DateTime dt = new DateTime(dates1[i].getTime())
                    .withTimeAtStartOfDay();
            Date expected = new Date(dt.toDate().getTime());
            Assert.assertEquals(dateArray[i], expected);
        }
        final Timestamp[] dateTimeArray = (Timestamp[]) rs.getArray("dateTimeArray").getArray();
        Assert.assertEquals(dateTimeArray.length, dateTimes1.length);
        for (int i = 0; i < dateTimeArray.length; i++) {
            Assert.assertEquals(dateTimeArray[i], dateTimes1[i]);
        }
        final String[] stringArray = (String[]) rs.getArray("stringArray").getArray();
        Assert.assertEquals(stringArray.length, strings1.length);
        for (int i = 0; i < stringArray.length; i++) {
            Assert.assertEquals(stringArray[i], strings1[i]);
        }
        final int[] int8Array = (int[]) rs.getArray("int8Array").getArray();
        Assert.assertEquals(int8Array.length, int8s1.length);
        for (int i = 0; i < int8Array.length; i++) {
            Assert.assertEquals(int8Array[i], int8s1[i]);
        }
        final long[] uInt8Array = (long[]) rs.getArray("uInt8Array").getArray();
        Assert.assertEquals(uInt8Array.length, uint8s1.length);
        for (int i = 0; i < uInt8Array.length; i++) {
            Assert.assertEquals(uInt8Array[i], uint8s1[i]);
        }
        final int[] int16Array = (int[]) rs.getArray("int16Array").getArray();
        Assert.assertEquals(int16Array.length, int16s1.length);
        for (int i = 0; i < int16Array.length; i++) {
            Assert.assertEquals(int16Array[i], int16s1[i]);
        }
        final long[] uInt16Array = (long[]) rs.getArray("uInt16Array").getArray();
        Assert.assertEquals(uInt16Array.length, uint16s1.length);
        for (int i = 0; i < uInt16Array.length; i++) {
            Assert.assertEquals(uInt16Array[i], uint16s1[i]);
        }
        final int[] int32Array = (int[]) rs.getArray("int32Array").getArray();
        Assert.assertEquals(int32Array.length, int32s1.length);
        for (int i = 0; i < int32Array.length; i++) {
            Assert.assertEquals(int32Array[i], int32s1[i]);
        }
        final long[] uInt32Array = (long[]) rs.getArray("uInt32Array").getArray();
        Assert.assertEquals(uInt32Array.length, uint32s1.length);
        for (int i = 0; i < uInt32Array.length; i++) {
            Assert.assertEquals(uInt32Array[i], uint32s1[i]);
        }
        final long[] int64Array = (long[]) rs.getArray("int64Array").getArray();
        Assert.assertEquals(int64Array.length, int64s1.length);
        for (int i = 0; i < int64Array.length; i++) {
            Assert.assertEquals(int64Array[i], int64s1[i]);
        }
        final BigInteger[] uInt64Array = (BigInteger[]) rs.getArray("uInt64Array").getArray();
        Assert.assertEquals(uInt64Array.length, uint64s1.length);
        for (int i = 0; i < uInt64Array.length; i++) {
            Assert.assertEquals(uInt64Array[i], BigInteger.valueOf(uint64s1[i]));
        }
        final float[] float32Array = (float[]) rs.getArray("float32Array").getArray();
        Assert.assertEquals(float32Array.length, float32s1.length);
        for (int i = 0; i < float32Array.length; i++) {
            Assert.assertEquals(float32Array[i], float32s1[i]);
        }
        final double[] float64Array = (double[]) rs.getArray("float64Array").getArray();
        Assert.assertEquals(float64Array.length, float64s1.length);
        for (int i = 0; i < float64Array.length; i++) {
            Assert.assertEquals(float64Array[i], float64s1[i]);
        }

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
        Assert.assertEquals(rs.getString("uuid"), "789e0123-e89b-12d3-a456-426655444444");

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
            "SELECT date, dateTime from test.binary_tz"
        );

        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getTime("dateTime"), new Time(date1.getTime()));
        DateTime dt = new DateTime(date1.getTime())
                .withTimeAtStartOfDay();
        Date expectedDate = new Date(dt.toDate().getTime()); // expected start of the day in local timezone
        Assert.assertEquals(rs.getDate("date"), expectedDate);
    }

}
