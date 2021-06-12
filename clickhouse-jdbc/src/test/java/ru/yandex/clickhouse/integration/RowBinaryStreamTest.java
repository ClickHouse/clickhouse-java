package ru.yandex.clickhouse.integration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseContainerForTest;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.domain.ClickHouseDataType;
import ru.yandex.clickhouse.util.ClickHouseBitmap;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryInputStream;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;
import ru.yandex.clickhouse.util.ClickHouseVersionNumberUtil;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class RowBinaryStreamTest {

    private ClickHouseDataSource dataSource;
    private ClickHouseConnection connection;

    @BeforeTest
    public void setUp() throws Exception {
        dataSource = ClickHouseContainerForTest.newDataSource();
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
                        "uuid UUID," +
                        "lowCardinality LowCardinality(String)," +
                        "fixedString FixedString(15)" +
                        ") ENGINE = MergeTree partition by toYYYYMM(date) order by date"
        );
    }

    @Test
    public void multiRowTest() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.big_data");
        connection.createStatement().execute(
                "CREATE TABLE test.big_data (value Int32) ENGINE = TinyLog()"
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
        assertEquals(rs.getInt("cnt"), count);
        assertEquals(rs.getLong("sum"), sum.get());
    }


    @Test
    public void testRowBinaryStream() throws Exception {
        testRowBinaryStream(false);
    }

    @Test
    public void testRowBinaryInputStream() throws Exception {
        testRowBinaryStream(true);
    }

    private String createtestBitmapTable(ClickHouseDataType innerType) throws Exception {
        String tableName = "test_binary_rb_" + innerType.name();
        String arrType = "Array(" + innerType.name() + ")";
        String rbTypeName = "AggregateFunction(groupBitmap, " + innerType.name() + ")";
        try (ClickHouseStatement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + tableName);
            statement.execute("CREATE TABLE IF NOT EXISTS " + tableName + 
                "(i UInt8, a " + arrType + ", b " + rbTypeName + ") engine=Memory");
        }

        return tableName;
    }

    private int[] genRoaringBitmapValues(int length, ClickHouseDataType innerType) {
        int[] values = new int[length];

        for (int i = 0; i < length; i++) {
            values[i] = i;
        }

        return values;
    }

    private long[] gen64BitmapValues(int length, long base, long step) {
        long[] values = new long[length];

        for (int i = 0; i < length; i++) {
            values[i] = base + i * step;
        }

        return values;
    }

    private void writeValues(ClickHouseRowBinaryStream stream,
        int [] values, ClickHouseDataType innerType) throws IOException {
        switch (innerType) {
            case Int8:
            case UInt8:
                stream.writeUInt8Array(values);
                break;
            case Int16:
            case UInt16:
                stream.writeUInt16Array(values);
                break;
            case Int32:
            case UInt32:
                stream.writeInt32Array(values);
                break;
            default:
                throw new IllegalArgumentException(innerType.name() + " is not supported!");
        }
    }

    private void testBitmap(ClickHouseDataType innerType, int valueLength) throws Exception {
        try (ClickHouseStatement statement = connection.createStatement()) {
            String tableName = createtestBitmapTable(innerType);
            int[] values = genRoaringBitmapValues(valueLength, innerType);
            statement.sendRowBinaryStream("insert into table " + tableName, new ClickHouseStreamCallback() {
                @Override
                public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                    stream.writeByte((byte) 1);
                    writeValues(stream, values, innerType);
                    stream.writeBitmap(ClickHouseBitmap.wrap(RoaringBitmap.bitmapOf(values), innerType));
                    stream.writeByte((byte) 2);
                    writeValues(stream, values, innerType);
                    stream.writeBitmap(ClickHouseBitmap.wrap(ImmutableRoaringBitmap.bitmapOf(values), innerType));
                    stream.writeByte((byte) 3);
                    writeValues(stream, values, innerType);
                    stream.writeBitmap(ClickHouseBitmap.wrap(MutableRoaringBitmap.bitmapOf(values), innerType));
                }
            });

            for (int i = 0; i < 3; i++) {
                String sql = "select b = bitmapBuild(a) ? 1 : 0 from " + tableName + " where i = " + (i+1);
                try (ResultSet rs = statement.executeQuery(sql)) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), 1);
                    assertFalse(rs.next());
                }

                sql = "select b from " + tableName + " where i = " + (i+1);
                try (ClickHouseRowBinaryInputStream in = statement.executeQueryClickhouseRowBinaryStream(sql)) {
                    assertEquals(in.readBitmap(innerType), ClickHouseBitmap.wrap(RoaringBitmap.bitmapOf(values), innerType));
                }
            }

            statement.execute("drop table if exists " + tableName);
        }
    }

    private void testBitmap64(int valueLength, long base, long step) throws Exception {
        ClickHouseDataType innerType = ClickHouseDataType.UInt64;
        try (ClickHouseStatement statement = connection.createStatement()) {
            String tableName = createtestBitmapTable(innerType);
            long[] values = gen64BitmapValues(valueLength, base, step);
            statement.sendRowBinaryStream("insert into table " + tableName, new ClickHouseStreamCallback() {
                @Override
                public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                    stream.writeByte((byte) 1);
                    stream.writeUInt64Array(values);
                    stream.writeBitmap(ClickHouseBitmap.wrap(Roaring64NavigableMap.bitmapOf(values), ClickHouseDataType.UInt64));
                    stream.writeByte((byte) 2);
                    stream.writeUInt64Array(values);
                    stream.writeBitmap(ClickHouseBitmap.wrap(Roaring64Bitmap.bitmapOf(values), ClickHouseDataType.UInt64));
                }
            });

            String sql = "select bitmapBuild(a) = b ? 1 : 0 from " + tableName + " order by i";
            try (ResultSet rs = statement.executeQuery(sql)) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 1);
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 1);
                assertFalse(rs.next());
            }

            sql = "select b from " + tableName + " order by i";
            try (ClickHouseRowBinaryInputStream in = statement.executeQueryClickhouseRowBinaryStream(sql)) {
                assertEquals(in.readBitmap(innerType), ClickHouseBitmap.wrap(Roaring64NavigableMap.bitmapOf(values), innerType));
            }

            statement.execute("drop table if exists " + tableName);
        }
    }
    
    @Test
    public void testBitmap() throws Exception {
        // TODO seems Int8, Int16 and Int32 are still not supported in ClickHouse
        testBitmap(ClickHouseDataType.UInt8, 32);
        testBitmap(ClickHouseDataType.UInt8, 256);
        testBitmap(ClickHouseDataType.UInt16, 32);
        testBitmap(ClickHouseDataType.UInt16, 65536);
        testBitmap(ClickHouseDataType.UInt32, 32);
        testBitmap(ClickHouseDataType.UInt32, 65537);

        testBitmap64(32, 0L, 1L);
        testBitmap64(32, Long.MAX_VALUE, -1L);

        String versionNumber = connection.getServerVersion();
        int majorVersion = ClickHouseVersionNumberUtil.getMajorVersion(versionNumber);
        int minorVersion = ClickHouseVersionNumberUtil.getMinorVersion(versionNumber);
        if (majorVersion > 20 || (majorVersion == 20 && minorVersion > 8)) {
            testBitmap64(65537, 100000L, 1L); // highToBitmap.size() == 1
            testBitmap64(65537, 9223372036854775807L, -1000000000L); // highToBitmap.size() > 1
        }
    }

    @Test
    public void testBigDecimals() throws Exception {
        try (ClickHouseStatement statement = connection.createStatement()) {
            statement.execute("set allow_experimental_bigint_types=1;"
                + "create table if not exists test.test_big_decimals(d128 Decimal128(6), d256 Decimal256(12)) engine=Memory");
        } catch (SQLException e) {
            return;
        }

        try (ClickHouseStatement statement = connection.createStatement()) {
            BigDecimal[] values = new BigDecimal[] {
                BigDecimal.valueOf(-123.123456789D),
                BigDecimal.ZERO,
                BigDecimal.valueOf(123.123456789D)
            };
            statement.sendRowBinaryStream("insert into table test.test_big_decimals", new ClickHouseStreamCallback() {
                @Override
                public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                    for (int i = 0; i < values.length; i++) {
                        stream.writeDecimal128(values[i], 6);
                        stream.writeDecimal256(values[i], 12);
                    }
                }
            });

            try (ResultSet rs = statement.executeQuery("select * from test.test_big_decimals order by d128")) {
                int rowCounter = 0;
                while (rs.next()) {
                    rowCounter++;
                    assertEquals(rs.getBigDecimal(1, 6), values[rowCounter - 1].setScale(6, BigDecimal.ROUND_DOWN));
                    assertEquals(rs.getBigDecimal(2, 12), values[rowCounter - 1].setScale(12, BigDecimal.ROUND_DOWN));
                }

                assertEquals(rowCounter, values.length);
            }

            statement.execute("drop table if exists test.test_big_decimals");
        }
    }

    private void testRowBinaryStream(boolean rowBinaryResult) throws Exception {
        createTable("test.raw_binary");
        ClickHouseStatement statement = connection.createStatement();
        final long timestamp = 1483230102000L; //2017-01-01 03:21:42
        final Date date1 = new Date(timestamp);
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
                        "(date, dateTime, string, int8, uInt8, int16, uInt16, int32, uInt32, int64, uInt64, float32, float64, dateArray, dateTimeArray, stringArray, int8Array, uInt8Array, int16Array, uInt16Array, int32Array, uInt32Array, int64Array, uInt64Array, float32Array, float64Array, uuid, lowCardinality, fixedString)",
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
                        stream.writeString("lowCardinality\n1");
                        stream.writeFixedString("fixedString1", 15);

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
                        stream.writeUInt64(new BigInteger(Long.toUnsignedString(-1L)));
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
                        stream.writeString("lowCardinality\n2");
                        stream.writeFixedString("fixedString2", 15);
                    }
                }
        );

        if (!rowBinaryResult) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM test.raw_binary ORDER BY date");

            Assert.assertTrue(rs.next());
            assertEquals(rs.getString("date"), 
                Instant.ofEpochMilli(timestamp).atZone(connection.getTimeZone().toZoneId())
                    .withZoneSameInstant(ZoneId.systemDefault()).toLocalDate().toString());
            assertEquals(rs.getTimestamp("dateTime").getTime(), date1.getTime());
            assertEquals(rs.getString("string"), "string\n1");
            assertEquals(rs.getInt("int8"), Byte.MIN_VALUE);
            assertEquals(rs.getInt("uInt8"), 0);
            assertEquals(rs.getInt("int16"), Short.MIN_VALUE);
            assertEquals(rs.getInt("uInt16"), 0);
            assertEquals(rs.getInt("int32"), Integer.MIN_VALUE);
            assertEquals(rs.getInt("uInt32"), 0);
            assertEquals(rs.getLong("int64"), Long.MIN_VALUE);
            assertEquals(rs.getLong("uInt64"), 0);
            assertEquals(rs.getDouble("float32"), 123.456);
            assertEquals(rs.getDouble("float64"), 42.21);
            assertEquals(rs.getObject("uuid").toString(), "123e4567-e89b-12d3-a456-426655440000");
            assertEquals(rs.getString("lowCardinality"), "lowCardinality\n1");
            assertEquals(rs.getString("fixedString"), "fixedString1\0\0\0");

            Date[] expectedDates1 = new Date[dates1.length];
            for (int i = 0; i < dates1.length; i++) {
                // expected is Date at start of the day in local timezone
                expectedDates1[i] = withTimeAtStartOfDay(dates1[i]);
            }

            assertEquals(rs.getArray("dateArray").getArray(), expectedDates1);
            assertEquals(rs.getArray("dateTimeArray").getArray(), dateTimes1);

            assertEquals(rs.getArray("stringArray").getArray(), strings1);

            assertArrayEquals((int[]) rs.getArray("int8Array").getArray(), int8s1);
            assertEquals(rs.getArray("uInt8Array").getArray(), uint8s1);

            assertArrayEquals((int[]) rs.getArray("int16Array").getArray(), int16s1);
            assertEquals(rs.getArray("uInt16Array").getArray(), uint16s1);

            assertEquals(rs.getArray("int32Array").getArray(), int32s1);
            assertEquals(rs.getArray("uInt32Array").getArray(), uint32s1);

            assertEquals(rs.getArray("int64Array").getArray(), int64s1);
            assertArrayEquals((BigInteger[]) rs.getArray("uInt64Array").getArray(), uint64s1);

            assertEquals(rs.getArray("float32Array").getArray(), float32s1);
            assertEquals(rs.getArray("float64Array").getArray(), float64s1);

            Assert.assertTrue(rs.next());
            assertEquals(rs.getString("date"), "2017-05-09");
            assertEquals(rs.getTimestamp("dateTime").getTime(), date2.getTime());
            assertEquals(rs.getString("string"), "a\tbdasd''a");
            assertEquals(rs.getInt("int8"), Byte.MAX_VALUE);
            assertEquals(rs.getInt("uInt8"), 255);
            assertEquals(rs.getInt("int16"), Short.MAX_VALUE);
            assertEquals(rs.getInt("uInt16"), 42000);
            assertEquals(rs.getInt("int32"), Integer.MAX_VALUE);
            assertEquals(rs.getLong("uInt32"), 2147483747L);
            assertEquals(rs.getLong("int64"), Long.MAX_VALUE);
            assertEquals(rs.getString("uInt64"), "18446744073709551615");
            assertEquals(rs.getDouble("float32"), 21.21);
            assertEquals(rs.getDouble("float64"), 77.77);
            assertEquals(rs.getString("uuid"), "789e0123-e89b-12d3-a456-426655444444");
            assertEquals(rs.getString("lowCardinality"), "lowCardinality\n2");
            assertEquals(rs.getString("fixedString"), "fixedString2\0\0\0");

            Assert.assertFalse(rs.next());
        } else {
            ClickHouseRowBinaryInputStream is = connection.createStatement().executeQueryClickhouseRowBinaryStream(
                "SELECT * FROM test.raw_binary ORDER BY date");

            assertEquals(is.readDate(), withTimeAtStartOfDay(date1));
            assertEquals(is.readDateTime(), new Timestamp(timestamp));
            assertEquals(is.readString(), "string\n1");
            assertEquals(is.readInt8(), Byte.MIN_VALUE);
            assertEquals(is.readUInt8(), (short) 0);
            assertEquals(is.readInt16(), Short.MIN_VALUE);
            assertEquals(is.readUInt16(), 0);
            assertEquals(is.readInt32(), Integer.MIN_VALUE);
            assertEquals(is.readUInt32(), 0);
            assertEquals(is.readInt64(), Long.MIN_VALUE);
            assertEquals(is.readUInt64(), BigInteger.valueOf(0));
            assertEquals(is.readFloat32(), (float) 123.456);
            assertEquals(is.readFloat64(), 42.21);

            Date[] expectedDates1 = new Date[dates1.length];
            for (int i = 0; i < dates1.length; i++) {
                // expected is Date at start of the day in local timezone
                expectedDates1[i] = withTimeAtStartOfDay(dates1[i]);
            }

            assertEquals(is.readDateArray(), expectedDates1);
            assertEquals(is.readDateTimeArray(), dateTimes1);

            assertEquals(is.readStringArray(), strings1);

            assertEquals(is.readInt8Array(), int8s1);
            assertArrayEquals(is.readUInt8Array(), uint8s1);

            assertEquals(is.readInt16Array(), int16s1);
            assertEquals(is.readUInt16Array(), uint16s1);

            assertEquals(is.readInt32Array(), int32s1);
            assertEquals(is.readUInt32Array(), uint32s1);

            assertEquals(is.readInt64Array(), int64s1);
            assertArrayEquals(is.readUInt64Array(), uint64s1);

            assertEquals(is.readFloat32Array(), float32s1);
            assertEquals(is.readFloat64Array(), float64s1);

            assertEquals(is.readUUID(), uuid1);
            assertEquals(is.readString(), "lowCardinality\n1");

            assertEquals(is.readDate(), withTimeAtStartOfDay(date2));
            assertEquals(is.readDateTime().getTime(), date2.getTime());
            assertEquals(is.readString(), "a\tbdasd''a");
            assertEquals(is.readInt8(), Byte.MAX_VALUE);
            assertEquals(is.readUInt8(), (short) 255);
            assertEquals(is.readInt16(), Short.MAX_VALUE);
            assertEquals(is.readUInt16(), 42000);
            assertEquals(is.readInt32(), Integer.MAX_VALUE);
            assertEquals(is.readUInt32(), (Integer.MAX_VALUE) + 100L);
            assertEquals(is.readInt64(), Long.MAX_VALUE);
            assertEquals(is.readUInt64(), new BigInteger(Long.toUnsignedString(-1L)));
            assertEquals(is.readFloat32(), (float) 21.21);
            assertEquals(is.readFloat64(), 77.77);

            // skip arrays
            is.readDateArray();
            is.readDateTimeArray();
            is.readStringArray();
            is.readInt8Array();
            is.readUInt8Array();
            is.readInt16Array();
            is.readUInt16Array();
            is.readInt32Array();
            is.readUInt32Array();
            is.readInt64Array();
            is.readUInt64Array();
            is.readFloat32Array();
            is.readFloat64Array();

            assertEquals(is.readUUID(), uuid2);
            assertEquals(is.readString(), "lowCardinality\n2");
            assertEquals(is.readString(), "fixedString2\0\0\0");

            // check EOF
            try {
                is.readInt8();
                Assert.fail();
            } catch (EOFException e) {
                // expected
            }
        }
    }


    @Test
    public void testTimeZone() throws Exception{
        final ClickHouseStatement statement = connection.createStatement();
        connection.createStatement().execute("DROP TABLE IF EXISTS test.binary_tz");
        connection.createStatement().execute(
            "CREATE TABLE test.binary_tz (date Date, dateTime DateTime) ENGINE = MergeTree(date, (date), 8192)"
        );

        //
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

        /*
         * The following, commented-out assertion would be nice, but against the
         * definition of the Time class:
         *
         * "The date components should be set to the "zero epoch" value of
         * January 1, 1970 and should not be accessed."
         *
         * assertEquals(rs.getTime("dateTime"), new Time(date1.getTime()));
         *
         * The original timestamp is 2017-06-14 21:00:18 (UTC), so we expect
         * 21:00:18 as the local time, regardless of a different DST offset
         */
        assertEquals(
            Instant.ofEpochMilli(rs.getTime("dateTime").getTime())
                .atZone(ZoneId.of("UTC"))
                .toLocalTime(),
            LocalTime.of(21, 0, 18));

        Date expectedDate = withTimeAtStartOfDay(date1); // expected start of the day in local timezone
        assertEquals(rs.getDate("date"), expectedDate);

        assertEquals(rs.getTimestamp("dateTime"), new Timestamp(date1.getTime()));
    }

    private static Date withTimeAtStartOfDay(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Date(cal.getTimeInMillis());
    }

    private static void assertArrayEquals(int[] actual, byte[] expected) {
        int[] expectedInts = new int[expected.length];
        for (int i = 0; i < expected.length; i++) {
            expectedInts[i] = expected[i];
        }
        assertEquals(actual, expectedInts);
    }

    private static void assertArrayEquals(int[] actual, short[] expected) {
        int[] expectedInts = new int[expected.length];
        for (int i = 0; i < expected.length; i++) {
            expectedInts[i] = expected[i];
        }

        assertEquals(actual, expectedInts);
    }

    private static void assertArrayEquals(short[] actual, int[] expected) {
        int[] actualInts = new int[actual.length];
        for (int i = 0; i < actual.length; i++) {
            actualInts[i] = actual[i];
        }

        assertEquals(actualInts, expected);
    }

    private static void assertArrayEquals(long[] actual, int[] expected) {
        long[] expectedLongs = new long[expected.length];
        for (int i = 0; i < expected.length; i++) {
            expectedLongs[i] = expected[i];
        }

        assertEquals(actual, expectedLongs);

    }

    private static void assertArrayEquals(BigInteger[] actual, long[] expected) {
        BigInteger[] expectedBigs = new BigInteger[expected.length];
        for (int i = 0; i < expected.length; i++) {
            expectedBigs[i] = BigInteger.valueOf(expected[i]);
        }

        assertEquals(actual, expectedBigs);
    }
}
