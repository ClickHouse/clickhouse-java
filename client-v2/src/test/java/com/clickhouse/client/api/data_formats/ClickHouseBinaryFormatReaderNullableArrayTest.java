package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

/**
 * Test coverage for nullable array types in ClickHouseBinaryFormatReader.
 * This complements the existing ClickHouseBinaryFormatReaderTest.
 */
public class ClickHouseBinaryFormatReaderNullableArrayTest {

    @Test
    public void testNullableArrays() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String[] names = new String[]{"nullable_int_arr", "nullable_str_arr", "nullable_bool_arr", 
                                   "nullable_double_arr", "nullable_long_arr"};
        String[] types = new String[]{"Array(Nullable(Int32))", "Array(Nullable(String))", 
                                   "Array(Nullable(Bool))", "Array(Nullable(Float64))",
                                   "Array(Nullable(Int64))"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // Array(Nullable(Int32)): [1, NULL, 3, NULL, 5]
        BinaryStreamUtils.writeVarInt(out, 5);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeInt32(out, 1);
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeInt32(out, 3);
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeInt32(out, 5);

        // Array(Nullable(String)): ["a", NULL, "c", NULL]
        BinaryStreamUtils.writeVarInt(out, 4);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeString(out, "a");
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeString(out, "c");
        BinaryStreamUtils.writeNull(out);

        // Array(Nullable(Bool)): [true, NULL, false]
        BinaryStreamUtils.writeVarInt(out, 3);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeBoolean(out, true);
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeBoolean(out, false);

        // Array(Nullable(Float64)): [1.5, NULL, 2.5]
        BinaryStreamUtils.writeVarInt(out, 3);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeFloat64(out, 1.5);
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeFloat64(out, 2.5);

        // Array(Nullable(Int64)): [100L, NULL, 300L]
        BinaryStreamUtils.writeVarInt(out, 3);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeInt64(out, 100L);
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeInt64(out, 300L);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        // Test nullable arrays with column names
        Object[] nullableIntResult = reader.getObjectArray("nullable_int_arr");
        Assert.assertNotNull(nullableIntResult);
        Assert.assertEquals(nullableIntResult.length, 5);
        Assert.assertEquals(nullableIntResult[0], 1);
        Assert.assertNull(nullableIntResult[1]);
        Assert.assertEquals(nullableIntResult[2], 3);
        Assert.assertNull(nullableIntResult[3]);
        Assert.assertEquals(nullableIntResult[4], 5);

        Object[] nullableStrResult = reader.getObjectArray("nullable_str_arr");
        Assert.assertNotNull(nullableStrResult);
        Assert.assertEquals(nullableStrResult.length, 4);
        Assert.assertEquals(nullableStrResult[0], "a");
        Assert.assertNull(nullableStrResult[1]);
        Assert.assertEquals(nullableStrResult[2], "c");
        Assert.assertNull(nullableStrResult[3]);

        Object[] nullableBoolResult = reader.getObjectArray("nullable_bool_arr");
        Assert.assertNotNull(nullableBoolResult);
        Assert.assertEquals(nullableBoolResult.length, 3);
        Assert.assertEquals(nullableBoolResult[0], true);
        Assert.assertNull(nullableBoolResult[1]);
        Assert.assertEquals(nullableBoolResult[2], false);

        Object[] nullableDoubleResult = reader.getObjectArray("nullable_double_arr");
        Assert.assertNotNull(nullableDoubleResult);
        Assert.assertEquals(nullableDoubleResult.length, 3);
        Assert.assertEquals(nullableDoubleResult[0], 1.5);
        Assert.assertNull(nullableDoubleResult[1]);
        Assert.assertEquals(nullableDoubleResult[2], 2.5);

        Object[] nullableLongResult = reader.getObjectArray("nullable_long_arr");
        Assert.assertNotNull(nullableLongResult);
        Assert.assertEquals(nullableLongResult.length, 3);
        Assert.assertEquals(nullableLongResult[0], 100L);
        Assert.assertNull(nullableLongResult[1]);
        Assert.assertEquals(nullableLongResult[2], 300L);

        // Test getList with nullable arrays
        List<Object> nullableIntList = reader.getList("nullable_int_arr");
        Assert.assertEquals(nullableIntList.size(), 5);
        Assert.assertEquals(nullableIntList.get(0), 1);
        Assert.assertNull(nullableIntList.get(1));
        Assert.assertEquals(nullableIntList.get(2), 3);
        Assert.assertNull(nullableIntList.get(3));
        Assert.assertEquals(nullableIntList.get(4), 5);

        List<Object> nullableStrList = reader.getList("nullable_str_arr");
        Assert.assertEquals(nullableStrList.size(), 4);
        Assert.assertEquals(nullableStrList.get(0), "a");
        Assert.assertNull(nullableStrList.get(1));
        Assert.assertEquals(nullableStrList.get(2), "c");
        Assert.assertNull(nullableStrList.get(3));
    }

    @Test
    public void testNullableArraysByIndex() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String[] names = new String[]{"nullable_int_arr", "nullable_str_arr", "nullable_bool_arr"};
        String[] types = new String[]{"Array(Nullable(Int32))", "Array(Nullable(String))", "Array(Nullable(Bool))"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // Array(Nullable(Int32)): [10, NULL, 30]
        BinaryStreamUtils.writeVarInt(out, 3);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeInt32(out, 10);
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeInt32(out, 30);

        // Array(Nullable(String)): ["x", NULL, "z"]
        BinaryStreamUtils.writeVarInt(out, 3);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeString(out, "x");
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeString(out, "z");

        // Array(Nullable(Bool)): [false, NULL, true]
        BinaryStreamUtils.writeVarInt(out, 3);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeBoolean(out, false);
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeBoolean(out, true);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        // Test nullable arrays with index parameters
        Object[] nullableIntResult = reader.getObjectArray(1);
        Assert.assertNotNull(nullableIntResult);
        Assert.assertEquals(nullableIntResult.length, 3);
        Assert.assertEquals(nullableIntResult[0], 10);
        Assert.assertNull(nullableIntResult[1]);
        Assert.assertEquals(nullableIntResult[2], 30);

        Object[] nullableStrResult = reader.getObjectArray(2);
        Assert.assertNotNull(nullableStrResult);
        Assert.assertEquals(nullableStrResult.length, 3);
        Assert.assertEquals(nullableStrResult[0], "x");
        Assert.assertNull(nullableStrResult[1]);
        Assert.assertEquals(nullableStrResult[2], "z");

        Object[] nullableBoolResult = reader.getObjectArray(3);
        Assert.assertNotNull(nullableBoolResult);
        Assert.assertEquals(nullableBoolResult.length, 3);
        Assert.assertEquals(nullableBoolResult[0], false);
        Assert.assertNull(nullableBoolResult[1]);
        Assert.assertEquals(nullableBoolResult[2], true);

        // Test getList with index parameters
        List<Object> nullableIntList = reader.getList(1);
        Assert.assertEquals(nullableIntList.size(), 3);
        Assert.assertEquals(nullableIntList.get(0), 10);
        Assert.assertNull(nullableIntList.get(1));
        Assert.assertEquals(nullableIntList.get(2), 30);

        List<Object> nullableStrList = reader.getList(2);
        Assert.assertEquals(nullableStrList.size(), 3);
        Assert.assertEquals(nullableStrList.get(0), "x");
        Assert.assertNull(nullableStrList.get(1));
        Assert.assertEquals(nullableStrList.get(2), "z");
    }

    @Test
    public void testEmptyNullableArrays() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String[] names = new String[]{"empty_nullable_int", "empty_nullable_str"};
        String[] types = new String[]{"Array(Nullable(Int32))", "Array(Nullable(String))"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // Empty nullable arrays
        BinaryStreamUtils.writeVarInt(out, 0); // empty_nullable_int
        BinaryStreamUtils.writeVarInt(out, 0); // empty_nullable_str

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        // Test empty nullable arrays
        Object[] emptyIntResult = reader.getObjectArray("empty_nullable_int");
        Assert.assertNotNull(emptyIntResult);
        Assert.assertEquals(emptyIntResult.length, 0);

        Object[] emptyStrResult = reader.getObjectArray("empty_nullable_str");
        Assert.assertNotNull(emptyStrResult);
        Assert.assertEquals(emptyStrResult.length, 0);

        List<Object> emptyIntList = reader.getList("empty_nullable_int");
        Assert.assertEquals(emptyIntList.size(), 0);

        List<Object> emptyStrList = reader.getList("empty_nullable_str");
        Assert.assertEquals(emptyStrList.size(), 0);
    }

    @Test
    public void testNullableArraysWithAllNulls() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String[] names = new String[]{"all_null_int", "all_null_str"};
        String[] types = new String[]{"Array(Nullable(Int32))", "Array(Nullable(String))"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // Array(Nullable(Int32)): [NULL, NULL, NULL]
        BinaryStreamUtils.writeVarInt(out, 3);
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNull(out);

        // Array(Nullable(String)): [NULL, NULL]
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNull(out);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        // Test arrays with all nulls
        Object[] allNullIntResult = reader.getObjectArray("all_null_int");
        Assert.assertNotNull(allNullIntResult);
        Assert.assertEquals(allNullIntResult.length, 3);
        Assert.assertNull(allNullIntResult[0]);
        Assert.assertNull(allNullIntResult[1]);
        Assert.assertNull(allNullIntResult[2]);

        Object[] allNullStrResult = reader.getObjectArray("all_null_str");
        Assert.assertNotNull(allNullStrResult);
        Assert.assertEquals(allNullStrResult.length, 2);
        Assert.assertNull(allNullStrResult[0]);
        Assert.assertNull(allNullStrResult[1]);

        List<Object> allNullIntList = reader.getList("all_null_int");
        Assert.assertEquals(allNullIntList.size(), 3);
        Assert.assertNull(allNullIntList.get(0));
        Assert.assertNull(allNullIntList.get(1));
        Assert.assertNull(allNullIntList.get(2));

        List<Object> allNullStrList = reader.getList("all_null_str");
        Assert.assertEquals(allNullStrList.size(), 2);
        Assert.assertNull(allNullStrList.get(0));
        Assert.assertNull(allNullStrList.get(1));
    }

    @Test
    public void testNullableArraysWithMixedTypes() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String[] names = new String[]{"mixed_nullable_arr"};
        String[] types = new String[]{"Array(Nullable(Int64))"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // Array(Nullable(Int64)): [1L, NULL, 3L, NULL, 5L]
        BinaryStreamUtils.writeVarInt(out, 5);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeInt64(out, 1L);
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeInt64(out, 3L);
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeInt64(out, 5L);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        // Test mixed nullable array
        Object[] mixedResult = reader.getObjectArray("mixed_nullable_arr");
        Assert.assertNotNull(mixedResult);
        Assert.assertEquals(mixedResult.length, 5);
        Assert.assertEquals(mixedResult[0], 1L);
        Assert.assertNull(mixedResult[1]);
        Assert.assertEquals(mixedResult[2], 3L);
        Assert.assertNull(mixedResult[3]);
        Assert.assertEquals(mixedResult[4], 5L);

        List<Object> mixedList = reader.getList("mixed_nullable_arr");
        Assert.assertEquals(mixedList.size(), 5);
        Assert.assertEquals(mixedList.get(0), 1L);
        Assert.assertNull(mixedList.get(1));
        Assert.assertEquals(mixedList.get(2), 3L);
        Assert.assertNull(mixedList.get(3));
        Assert.assertEquals(mixedList.get(4), 5L);
    }

    @Test
    public void testNullableArraysOfTypesReadAsOtherClasses() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String[] names = new String[]{"uint64_arr", "datetime64_arr", "ipv6_arr", "all_null_uint64_arr"};
        String[] types = new String[]{"Array(Nullable(UInt64))", "Array(Nullable(DateTime64(3, 'UTC')))",
                                   "Array(Nullable(IPv6))", "Array(Nullable(UInt64))"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // Array(Nullable(UInt64)): [0, NULL, 18446744073709551615]
        BigInteger maxUInt64 = new BigInteger("18446744073709551615");
        BinaryStreamUtils.writeVarInt(out, 3);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeUnsignedInt64(out, BigInteger.ZERO);
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeUnsignedInt64(out, maxUInt64);

        // Array(Nullable(DateTime64(3, 'UTC'))): ['2026-01-01 13:34:56.780', NULL]
        LocalDateTime dateTime = LocalDateTime.of(2026, 1, 1, 13, 34, 56, 780_000_000);
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeDateTime64(out, dateTime, 3, TimeZone.getTimeZone("UTC"));
        BinaryStreamUtils.writeNull(out);

        // Array(Nullable(IPv6)): ['::ffff:1.2.3.4', NULL, '2001:db8::1']
        // An IPv4-mapped address is read as Inet4Address, the other one as Inet6Address.
        BinaryStreamUtils.writeVarInt(out, 3);
        BinaryStreamUtils.writeNonNull(out);
        out.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xff, (byte) 0xff, 1, 2, 3, 4});
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNonNull(out);
        out.write(InetAddress.getByName("2001:db8::1").getAddress());

        // Array(Nullable(UInt64)): [NULL, NULL]
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeNull(out);
        BinaryStreamUtils.writeNull(out);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        List<Object> uint64List = reader.getList("uint64_arr");
        Assert.assertEquals(uint64List, Arrays.asList(BigInteger.ZERO, null, maxUInt64));

        List<Object> dateTimeList = reader.getList("datetime64_arr");
        Assert.assertEquals(dateTimeList, Arrays.asList(dateTime.atZone(ZoneId.of("UTC")), null));

        List<Object> ipv6List = reader.getList("ipv6_arr");
        Assert.assertEquals(ipv6List, Arrays.asList(InetAddress.getByName("1.2.3.4"), null,
                InetAddress.getByName("2001:db8::1")));

        List<Object> allNullList = reader.getList("all_null_uint64_arr");
        Assert.assertEquals(allNullList, Arrays.asList(null, null));
    }

    @Test
    public void testNullableArraysOfArrayBackedTypes() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String[] names = new String[]{"point_arr", "ring_arr", "tuple_arr"};
        String[] types = new String[]{"Array(Nullable(Point))", "Array(Nullable(Ring))",
                                   "Array(Nullable(Tuple(Int32, Int32)))"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // Array(Nullable(Point)): [(1.5, 2.5), NULL]
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeFloat64(out, 1.5);
        BinaryStreamUtils.writeFloat64(out, 2.5);
        BinaryStreamUtils.writeNull(out);

        // Array(Nullable(Ring)): [[(1.5, 2.5)], NULL]
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeVarInt(out, 1);
        BinaryStreamUtils.writeFloat64(out, 1.5);
        BinaryStreamUtils.writeFloat64(out, 2.5);
        BinaryStreamUtils.writeNull(out);

        // Array(Nullable(Tuple(Int32, Int32))): [(1, 2), NULL]
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeNonNull(out);
        BinaryStreamUtils.writeInt32(out, 1);
        BinaryStreamUtils.writeInt32(out, 2);
        BinaryStreamUtils.writeNull(out);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        List<Object> points = reader.getList("point_arr");
        Assert.assertEquals(points.size(), 2);
        Assert.assertEquals((double[]) points.get(0), new double[]{1.5, 2.5});
        Assert.assertNull(points.get(1));

        List<Object> rings = reader.getList("ring_arr");
        Assert.assertEquals(rings.size(), 2);
        Assert.assertEquals(((double[][]) rings.get(0))[0], new double[]{1.5, 2.5});
        Assert.assertNull(rings.get(1));

        List<Object> tuples = reader.getList("tuple_arr");
        Assert.assertEquals(tuples.size(), 2);
        Assert.assertEquals((Object[]) tuples.get(0), new Object[]{1, 2});
        Assert.assertNull(tuples.get(1));
    }
}
