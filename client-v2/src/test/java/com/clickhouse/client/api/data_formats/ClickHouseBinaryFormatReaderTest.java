package com.clickhouse.client.api.data_formats;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.format.BinaryStreamUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Consumer;

public class ClickHouseBinaryFormatReaderTest {

    @Test
    public void testReadingNumbers() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String[] names = new String[]{ "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
            "q", "r"};
        String[] types = new String[]{"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64",
                "Int128", "Int256", "UInt128", "UInt256", "Float32", "Float64",
                "Decimal32(3)", "Decimal64(3)", "Decimal128(4)", "Decimal256(4)"};


        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        final int testValue = 120;
        BinaryStreamUtils.writeInt8(out, testValue);
        BinaryStreamUtils.writeInt16(out, testValue);
        BinaryStreamUtils.writeInt32(out, testValue);
        BinaryStreamUtils.writeInt64(out, testValue);
        BinaryStreamUtils.writeUnsignedInt8(out, testValue);
        BinaryStreamUtils.writeUnsignedInt16(out, testValue);
        BinaryStreamUtils.writeUnsignedInt32(out, testValue);
        BinaryStreamUtils.writeUnsignedInt64(out, testValue);
        BinaryStreamUtils.writeInt128( out, new BigInteger(String.valueOf(testValue )));
        BinaryStreamUtils.writeInt256( out, new BigInteger(String.valueOf(testValue )));
        BinaryStreamUtils.writeUnsignedInt128(out, new BigInteger(String.valueOf(testValue )));
        BinaryStreamUtils.writeUnsignedInt256(out, new BigInteger(String.valueOf(testValue )));
        BinaryStreamUtils.writeFloat32(out, testValue);
        BinaryStreamUtils.writeFloat64(out, testValue);
        BinaryStreamUtils.writeDecimal32(out, BigDecimal.valueOf(testValue), 3);
        BinaryStreamUtils.writeDecimal64(out, BigDecimal.valueOf(testValue), 3);
        BinaryStreamUtils.writeDecimal128(out, BigDecimal.valueOf(testValue), 4);
        BinaryStreamUtils.writeDecimal256(out, BigDecimal.valueOf(testValue), 4);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            Assert.assertEquals(reader.getBoolean(name), Boolean.TRUE);
            Assert.assertEquals(reader.getByte(name), (byte)testValue);
            Assert.assertEquals(reader.getShort(name), (short)testValue);
            Assert.assertEquals(reader.getInteger(name), testValue);
            Assert.assertEquals(reader.getLong(name), testValue);

            Assert.assertEquals(reader.getFloat(name), testValue);
            Assert.assertEquals(reader.getDouble(name), testValue);
            Assert.assertEquals(reader.getBigInteger(name), BigInteger.valueOf((testValue)));

            Assert.assertTrue(reader.getBigDecimal(name).compareTo(BigDecimal.valueOf((testValue))) == 0);
        }
    }

    @Test
    public void testReadingNumbersWithOverflow() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String[] names = new String[]{ "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
                "q", "r", "s"};
        String[] types = new String[]{"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64",
                "Int128", "Int256", "UInt128", "UInt256", "Float32", "Float64",
                "Decimal32(3)", "Decimal64(3)", "Decimal128(4)", "Decimal256(4)", "Float64"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        BinaryStreamUtils.writeInt8(out, 127); // a
        BinaryStreamUtils.writeInt16(out, 2000); // b
        BinaryStreamUtils.writeInt32(out, 300000); // c
        BinaryStreamUtils.writeInt64(out, 4000000000L); // d
        BinaryStreamUtils.writeUnsignedInt8(out, 255); // e
        BinaryStreamUtils.writeUnsignedInt16(out, 60000); // f
        BinaryStreamUtils.writeUnsignedInt32(out, 4000000000L); // g
        BinaryStreamUtils.writeUnsignedInt64(out, new BigInteger("18000044073709551615")); // h
        BinaryStreamUtils.writeInt128(out, new BigInteger("18000044073709551615")); // i
        BinaryStreamUtils.writeInt256(out, new BigInteger("18000044073709551615")); // j
        BinaryStreamUtils.writeUnsignedInt128(out, new BigInteger("18000044073709551615")); // k
        BinaryStreamUtils.writeUnsignedInt256(out, new BigInteger("18000044073709551615")); // l
        BinaryStreamUtils.writeFloat32(out,  900000.123f); // m
        BinaryStreamUtils.writeFloat64(out, 1000000.333); // n
        BinaryStreamUtils.writeDecimal32(out, BigDecimal.valueOf(100000), 3); // o
        BinaryStreamUtils.writeDecimal64(out, BigDecimal.valueOf(10000000.10000), 3); // p
        BinaryStreamUtils.writeDecimal128(out, BigDecimal.valueOf(1000000000.1000000), 4); // q
        BinaryStreamUtils.writeDecimal256(out, BigDecimal.valueOf(1000000000.1000000), 4); // r
        BinaryStreamUtils.writeFloat64(out, 123.456); // s

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        Consumer<String> byteConsumer = name -> Assert.expectThrows(ArithmeticException.class, () -> reader.getByte(name));
        Consumer<String> shortConsumer = name -> Assert.expectThrows(ArithmeticException.class, () -> reader.getShort(name));
        Consumer<String> integerConsumer = name -> Assert.expectThrows(ArithmeticException.class, () -> reader.getInteger(name));
        Consumer<String> longConsumer = name -> Assert.expectThrows(ArithmeticException.class, () -> reader.getLong(name));
//        Consumer<String> floatConsumer = name -> Assert.expectThrows(ArithmeticException.class, () -> reader.getFloat(name));
//        Consumer<String> doubleConsumer = name -> Assert.expectThrows(ArithmeticException.class, () -> reader.getDouble(name));

        Arrays.stream("b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r".split(",")).forEach(byteConsumer);
        Arrays.stream("c,d,f,g,h,i,j,k,l,m,n,o,p,q,r".split(",")).forEach(shortConsumer);
        Arrays.stream("d,g,h,i,j,k,l".split(",")).forEach(integerConsumer);
        Arrays.stream("h,i,j,k,l".split(",")).forEach(longConsumer);
//        Arrays.stream("h,i,j,k,l,n,p,q,r".split(",")).forEach(floatConsumer);
//        Arrays.stream("h,i,j,k,l,p,q,r".split(",")).forEach(doubleConsumer);

        Assert.assertEquals(reader.getFloat("m"), 900000.123f);
        Assert.assertEquals(reader.getDouble("m"), 900000.123f);

        Assert.assertEquals(reader.getBigDecimal("n"), BigDecimal.valueOf(1000000.333));
        Assert.assertEquals(reader.getBigDecimal("n"), BigDecimal.valueOf(1000000.333));

        Assert.assertEquals(reader.getFloat("s"), 123.456f);
    }

    @Test
    public void testReadingAsBoolean() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String[] names = new String[]{ "a", "b"};
        String[] types = new String[]{"Bool", "Bool"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }


        BinaryStreamUtils.writeBoolean(out, true);
        BinaryStreamUtils.writeBoolean(out, false);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        Assert.assertEquals(reader.getBoolean("a"), Boolean.TRUE);
        Assert.assertEquals(reader.getBoolean("b"), Boolean.FALSE);
        Assert.assertEquals(reader.getByte("a"), (byte) 1);
        Assert.assertEquals(reader.getByte("b"), (byte) 0);
        Assert.assertEquals(reader.getShort("a"), (short) 1);
        Assert.assertEquals(reader.getShort("b"), (short) 0);
        Assert.assertEquals(reader.getInteger("a"),  1);
        Assert.assertEquals(reader.getInteger("b"),  0);
        Assert.assertEquals(reader.getLong("a"),  1);
        Assert.assertEquals(reader.getLong("b"),  0);
        Assert.assertEquals(reader.getFloat("a"),  1.0f);
        Assert.assertEquals(reader.getFloat("b"),  0.0f);
        Assert.assertEquals(reader.getDouble("a"),  1.0d);
        Assert.assertEquals(reader.getDouble("b"),  0.0d);
        Assert.assertEquals(reader.getBigInteger("a"),  BigInteger.ONE);
        Assert.assertEquals(reader.getBigInteger("b"),  BigInteger.ZERO);
        Assert.assertEquals(reader.getBigDecimal("a"),  BigDecimal.ONE);
        Assert.assertEquals(reader.getBigDecimal("b"),  BigDecimal.ZERO);
        Assert.assertEquals(reader.getString("a"),  "true");
        Assert.assertEquals(reader.getString("b"),  "false");
    }

    @Test
    public void testReadingArrays() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String[] names = new String[]{ "a1", "a2", "a3", "a4", "a5"};
        String[] types = new String[]{"Array(Int8)", "Array(String)", "Array(Int16)", "Array(Int32)", "Array(Int64)"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // write data
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeInt8(out, (byte) 1);
        BinaryStreamUtils.writeInt8(out, (byte) 2);

        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeString(out, "a");
        BinaryStreamUtils.writeString(out, "b");

        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeInt16(out, (short) 1);
        BinaryStreamUtils.writeInt16(out, (short) 2);

        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeInt32(out, (int) 1);
        BinaryStreamUtils.writeInt32(out, (int) 2);

        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeInt64(out, (long) 1);
        BinaryStreamUtils.writeInt64(out, (long) 2);


        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        Assert.assertEquals(reader.getByteArray("a1"), new byte[] {(byte) 1, (byte) 2});
        Assert.assertEquals(reader.getStringArray("a2"), new String[] {"a", "b"});
        Assert.assertEquals(reader.getShortArray("a3"), new short[] {(short) 1, (short) 2});
        Assert.assertEquals(reader.getIntArray("a4"), new int[] {1, 2});
        Assert.assertEquals(reader.getLongArray("a5"), new long[] {1L, 2L});

    }

    @Test
    public void testGetObjectArray1D() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String[] names = new String[]{"uint64_arr", "enum_arr", "dt_arr", "fstr_arr", "str_arr"};
        String[] types = new String[]{
                "Array(UInt64)",
                "Array(Enum8('abc' = 1, 'cde' = 2))",
                "Array(DateTime('UTC'))",
                "Array(FixedString(4))",
                "Array(String)"
        };

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // Array(UInt64): [100, 200]
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeUnsignedInt64(out, BigInteger.valueOf(100));
        BinaryStreamUtils.writeUnsignedInt64(out, BigInteger.valueOf(200));

        // Array(Enum8('abc' = 1, 'cde' = 2)): [1, 2]
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeEnum8(out, (byte) 1);
        BinaryStreamUtils.writeEnum8(out, (byte) 2);

        // Array(DateTime('UTC')): two timestamps
        LocalDateTime dt1 = LocalDateTime.of(2030, 10, 9, 8, 7, 6);
        LocalDateTime dt2 = LocalDateTime.of(2031, 10, 9, 8, 7, 6);
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeDateTime32(out, dt1, TimeZone.getTimeZone("UTC"));
        BinaryStreamUtils.writeDateTime32(out, dt2, TimeZone.getTimeZone("UTC"));

        // Array(FixedString(4)): ["abcd", "efgh"]
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeFixedString(out, "abcd", 4);
        BinaryStreamUtils.writeFixedString(out, "efgh", 4);

        // Array(String): ["hello", "world"]
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeString(out, "hello");
        BinaryStreamUtils.writeString(out, "world");

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone("UTC");
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());
        reader.next();

        // Test Array(UInt64) via getObjectArray
        Object[] uint64Result = reader.getObjectArray("uint64_arr");
        Assert.assertNotNull(uint64Result);
        Assert.assertEquals(uint64Result.length, 2);
        Assert.assertEquals(uint64Result[0], BigInteger.valueOf(100));
        Assert.assertEquals(uint64Result[1], BigInteger.valueOf(200));

        // Test Array(Enum8) via getObjectArray
        Object[] enumResult = reader.getObjectArray("enum_arr");
        Assert.assertNotNull(enumResult);
        Assert.assertEquals(enumResult.length, 2);
        Assert.assertTrue(enumResult[0] instanceof BinaryStreamReader.EnumValue);
        Assert.assertEquals(enumResult[0].toString(), "abc");
        Assert.assertEquals(enumResult[1].toString(), "cde");

        // Test Array(Enum8) via getStringArray (sugar)
        String[] enumStrings = reader.getStringArray("enum_arr");
        Assert.assertEquals(enumStrings, new String[]{"abc", "cde"});

        // Test Array(DateTime) via getObjectArray
        Object[] dtResult = reader.getObjectArray("dt_arr");
        Assert.assertNotNull(dtResult);
        Assert.assertEquals(dtResult.length, 2);
        Assert.assertTrue(dtResult[0] instanceof ZonedDateTime);
        ZonedDateTime zdt1 = (ZonedDateTime) dtResult[0];
        ZonedDateTime zdt2 = (ZonedDateTime) dtResult[1];
        Assert.assertEquals(zdt1.toLocalDateTime(), dt1);
        Assert.assertEquals(zdt2.toLocalDateTime(), dt2);

        // Test Array(FixedString) via getObjectArray
        Object[] fstrResult = reader.getObjectArray("fstr_arr");
        Assert.assertNotNull(fstrResult);
        Assert.assertEquals(fstrResult.length, 2);
        Assert.assertEquals(fstrResult[0], "abcd");
        Assert.assertEquals(fstrResult[1], "efgh");

        // Test Array(String) via getObjectArray
        Object[] strResult = reader.getObjectArray("str_arr");
        Assert.assertNotNull(strResult);
        Assert.assertEquals(strResult.length, 2);
        Assert.assertEquals(strResult[0], "hello");
        Assert.assertEquals(strResult[1], "world");

        // Also verify getObjectArray works for primitive-backed arrays too
        // (int arrays are still returned as boxed objects)
    }

    @Test
    public void testGetObjectArray2D() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String[] names = new String[]{"arr2d_int", "arr2d_str"};
        String[] types = new String[]{"Array(Array(Int64))", "Array(Array(String))"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // Array(Array(Int64)): [[1, 2, 3], [4, 5]]
        BinaryStreamUtils.writeVarInt(out, 2); // outer array length
        BinaryStreamUtils.writeVarInt(out, 3); // inner[0] length
        BinaryStreamUtils.writeInt64(out, 1L);
        BinaryStreamUtils.writeInt64(out, 2L);
        BinaryStreamUtils.writeInt64(out, 3L);
        BinaryStreamUtils.writeVarInt(out, 2); // inner[1] length
        BinaryStreamUtils.writeInt64(out, 4L);
        BinaryStreamUtils.writeInt64(out, 5L);

        // Array(Array(String)): [["a", "b"], ["c"]]
        BinaryStreamUtils.writeVarInt(out, 2); // outer array length
        BinaryStreamUtils.writeVarInt(out, 2); // inner[0] length
        BinaryStreamUtils.writeString(out, "a");
        BinaryStreamUtils.writeString(out, "b");
        BinaryStreamUtils.writeVarInt(out, 1); // inner[1] length
        BinaryStreamUtils.writeString(out, "c");

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone("UTC");
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());
        reader.next();

        // Test 2D int array
        Object[] arr2dInt = reader.getObjectArray("arr2d_int");
        Assert.assertNotNull(arr2dInt);
        Assert.assertEquals(arr2dInt.length, 2);

        // Inner arrays should be Object[] (recursively converted)
        Assert.assertTrue(arr2dInt[0] instanceof Object[]);
        Assert.assertTrue(arr2dInt[1] instanceof Object[]);

        Object[] inner0 = (Object[]) arr2dInt[0];
        Object[] inner1 = (Object[]) arr2dInt[1];
        Assert.assertEquals(inner0.length, 3);
        Assert.assertEquals(inner0[0], 1L);
        Assert.assertEquals(inner0[1], 2L);
        Assert.assertEquals(inner0[2], 3L);
        Assert.assertEquals(inner1.length, 2);
        Assert.assertEquals(inner1[0], 4L);
        Assert.assertEquals(inner1[1], 5L);

        // Test 2D string array
        Object[] arr2dStr = reader.getObjectArray("arr2d_str");
        Assert.assertNotNull(arr2dStr);
        Assert.assertEquals(arr2dStr.length, 2);
        Assert.assertTrue(arr2dStr[0] instanceof Object[]);
        Assert.assertTrue(arr2dStr[1] instanceof Object[]);

        Object[] strInner0 = (Object[]) arr2dStr[0];
        Object[] strInner1 = (Object[]) arr2dStr[1];
        Assert.assertEquals(strInner0, new Object[]{"a", "b"});
        Assert.assertEquals(strInner1, new Object[]{"c"});
    }

    @Test
    public void testGetObjectArray3D() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String[] names = new String[]{"arr3d"};
        String[] types = new String[]{"Array(Array(Array(Int32)))"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // Array(Array(Array(Int32))): [[[1, 2], [3]], [[4]]]
        BinaryStreamUtils.writeVarInt(out, 2);   // dim1 length = 2
        // dim1[0] = [[1, 2], [3]]
        BinaryStreamUtils.writeVarInt(out, 2);   // dim2 length = 2
        BinaryStreamUtils.writeVarInt(out, 2);   // dim3 length = 2
        BinaryStreamUtils.writeInt32(out, 1);
        BinaryStreamUtils.writeInt32(out, 2);
        BinaryStreamUtils.writeVarInt(out, 1);   // dim3 length = 1
        BinaryStreamUtils.writeInt32(out, 3);
        // dim1[1] = [[4]]
        BinaryStreamUtils.writeVarInt(out, 1);   // dim2 length = 1
        BinaryStreamUtils.writeVarInt(out, 1);   // dim3 length = 1
        BinaryStreamUtils.writeInt32(out, 4);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone("UTC");
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());
        reader.next();

        // Test 3D array: [[[1, 2], [3]], [[4]]]
        Object[] arr3d = reader.getObjectArray("arr3d");
        Assert.assertNotNull(arr3d);
        Assert.assertEquals(arr3d.length, 2);

        // dim1[0] = [[1, 2], [3]]
        Assert.assertTrue(arr3d[0] instanceof Object[]);
        Object[] dim1_0 = (Object[]) arr3d[0];
        Assert.assertEquals(dim1_0.length, 2);

        // dim1[0][0] = [1, 2]
        Assert.assertTrue(dim1_0[0] instanceof Object[]);
        Object[] dim2_0_0 = (Object[]) dim1_0[0];
        Assert.assertEquals(dim2_0_0.length, 2);
        Assert.assertEquals(dim2_0_0[0], 1);
        Assert.assertEquals(dim2_0_0[1], 2);

        // dim1[0][1] = [3]
        Assert.assertTrue(dim1_0[1] instanceof Object[]);
        Object[] dim2_0_1 = (Object[]) dim1_0[1];
        Assert.assertEquals(dim2_0_1.length, 1);
        Assert.assertEquals(dim2_0_1[0], 3);

        // dim1[1] = [[4]]
        Assert.assertTrue(arr3d[1] instanceof Object[]);
        Object[] dim1_1 = (Object[]) arr3d[1];
        Assert.assertEquals(dim1_1.length, 1);

        Assert.assertTrue(dim1_1[0] instanceof Object[]);
        Object[] dim2_1_0 = (Object[]) dim1_1[0];
        Assert.assertEquals(dim2_1_0.length, 1);
        Assert.assertEquals(dim2_1_0[0], 4);
    }

    @Test
    public void testGetObjectArrayEmpty() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String[] names = new String[]{"empty_arr"};
        String[] types = new String[]{"Array(Int32)"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // Empty array
        BinaryStreamUtils.writeVarInt(out, 0);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone("UTC");
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());
        reader.next();

        Object[] result = reader.getObjectArray("empty_arr");
        Assert.assertNotNull(result);
        Assert.assertEquals(result.length, 0);
    }

    @Test
    public void testGetObjectArrayPrimitiveTypes() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String[] names = new String[]{"int_arr", "bool_arr", "float_arr"};
        String[] types = new String[]{"Array(Int32)", "Array(Bool)", "Array(Float64)"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // Array(Int32): [10, 20, 30]
        BinaryStreamUtils.writeVarInt(out, 3);
        BinaryStreamUtils.writeInt32(out, 10);
        BinaryStreamUtils.writeInt32(out, 20);
        BinaryStreamUtils.writeInt32(out, 30);

        // Array(Bool): [true, false]
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeBoolean(out, true);
        BinaryStreamUtils.writeBoolean(out, false);

        // Array(Float64): [1.5, 2.5]
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeFloat64(out, 1.5);
        BinaryStreamUtils.writeFloat64(out, 2.5);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone("UTC");
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());
        reader.next();

        // getObjectArray should work for primitive-backed arrays too (auto-boxes)
        Object[] intResult = reader.getObjectArray("int_arr");
        Assert.assertNotNull(intResult);
        Assert.assertEquals(intResult.length, 3);
        Assert.assertEquals(intResult[0], 10);
        Assert.assertEquals(intResult[1], 20);
        Assert.assertEquals(intResult[2], 30);

        Object[] boolResult = reader.getObjectArray("bool_arr");
        Assert.assertNotNull(boolResult);
        Assert.assertEquals(boolResult.length, 2);
        Assert.assertEquals(boolResult[0], true);
        Assert.assertEquals(boolResult[1], false);

        Object[] floatResult = reader.getObjectArray("float_arr");
        Assert.assertNotNull(floatResult);
        Assert.assertEquals(floatResult.length, 2);
        Assert.assertEquals(floatResult[0], 1.5);
        Assert.assertEquals(floatResult[1], 2.5);
    }

    @Test
    public void testReadingArraysByIndex() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String[] names = new String[]{"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8"};
        String[] types = new String[]{"Array(Int8)", "Array(String)", "Array(Int16)", "Array(Int32)",
                "Array(Int64)", "Array(Float32)", "Array(Float64)", "Array(Bool)"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // write data
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeInt8(out, (byte) 1);
        BinaryStreamUtils.writeInt8(out, (byte) 2);

        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeString(out, "a");
        BinaryStreamUtils.writeString(out, "b");

        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeInt16(out, (short) 1);
        BinaryStreamUtils.writeInt16(out, (short) 2);

        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeInt32(out, (int) 1);
        BinaryStreamUtils.writeInt32(out, (int) 2);

        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeInt64(out, (long) 1);
        BinaryStreamUtils.writeInt64(out, (long) 2);

        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeFloat32(out, 1.5f);
        BinaryStreamUtils.writeFloat32(out, 2.5f);

        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeFloat64(out, 1.5);
        BinaryStreamUtils.writeFloat64(out, 2.5);

        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeBoolean(out, true);
        BinaryStreamUtils.writeBoolean(out, false);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone(TimeZone.getTimeZone("UTC").toZoneId().getId());
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());

        reader.next();

        // Test all array methods with index parameters
        Assert.assertEquals(reader.getByteArray(1), new byte[] {(byte) 1, (byte) 2});
        Assert.assertEquals(reader.getStringArray(2), new String[] {"a", "b"});
        Assert.assertEquals(reader.getShortArray(3), new short[] {(short) 1, (short) 2});
        Assert.assertEquals(reader.getIntArray(4), new int[] {1, 2});
        Assert.assertEquals(reader.getLongArray(5), new long[] {1L, 2L});
        Assert.assertEquals(reader.getFloatArray(6), new float[] {1.5f, 2.5f});
        Assert.assertEquals(reader.getDoubleArray(7), new double[] {1.5, 2.5}, 0.001);
        Assert.assertEquals(reader.getBooleanArray(8), new boolean[] {true, false});
    }

    @Test
    public void testGetObjectArrayByIndex() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String[] names = new String[]{"uint64_arr", "enum_arr", "dt_arr", "str_arr"};
        String[] types = new String[]{
                "Array(UInt64)",
                "Array(Enum8('abc' = 1, 'cde' = 2))",
                "Array(DateTime('UTC'))",
                "Array(String)"
        };

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // Array(UInt64): [100, 200]
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeUnsignedInt64(out, BigInteger.valueOf(100));
        BinaryStreamUtils.writeUnsignedInt64(out, BigInteger.valueOf(200));

        // Array(Enum8('abc' = 1, 'cde' = 2)): [1, 2]
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeEnum8(out, (byte) 1);
        BinaryStreamUtils.writeEnum8(out, (byte) 2);

        // Array(DateTime('UTC')): two timestamps
        LocalDateTime dt1 = LocalDateTime.of(2030, 10, 9, 8, 7, 6);
        LocalDateTime dt2 = LocalDateTime.of(2031, 10, 9, 8, 7, 6);
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeDateTime32(out, dt1, TimeZone.getTimeZone("UTC"));
        BinaryStreamUtils.writeDateTime32(out, dt2, TimeZone.getTimeZone("UTC"));

        // Array(String): ["hello", "world"]
        BinaryStreamUtils.writeVarInt(out, 2);
        BinaryStreamUtils.writeString(out, "hello");
        BinaryStreamUtils.writeString(out, "world");

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone("UTC");
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());
        reader.next();

        // Test getObjectArray with index parameters
        Object[] uint64Result = reader.getObjectArray(1);
        Assert.assertNotNull(uint64Result);
        Assert.assertEquals(uint64Result.length, 2);
        Assert.assertEquals(uint64Result[0], BigInteger.valueOf(100));
        Assert.assertEquals(uint64Result[1], BigInteger.valueOf(200));

        Object[] enumResult = reader.getObjectArray(2);
        Assert.assertNotNull(enumResult);
        Assert.assertEquals(enumResult.length, 2);
        Assert.assertTrue(enumResult[0] instanceof BinaryStreamReader.EnumValue);
        Assert.assertEquals(enumResult[0].toString(), "abc");
        Assert.assertEquals(enumResult[1].toString(), "cde");

        Object[] dtResult = reader.getObjectArray(3);
        Assert.assertNotNull(dtResult);
        Assert.assertEquals(dtResult.length, 2);
        Assert.assertTrue(dtResult[0] instanceof ZonedDateTime);
        ZonedDateTime zdt1 = (ZonedDateTime) dtResult[0];
        ZonedDateTime zdt2 = (ZonedDateTime) dtResult[1];
        Assert.assertEquals(zdt1.toLocalDateTime(), dt1);
        Assert.assertEquals(zdt2.toLocalDateTime(), dt2);

        Object[] strResult = reader.getObjectArray(4);
        Assert.assertNotNull(strResult);
        Assert.assertEquals(strResult.length, 2);
        Assert.assertEquals(strResult[0], "hello");
        Assert.assertEquals(strResult[1], "world");
    }

    @Test
    public void testGetListByIndex() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String[] names = new String[]{"int_list", "string_list", "nested_list"};
        String[] types = new String[]{"Array(Int32)", "Array(String)", "Array(Array(Int64))"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // Array(Int32): [10, 20, 30]
        BinaryStreamUtils.writeVarInt(out, 3);
        BinaryStreamUtils.writeInt32(out, 10);
        BinaryStreamUtils.writeInt32(out, 20);
        BinaryStreamUtils.writeInt32(out, 30);

        // Array(String): ["a", "b", "c"]
        BinaryStreamUtils.writeVarInt(out, 3);
        BinaryStreamUtils.writeString(out, "a");
        BinaryStreamUtils.writeString(out, "b");
        BinaryStreamUtils.writeString(out, "c");

        // Array(Array(Int64)): [[1, 2], [3, 4]]
        BinaryStreamUtils.writeVarInt(out, 2); // outer array length
        BinaryStreamUtils.writeVarInt(out, 2); // inner[0] length
        BinaryStreamUtils.writeInt64(out, 1L);
        BinaryStreamUtils.writeInt64(out, 2L);
        BinaryStreamUtils.writeVarInt(out, 2); // inner[1] length
        BinaryStreamUtils.writeInt64(out, 3L);
        BinaryStreamUtils.writeInt64(out, 4L);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone("UTC");
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());
        reader.next();

        // Test getList with index parameters
        List<Integer> intList = reader.getList(1);
        Assert.assertEquals(intList, Arrays.asList(10, 20, 30));

        List<String> stringList = reader.getList(2);
        Assert.assertEquals(stringList, Arrays.asList("a", "b", "c"));

        List<List<Long>> nestedList = reader.getList(3);
        Assert.assertEquals(nestedList.size(), 2);
        Assert.assertEquals(nestedList.get(0), Arrays.asList(1L, 2L));
        Assert.assertEquals(nestedList.get(1), Arrays.asList(3L, 4L));
    }

    @Test
    public void testArrayMethodsErrorConditions() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String[] names = new String[]{"int_col", "string_col"};
        String[] types = new String[]{"Int32", "String"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        BinaryStreamUtils.writeInt32(out, 42);
        BinaryStreamUtils.writeString(out, "test");

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone("UTC");
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());
        reader.next();

        // Test that calling array methods on non-array columns throws exceptions
        Assert.assertThrows(Exception.class, () -> reader.getByteArray(1));
        Assert.assertThrows(Exception.class, () -> reader.getStringArray(1));
        Assert.assertThrows(Exception.class, () -> reader.getShortArray(1));
        Assert.assertThrows(Exception.class, () -> reader.getIntArray(1));
        Assert.assertThrows(Exception.class, () -> reader.getLongArray(1));
        Assert.assertThrows(Exception.class, () -> reader.getFloatArray(1));
        Assert.assertThrows(Exception.class, () -> reader.getDoubleArray(1));
        Assert.assertThrows(Exception.class, () -> reader.getBooleanArray(1));
        Assert.assertThrows(Exception.class, () -> reader.getObjectArray(1));
        Assert.assertThrows(Exception.class, () -> reader.getList(1));
    }

    @Test
    public void testEmptyArraysByIndex() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String[] names = new String[]{"empty_int_arr", "empty_str_arr", "empty_obj_arr"};
        String[] types = new String[]{"Array(Int32)", "Array(String)", "Array(Array(Int64))"};

        BinaryStreamUtils.writeVarInt(out, names.length);
        for (String name : names) {
            BinaryStreamUtils.writeString(out, name);
        }
        for (String type : types) {
            BinaryStreamUtils.writeString(out, type);
        }

        // Empty arrays
        BinaryStreamUtils.writeVarInt(out, 0); // empty_int_arr
        BinaryStreamUtils.writeVarInt(out, 0); // empty_str_arr
        BinaryStreamUtils.writeVarInt(out, 0); // empty_obj_arr

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        QuerySettings querySettings = new QuerySettings().setUseTimeZone("UTC");
        RowBinaryWithNamesAndTypesFormatReader reader =
                new RowBinaryWithNamesAndTypesFormatReader(in, querySettings, new BinaryStreamReader.CachingByteBufferAllocator());
        reader.next();

        // Test empty arrays
        Assert.assertEquals(reader.getIntArray(1).length, 0);
        Assert.assertEquals(reader.getStringArray(2).length, 0);
        Assert.assertEquals(reader.getObjectArray(3).length, 0);
        Assert.assertEquals(reader.getList(1).size(), 0);
        Assert.assertEquals(reader.getList(2).size(), 0);
        Assert.assertEquals(reader.getList(3).size(), 0);
    }
}