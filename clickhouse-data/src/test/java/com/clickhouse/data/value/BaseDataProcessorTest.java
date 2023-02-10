package com.clickhouse.data.value;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Consumer;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseTestDataConfig;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.format.BinaryStreamUtilsTest;
import com.clickhouse.data.value.ClickHouseByteValue.UnsignedByteValue;
import com.clickhouse.data.value.ClickHouseIntegerValue.UnsignedIntegerValue;
import com.clickhouse.data.value.ClickHouseLongValue.UnsignedLongValue;
import com.clickhouse.data.value.ClickHouseShortValue.UnsignedShortValue;
import com.clickhouse.data.value.array.ClickHouseBoolArrayValue;
import com.clickhouse.data.value.array.ClickHouseByteArrayValue;
import com.clickhouse.data.value.array.ClickHouseDoubleArrayValue;
import com.clickhouse.data.value.array.ClickHouseFloatArrayValue;
import com.clickhouse.data.value.array.ClickHouseIntArrayValue;
import com.clickhouse.data.value.array.ClickHouseLongArrayValue;
import com.clickhouse.data.value.array.ClickHouseShortArrayValue;

public abstract class BaseDataProcessorTest {
    protected void checkNull(ClickHouseValue value) {
        Assert.assertFalse(value.asBoolean());
        Assert.assertEquals(value.asByte(), (byte) 0);
        Assert.assertEquals(value.asShort(), (short) 0);
        Assert.assertEquals(value.asInteger(), 0);
        Assert.assertEquals(value.asLong(), 0L);
        Assert.assertEquals(value.asFloat(), 0F);
        Assert.assertEquals(value.asDouble(), 0D);
        Assert.assertEquals(value.asBigInteger(), null);
        Assert.assertEquals(value.asBigDecimal(), null);
        Assert.assertEquals(value.asBigDecimal(5), null);
    }

    protected void checkZero(ClickHouseValue value) {
        Assert.assertFalse(value.asBoolean());
        Assert.assertEquals(value.asByte(), (byte) 0);
        Assert.assertEquals(value.asShort(), (short) 0);
        Assert.assertEquals(value.asInteger(), 0);
        Assert.assertEquals(value.asLong(), 0L);
        Assert.assertEquals(value.asFloat(), 0F);
        Assert.assertEquals(value.asDouble(), 0D);
        Assert.assertEquals(value.asBigInteger(), BigInteger.ZERO);
        Assert.assertEquals(value.asBigDecimal(), BigDecimal.ZERO);
        Assert.assertEquals(value.asBigDecimal(5), new BigDecimal("0.00000"));
    }

    protected void checkOne(ClickHouseValue value) {
        Assert.assertTrue(value.asBoolean());
        Assert.assertEquals(value.asByte(), (byte) 1);
        Assert.assertEquals(value.asShort(), (short) 1);
        Assert.assertEquals(value.asInteger(), 1);
        Assert.assertEquals(value.asLong(), 1L);
        Assert.assertEquals(value.asFloat(), 1F);
        Assert.assertEquals(value.asDouble(), 1D);
        Assert.assertEquals(value.asBigInteger(), BigInteger.ONE);
        Assert.assertEquals(value.asBigDecimal(), BigDecimal.ONE);
        Assert.assertEquals(value.asBigDecimal(5), new BigDecimal("0.00001"));
    }

    protected void checkNegativeOne(ClickHouseValue value) {
        Assert.assertThrows(IllegalArgumentException.class, () -> value.asBoolean());
        Assert.assertEquals(value.asByte(), (byte) -1);
        Assert.assertEquals(value.asShort(), (short) -1);
        Assert.assertEquals(value.asInteger(), -1);
        Assert.assertEquals(value.asLong(), -1L);
        Assert.assertEquals(value.asFloat(), -1F);
        Assert.assertEquals(value.asDouble(), -1D);
        Assert.assertEquals(value.asBigInteger(), BigInteger.valueOf(-1L));
        if (value instanceof ClickHouseFloatValue || value instanceof ClickHouseDoubleValue) {
            Assert.assertEquals(value.asBigDecimal(), new BigDecimal("-1.0"));
        } else {
            Assert.assertEquals(value.asBigDecimal(), BigDecimal.valueOf(-1L));
        }
        Assert.assertEquals(value.asBigDecimal(5), new BigDecimal("-0.00001"));
    }

    protected void checkMaxUnsignedByte(ClickHouseValue value) {
        Assert.assertThrows(IllegalArgumentException.class, () -> value.asBoolean());
        Assert.assertEquals(value.asByte(), (byte) -1);
        Assert.assertEquals(value.asShort(), (short) 255);
        Assert.assertEquals(value.asInteger(), 255);
        Assert.assertEquals(value.asLong(), 255L);
        Assert.assertEquals(value.asFloat(), 255F);
        Assert.assertEquals(value.asDouble(), 255D);
        Assert.assertEquals(value.asBigInteger(), new BigInteger("255"));
        Assert.assertEquals(value.asBigDecimal(), new BigDecimal("255"));
        Assert.assertEquals(value.asBigDecimal(5), new BigDecimal("0.00255"));
    }

    protected void checkMaxUnsignedShort(ClickHouseValue value) {
        Assert.assertThrows(IllegalArgumentException.class, () -> value.asBoolean());
        Assert.assertEquals(value.asByte(), (byte) -1);
        Assert.assertEquals(value.asShort(), (short) -1);
        Assert.assertEquals(value.asInteger(), 65535);
        Assert.assertEquals(value.asLong(), 65535L);
        Assert.assertEquals(value.asFloat(), 65535F);
        Assert.assertEquals(value.asDouble(), 65535D);
        Assert.assertEquals(value.asBigInteger(), new BigInteger("65535"));
        Assert.assertEquals(value.asBigDecimal(), new BigDecimal("65535"));
        Assert.assertEquals(value.asBigDecimal(5), new BigDecimal("0.65535"));
    }

    protected void checkMaxUnsignedInt(ClickHouseValue value) {
        Assert.assertThrows(IllegalArgumentException.class, () -> value.asBoolean());
        Assert.assertEquals(value.asByte(), (byte) -1);
        Assert.assertEquals(value.asShort(), (short) -1);
        Assert.assertEquals(value.asInteger(), -1);
        Assert.assertEquals(value.asLong(), 4294967295L);
        Assert.assertEquals(value.asFloat(), 4294967295F);
        Assert.assertEquals(value.asDouble(), 4294967295D);
        Assert.assertEquals(value.asBigInteger(), new BigInteger("4294967295"));
        Assert.assertEquals(value.asBigDecimal(), new BigDecimal("4294967295"));
        Assert.assertEquals(value.asBigDecimal(5), new BigDecimal("42949.67295"));
    }

    protected void checkMaxUnsignedLong(ClickHouseValue value) {
        Assert.assertThrows(IllegalArgumentException.class, () -> value.asBoolean());
        Assert.assertEquals(value.asByte(), (byte) -1);
        Assert.assertEquals(value.asShort(), (short) -1);
        Assert.assertEquals(value.asInteger(), -1);
        Assert.assertEquals(value.asLong(), -1L);
        Assert.assertEquals(value.asFloat(), -1F);
        Assert.assertEquals(value.asDouble(), -1D);
        Assert.assertEquals(value.asBigInteger(), new BigInteger("18446744073709551615"));
        Assert.assertEquals(value.asBigDecimal(), new BigDecimal("18446744073709551615"));
        Assert.assertEquals(value.asBigDecimal(5), new BigDecimal("184467440737095.51615"));
    }

    protected void checkBinaryString(ClickHouseValue value) {
        Assert.assertFalse(value.asBoolean());
        Assert.assertTrue(((ClickHouseStringValue) value).isBinary());
        Assert.assertEquals(value.asByte(), (byte) 0);
        Assert.assertEquals(value.asShort(), (short) 0);
        Assert.assertEquals(value.asInteger(), 0);
        Assert.assertEquals(value.asLong(), 0L);
        Assert.assertEquals(value.asFloat(), 0F);
        Assert.assertEquals(value.asDouble(), 0D);
        Assert.assertEquals(value.asBigInteger(), BigInteger.ZERO);
        Assert.assertEquals(value.asBigDecimal(), BigDecimal.ZERO);
        Assert.assertEquals(value.asBigDecimal(5), new BigDecimal("0.00000"));
    }

    protected void checkTextString(ClickHouseValue value) {
        Assert.assertTrue(value.asBoolean());
        Assert.assertFalse(((ClickHouseStringValue) value).isBinary());
        Assert.assertEquals(value.asByte(), (byte) 1);
        Assert.assertEquals(value.asShort(), (short) 1);
        Assert.assertEquals(value.asInteger(), 1);
        Assert.assertEquals(value.asLong(), 1L);
        Assert.assertEquals(value.asFloat(), 1F);
        Assert.assertEquals(value.asDouble(), 1D);
        Assert.assertEquals(value.asBigInteger(), BigInteger.ONE);
        Assert.assertEquals(value.asBigDecimal(), BigDecimal.ONE);
        Assert.assertEquals(value.asBigDecimal(5), new BigDecimal("0.00001"));
    }

    protected byte[] toBytes(byte[]... arrays) {
        int len = 0;
        for (byte[] arr : arrays) {
            len += arr.length;
        }

        byte[] bytes = new byte[len];
        len = 0;
        for (byte[] arr : arrays) {
            System.arraycopy(arr, 0, bytes, len, arr.length);
            len += arr.length;
        }
        return bytes;
    }

    protected byte[] toBytes(byte[] header, int... ints) {
        byte[] bytes = new byte[header.length + ints.length];
        System.arraycopy(header, 0, bytes, 0, header.length);
        for (int i = 0; i < ints.length; i++) {
            bytes[header.length + i] = (byte) ints[i];
        }
        return bytes;
    }

    protected byte[] toBytes(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return bytes;
    }

    protected byte[] toBytes(long... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return bytes;
    }

    protected byte[] toBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    protected byte[] toLEBytes(short s) {
        return toBytes(0xFF & s, 0xFF & (s >> 8));
    }

    protected byte[] toLEBytes(int i) {
        return toBytes(0xFF & i, 0xFF & (i >> 8), 0xFF & (i >> 16), 0xFF & (i >> 24));
    }

    protected byte[] toLEBytes(long l) {
        return toBytes(0xFF & l, 0xFF & (l >> 8), 0xFF & (l >> 16), 0xFF & (l >> 24), 0xFF & (l >> 32),
                0xFF & (l >> 40), 0xFF & (l >> 48), 0xFF & (l >> 56));
    }

    protected byte[] toLEBytes(float f) {
        return toLEBytes(Float.floatToIntBits(f));
    }

    protected byte[] toLEBytes(double d) {
        return toLEBytes(Double.doubleToLongBits(d));
    }

    protected byte[] toLEBytes(String s) {
        return toBytes(new byte[] { (byte) s.length() }, toBytes(s));
    }

    protected abstract ClickHouseDataProcessor getDataProcessor(ClickHouseDataConfig config, ClickHouseColumn column,
            ClickHouseInputStream input, ClickHouseOutputStream output) throws IOException;

    protected abstract byte[] getRawData(String typeName, String key);

    protected ClickHouseInputStream getInputData(String typeName, String key) {
        return BinaryStreamUtilsTest.generateInput(getRawData(typeName, key));
    }

    @DataProvider(name = "nestedTypesForRead")
    protected Object[][] getNestedTypesForRead() {
        ClickHouseDataConfig defaultConf = new ClickHouseTestDataConfig();
        ClickHouseDataConfig binStrConf = new ClickHouseTestDataConfig() {
            @Override
            public boolean isUseBinaryString() {
                return true;
            }
        };
        ClickHouseDataConfig widenUtConf = new ClickHouseTestDataConfig() {
            @Override
            public boolean isWidenUnsignedTypes() {
                return true;
            }
        };
        ClickHouseDataConfig useObjsConf = new ClickHouseTestDataConfig() {
            @Override
            public boolean isUseObjectsInArray() {
                return true;
            }
        };
        ClickHouseDataConfig combinedConf = new ClickHouseTestDataConfig() {
            @Override
            public boolean isUseObjectsInArray() {
                return true;
            }

            @Override
            public boolean isWidenUnsignedTypes() {
                return true;
            }
        };

        return new Object[][] {
                { defaultConf, "Array(Nullable(Bool))", "0,1", "ClickHouseArrayValue",
                        new Boolean[] { null, false, true }, new Boolean[] { null, false, true }, "[null, false, true]",
                        "[NULL,false,true]" },
                { defaultConf, "Array(Bool)", "0,1", "ClickHouseBoolArrayValue", new Boolean[] { false, true },
                        new boolean[] { false, true }, "[false,true]", "[false,true]" },
                { defaultConf, "Array(Nullable(Int8))", "0,1,-1", "ClickHouseArrayValue",
                        new Byte[] { null, 0, 1, -1 }, new Byte[] { null, 0, 1, -1 }, "[null, 0, 1, -1]",
                        "[NULL,0,1,-1]" },
                { defaultConf, "Array(Int8)", "0,1,-1", "ClickHouseByteArrayValue", new Byte[] { 0, 1, -1 },
                        new byte[] { 0, 1, -1 }, "[0,1,-1]", "[0,1,-1]" },
                { defaultConf, "Array(Nullable(UInt8))", "0,1,-1", "ClickHouseArrayValue",
                        new UnsignedByte[] { null, UnsignedByte.ZERO, UnsignedByte.ONE, UnsignedByte.MAX_VALUE },
                        new UnsignedByte[] { null, UnsignedByte.ZERO, UnsignedByte.ONE, UnsignedByte.MAX_VALUE },
                        "[null, 0, 1, 255]", "[NULL,0,1,255]" },
                { defaultConf, "Array(UInt8)", "0,1,-1", "UnsignedByteArrayValue",
                        new UnsignedByte[] { UnsignedByte.ZERO, UnsignedByte.ONE, UnsignedByte.MAX_VALUE },
                        new byte[] { 0, 1, -1 }, "[0,1,255]", "[0,1,255]" },
                { defaultConf, "Array(Nullable(Int16))", "0,1,-1", "ClickHouseArrayValue",
                        new Short[] { null, 0, 1, -1 }, new Short[] { null, 0, 1, -1 }, "[null, 0, 1, -1]",
                        "[NULL,0,1,-1]" },
                { defaultConf, "Array(Int16)", "0,1,-1", "ClickHouseShortArrayValue", new Short[] { 0, 1, -1 },
                        new short[] { 0, 1, -1 }, "[0,1,-1]", "[0,1,-1]" },
                { defaultConf, "Array(Nullable(UInt16))", "0,1,-1", "ClickHouseArrayValue",
                        new UnsignedShort[] { null, UnsignedShort.ZERO, UnsignedShort.ONE, UnsignedShort.MAX_VALUE },
                        new UnsignedShort[] { null, UnsignedShort.ZERO, UnsignedShort.ONE, UnsignedShort.MAX_VALUE },
                        "[null, 0, 1, 65535]", "[NULL,0,1,65535]" },
                { defaultConf, "Array(UInt16)", "0,1,-1", "UnsignedShortArrayValue",
                        new UnsignedShort[] { UnsignedShort.ZERO, UnsignedShort.ONE, UnsignedShort.MAX_VALUE },
                        new short[] { 0, 1, -1 }, "[0,1,65535]", "[0,1,65535]" },
                { defaultConf, "Array(Nullable(Int32))", "0,1,-1", "ClickHouseArrayValue",
                        new Integer[] { null, 0, 1, -1 }, new Integer[] { null, 0, 1, -1 }, "[null, 0, 1, -1]",
                        "[NULL,0,1,-1]" },
                { defaultConf, "Array(Int32)", "0,1,-1", "ClickHouseIntArrayValue", new Integer[] { 0, 1, -1 },
                        new int[] { 0, 1, -1 }, "[0,1,-1]", "[0,1,-1]" },
                { defaultConf, "Array(Nullable(UInt32))", "0,1,-1", "ClickHouseArrayValue",
                        new UnsignedInteger[] { null, UnsignedInteger.ZERO, UnsignedInteger.ONE,
                                UnsignedInteger.MAX_VALUE },
                        new UnsignedInteger[] { null, UnsignedInteger.ZERO, UnsignedInteger.ONE,
                                UnsignedInteger.MAX_VALUE },
                        "[null, 0, 1, 4294967295]", "[NULL,0,1,4294967295]" },
                { defaultConf, "Array(UInt32)", "0,1,-1", "UnsignedIntArrayValue",
                        new UnsignedInteger[] { UnsignedInteger.ZERO, UnsignedInteger.ONE, UnsignedInteger.MAX_VALUE },
                        new int[] { 0, 1, -1 }, "[0,1,4294967295]", "[0,1,4294967295]" },
                { defaultConf, "Array(Nullable(Int64))", "0,1,-1", "ClickHouseArrayValue",
                        new Long[] { null, 0L, 1L, -1L }, new Long[] { null, 0L, 1L, -1L }, "[null, 0, 1, -1]",
                        "[NULL,0,1,-1]" },
                { defaultConf, "Array(Int64)", "0,1,-1", "ClickHouseLongArrayValue", new Long[] { 0L, 1L, -1L },
                        new long[] { 0L, 1L, -1L }, "[0,1,-1]", "[0,1,-1]" },
                { defaultConf, "Array(Nullable(UInt64))", "0,1,-1", "ClickHouseArrayValue",
                        new UnsignedLong[] { null, UnsignedLong.ZERO, UnsignedLong.ONE, UnsignedLong.MAX_VALUE },
                        new UnsignedLong[] { null, UnsignedLong.ZERO, UnsignedLong.ONE, UnsignedLong.MAX_VALUE },
                        "[null, 0, 1, 18446744073709551615]", "[NULL,0,1,18446744073709551615]" },
                { defaultConf, "Array(UInt64)", "0,1,-1", "UnsignedLongArrayValue",
                        new UnsignedLong[] { UnsignedLong.ZERO, UnsignedLong.ONE, UnsignedLong.MAX_VALUE },
                        new long[] { 0L, 1L, -1L }, "[0,1,18446744073709551615]", "[0,1,18446744073709551615]" },
                { defaultConf, "Array(Nullable(Float32))", "0,1,-1", "ClickHouseArrayValue",
                        new Float[] { null, 0F, 1F, -1F }, new Float[] { null, 0F, 1F, -1F }, "[null, 0.0, 1.0, -1.0]",
                        "[NULL,0.0,1.0,-1.0]" },
                { defaultConf, "Array(Float32)", "0,1,-1", "ClickHouseFloatArrayValue", new Float[] { 0F, 1F, -1F },
                        new float[] { 0F, 1F, -1F }, "[0.0,1.0,-1.0]", "[0.0,1.0,-1.0]" },
                { defaultConf, "Array(Nullable(Float64))", "0,1,-1", "ClickHouseArrayValue",
                        new Double[] { null, 0D, 1D, -1D }, new Double[] { null, 0D, 1D, -1D },
                        "[null, 0.0, 1.0, -1.0]", "[NULL,0.0,1.0,-1.0]" },
                { defaultConf, "Array(Float64)", "0,1,-1", "ClickHouseDoubleArrayValue", new Double[] { 0D, 1D, -1D },
                        new double[] { 0D, 1D, -1D }, "[0.0,1.0,-1.0]", "[0.0,1.0,-1.0]" },
                { defaultConf, "Array(Nullable(String))", "4bcd", "ClickHouseArrayValue",
                        new String[] { null, "", "B", "BC", "BCD" }, new String[] { null, "", "B", "BC", "BCD" },
                        "[null, , B, BC, BCD]", "[NULL,'','B','BC','BCD']" },
                { defaultConf, "Array(String)", "4bcd", "ClickHouseArrayValue", new String[] { "", "B", "BC", "BCD" },
                        new String[] { "", "B", "BC", "BCD" }, "[, B, BC, BCD]", "['','B','BC','BCD']" },
                { defaultConf, "Array(Array(Array(UInt8)))", "[[3]],[[1,2],[2,1]],[[4,5],[5,4]]",
                        "ClickHouseArrayValue",
                        new Byte[][][] { { { 3 } }, { { 1, 2 }, { 2, 1 } }, { { 4, 5 }, { 5, 4 } } },
                        new byte[][][] { { { 3 } }, { { 1, 2 }, { 2, 1 } }, { { 4, 5 }, { 5, 4 } } },
                        "[[[3]], [[1, 2], [2, 1]], [[4, 5], [5, 4]]]", "[[[3]],[[1,2],[2,1]],[[4,5],[5,4]]]" },
                { defaultConf, "Array(Array(UInt64))", "[1,2,3],[3,2,1],[4,5]", "ClickHouseArrayValue",
                        new Long[][] { { 1L, 2L, 3L }, { 3L, 2L, 1L }, { 4L, 5L } },
                        new long[][] { { 1L, 2L, 3L }, { 3L, 2L, 1L }, { 4L, 5L } }, "[[1, 2, 3], [3, 2, 1], [4, 5]]",
                        "[[1,2,3],[3,2,1],[4,5]]" },
                { defaultConf, "Map(String,Array(UInt8))", "[1,2,3],[3,2,1],[4,5]", "ClickHouseMapValue",
                        new Long[][] { { 1L, 2L, 3L }, { 3L, 2L, 1L }, { 4L, 5L } },
                        new long[][] { { 1L, 2L, 3L }, { 3L, 2L, 1L }, { 4L, 5L } }, "[[1, 2, 3], [3, 2, 1], [4, 5]]",
                        "{'a' : [1,2,3],'b' : [3,2,1],'c' : [4,5]}" },
                { defaultConf, "Tuple(Array(UInt8),Array(UInt8),Array(UInt8))", "[1,2,3],[3,2,1],[4,5]",
                        "ClickHouseTupleValue",
                        new Object[] { new byte[] { 1, 2, 3 }, new byte[] { 3, 2, 1 }, new byte[] { 4, 5 } },
                        Arrays.asList(
                                new Object[] { new byte[] { 1, 2, 3 }, new byte[] { 3, 2, 1 }, new byte[] { 4, 5 } }),
                        "[[1, 2, 3], [3, 2, 1], [4, 5]]", "([1,2,3],[3,2,1],[4,5])" },
                { defaultConf, "Array(Array(String))", "[foo,bar],[qaz,qux]", "ClickHouseArrayValue",
                        new String[][] { { "foo", "bar" }, { "qaz", "qux" } },
                        new String[][] { { "foo", "bar" }, { "qaz", "qux" } }, "[[foo, bar], [qaz, qux]]",
                        "[['foo','bar'],['qaz','qux']]" },

                { binStrConf, "Array(Nullable(String))", "4bcd", "ClickHouseArrayValue",
                        new byte[][] { null, new byte[0], { 66 }, { 66, 67 }, { 66, 67, 68 } },
                        new byte[][] { null, new byte[0], { 66 }, { 66, 67 }, { 66, 67, 68 } },
                        "[null, [], [66], [66, 67], [66, 67, 68]]", "[NULL,[],[66],[66,67],[66,67,68]]" },
                { binStrConf, "Array(String)", "4bcd", "ClickHouseArrayValue",
                        new byte[][] { new byte[0], { 66 }, { 66, 67 }, { 66, 67, 68 } },
                        new byte[][] { new byte[0], { 66 }, { 66, 67 }, { 66, 67, 68 } },
                        "[[], [66], [66, 67], [66, 67, 68]]", "[[],[66],[66,67],[66,67,68]]" },
                { binStrConf, "Array(Array(String))", "[foo,bar],[qaz,qux]", "ClickHouseArrayValue",
                        new byte[][][] { { { 0x66, 0x6F, 0x6F }, { 0x62, 0x61, 0x72 } },
                                { { 0x71, 0x61, 0x7A }, { 0x71, 0x75, 0x78 } } },
                        new byte[][][] { { { 0x66, 0x6F, 0x6F }, { 0x62, 0x61, 0x72 } },
                                { { 0x71, 0x61, 0x7A }, { 0x71, 0x75, 0x78 } } },
                        "[[[102, 111, 111], [98, 97, 114]], [[113, 97, 122], [113, 117, 120]]]",
                        "[[[102,111,111],[98,97,114]],[[113,97,122],[113,117,120]]]" },

                { widenUtConf, "Array(Nullable(Bool))", "0,1", "ClickHouseArrayValue",
                        new Boolean[] { null, false, true }, new Boolean[] { null, false, true }, "[null, false, true]",
                        "[NULL,false,true]" },
                { widenUtConf, "Array(Bool)", "0,1", "ClickHouseBoolArrayValue", new Boolean[] { false, true },
                        new boolean[] { false, true }, "[false,true]", "[false,true]" },
                { widenUtConf, "Array(Nullable(Int8))", "0,1,-1", "ClickHouseArrayValue",
                        new Byte[] { null, 0, 1, -1 }, new Byte[] { null, 0, 1, -1 }, "[null, 0, 1, -1]",
                        "[NULL,0,1,-1]" },
                { widenUtConf, "Array(Int8)", "0,1,-1", "ClickHouseByteArrayValue", new Byte[] { 0, 1, -1 },
                        new byte[] { 0, 1, -1 }, "[0,1,-1]", "[0,1,-1]" },
                { widenUtConf, "Array(Nullable(UInt8))", "0,1,-1", "ClickHouseArrayValue",
                        new Short[] { null, 0, 1, 255 }, new Short[] { null, 0, 1, 255 }, "[null, 0, 1, 255]",
                        "[NULL,0,1,255]" },
                { widenUtConf, "Array(UInt8)", "0,1,-1", "ClickHouseShortArrayValue", new Short[] { 0, 1, 255 },
                        new short[] { 0, 1, 255 }, "[0,1,255]", "[0,1,255]" },
                { widenUtConf, "Array(Nullable(Int16))", "0,1,-1", "ClickHouseArrayValue",
                        new Short[] { null, 0, 1, -1 }, new Short[] { null, 0, 1, -1 }, "[null, 0, 1, -1]",
                        "[NULL,0,1,-1]" },
                { widenUtConf, "Array(Int16)", "0,1,-1", "ClickHouseShortArrayValue", new Short[] { 0, 1, -1 },
                        new short[] { 0, 1, -1 }, "[0,1,-1]", "[0,1,-1]" },
                { widenUtConf, "Array(Nullable(UInt16))", "0,1,-1", "ClickHouseArrayValue",
                        new Integer[] { null, 0, 1, 65535 }, new Integer[] { null, 0, 1, 65535 }, "[null, 0, 1, 65535]",
                        "[NULL,0,1,65535]" },
                { widenUtConf, "Array(UInt16)", "0,1,-1", "ClickHouseIntArrayValue", new Integer[] { 0, 1, 65535 },
                        new int[] { 0, 1, 65535 }, "[0,1,65535]", "[0,1,65535]" },
                { widenUtConf, "Array(Int32)", "0,1,-1", "ClickHouseIntArrayValue", new Integer[] { 0, 1, -1 },
                        new int[] { 0, 1, -1 }, "[0,1,-1]", "[0,1,-1]" },
                { widenUtConf, "Array(Nullable(UInt32))", "0,1,-1", "ClickHouseArrayValue",
                        new Long[] { null, 0L, 1L, 4294967295L }, new Long[] { null, 0L, 1L, 4294967295L },
                        "[null, 0, 1, 4294967295]", "[NULL,0,1,4294967295]" },
                { widenUtConf, "Array(UInt32)", "0,1,-1", "ClickHouseLongArrayValue",
                        new Long[] { 0L, 1L, 4294967295L }, new long[] { 0L, 1L, 4294967295L }, "[0,1,4294967295]",
                        "[0,1,4294967295]" },
                { widenUtConf, "Array(Nullable(Int64))", "0,1,-1", "ClickHouseArrayValue",
                        new Long[] { null, 0L, 1L, -1L }, new Long[] { null, 0L, 1L, -1L }, "[null, 0, 1, -1]",
                        "[NULL,0,1,-1]" },
                { widenUtConf, "Array(Int64)", "0,1,-1", "ClickHouseLongArrayValue", new Long[] { 0L, 1L, -1L },
                        new long[] { 0L, 1L, -1L }, "[0,1,-1]", "[0,1,-1]" },
                { widenUtConf, "Array(Nullable(UInt64))", "0,1,-1", "ClickHouseArrayValue",
                        new UnsignedLong[] { null, UnsignedLong.ZERO, UnsignedLong.ONE, UnsignedLong.MAX_VALUE },
                        new UnsignedLong[] { null, UnsignedLong.ZERO, UnsignedLong.ONE, UnsignedLong.MAX_VALUE },
                        "[null, 0, 1, 18446744073709551615]", "[NULL,0,1,18446744073709551615]" },
                { widenUtConf, "Array(UInt64)", "0,1,-1", "UnsignedLongArrayValue",
                        new UnsignedLong[] { UnsignedLong.ZERO, UnsignedLong.ONE, UnsignedLong.MAX_VALUE },
                        new long[] { 0L, 1L, -1L }, "[0,1,18446744073709551615]", "[0,1,18446744073709551615]" },
                { widenUtConf, "Array(Nullable(Float32))", "0,1,-1", "ClickHouseArrayValue",
                        new Float[] { null, 0F, 1F, -1F }, new Float[] { null, 0F, 1F, -1F }, "[null, 0.0, 1.0, -1.0]",
                        "[NULL,0.0,1.0,-1.0]" },
                { widenUtConf, "Array(Float32)", "0,1,-1", "ClickHouseFloatArrayValue", new Float[] { 0F, 1F, -1F },
                        new float[] { 0F, 1F, -1F }, "[0.0,1.0,-1.0]", "[0.0,1.0,-1.0]" },
                { widenUtConf, "Array(Nullable(Float64))", "0,1,-1", "ClickHouseArrayValue",
                        new Double[] { null, 0D, 1D, -1D }, new Double[] { null, 0D, 1D, -1D },
                        "[null, 0.0, 1.0, -1.0]", "[NULL,0.0,1.0,-1.0]" },
                { widenUtConf, "Array(Float64)", "0,1,-1", "ClickHouseDoubleArrayValue", new Double[] { 0D, 1D, -1D },
                        new double[] { 0D, 1D, -1D }, "[0.0,1.0,-1.0]", "[0.0,1.0,-1.0]" },
                { widenUtConf, "Array(Nullable(String))", "4bcd", "ClickHouseArrayValue",
                        new String[] { null, "", "B", "BC", "BCD" }, new String[] { null, "", "B", "BC", "BCD" },
                        "[null, , B, BC, BCD]", "[NULL,'','B','BC','BCD']" },
                { widenUtConf, "Array(String)", "4bcd", "ClickHouseArrayValue", new String[] { "", "B", "BC", "BCD" },
                        new String[] { "", "B", "BC", "BCD" }, "[, B, BC, BCD]", "['','B','BC','BCD']" },
                { widenUtConf, "Array(Array(Array(UInt8)))", "[[3]],[[1,2],[2,1]],[[4,5],[5,4]]",
                        "ClickHouseArrayValue",
                        new Short[][][] { { { 3 } }, { { 1, 2 }, { 2, 1 } }, { { 4, 5 }, { 5, 4 } } },
                        new short[][][] { { { 3 } }, { { 1, 2 }, { 2, 1 } }, { { 4, 5 }, { 5, 4 } } },
                        "[[[3]], [[1, 2], [2, 1]], [[4, 5], [5, 4]]]", "[[[3]],[[1,2],[2,1]],[[4,5],[5,4]]]" },
                // better to use UnsignedLong and fix the ArrayStoreException
                { widenUtConf, "Array(Array(UInt64))", "[1,2,3],[3,2,1],[4,5]", "ClickHouseArrayValue",
                        new Long[][] { { 1L, 2L, 3L }, { 3L, 2L, 1L }, { 4L, 5L } },
                        new long[][] { { 1L, 2L, 3L }, { 3L, 2L, 1L }, { 4L, 5L } },
                        "[[1, 2, 3], [3, 2, 1], [4, 5]]", "[[1,2,3],[3,2,1],[4,5]]" },

                { useObjsConf, "Array(Bool)", "0,1", "ClickHouseArrayValue", new Boolean[] { false, true },
                        new Boolean[] { false, true }, "[false, true]", "[false,true]" },
                { useObjsConf, "Array(Int8)", "0,1,-1", "ClickHouseArrayValue", new Byte[] { 0, 1, -1 },
                        new Byte[] { 0, 1, -1 }, "[0, 1, -1]", "[0,1,-1]" },
                { useObjsConf, "Array(UInt8)", "0,1,-1", "ClickHouseArrayValue",
                        new UnsignedByte[] { UnsignedByte.ZERO, UnsignedByte.ONE, UnsignedByte.MAX_VALUE },
                        new UnsignedByte[] { UnsignedByte.ZERO, UnsignedByte.ONE, UnsignedByte.MAX_VALUE },
                        "[0, 1, 255]", "[0,1,255]" },
                { useObjsConf, "Array(Int16)", "0,1,-1", "ClickHouseArrayValue", new Short[] { 0, 1, -1 },
                        new Short[] { 0, 1, -1 }, "[0, 1, -1]", "[0,1,-1]" },
                { useObjsConf, "Array(UInt16)", "0,1,-1", "ClickHouseArrayValue",
                        new UnsignedShort[] { UnsignedShort.ZERO, UnsignedShort.ONE, UnsignedShort.MAX_VALUE },
                        new UnsignedShort[] { UnsignedShort.ZERO, UnsignedShort.ONE, UnsignedShort.MAX_VALUE },
                        "[0, 1, 65535]", "[0,1,65535]" },
                { useObjsConf, "Array(Int32)", "0,1,-1", "ClickHouseArrayValue", new Integer[] { 0, 1, -1 },
                        new Integer[] { 0, 1, -1 }, "[0, 1, -1]", "[0,1,-1]" },
                { useObjsConf, "Array(UInt32)", "0,1,-1", "ClickHouseArrayValue",
                        new UnsignedInteger[] { UnsignedInteger.ZERO, UnsignedInteger.ONE, UnsignedInteger.MAX_VALUE },
                        new UnsignedInteger[] { UnsignedInteger.ZERO, UnsignedInteger.ONE, UnsignedInteger.MAX_VALUE },
                        "[0, 1, 4294967295]", "[0,1,4294967295]" },
                { useObjsConf, "Array(Int64)", "0,1,-1", "ClickHouseArrayValue", new Long[] { 0L, 1L, -1L },
                        new Long[] { 0L, 1L, -1L }, "[0, 1, -1]", "[0,1,-1]" },
                { useObjsConf, "Array(UInt64)", "0,1,-1", "ClickHouseArrayValue",
                        new UnsignedLong[] { UnsignedLong.ZERO, UnsignedLong.ONE, UnsignedLong.MAX_VALUE },
                        new UnsignedLong[] { UnsignedLong.ZERO, UnsignedLong.ONE, UnsignedLong.MAX_VALUE },
                        "[0, 1, 18446744073709551615]", "[0,1,18446744073709551615]" },
                { useObjsConf, "Array(Float32)", "0,1,-1", "ClickHouseArrayValue", new Float[] { 0F, 1F, -1F },
                        new Float[] { 0F, 1F, -1F }, "[0.0, 1.0, -1.0]", "[0.0,1.0,-1.0]" },
                { useObjsConf, "Array(Float64)", "0,1,-1", "ClickHouseArrayValue", new Double[] { 0D, 1D, -1D },
                        new Double[] { 0D, 1D, -1D }, "[0.0, 1.0, -1.0]", "[0.0,1.0,-1.0]" },
                { useObjsConf, "Array(String)", "4bcd", "ClickHouseArrayValue", new String[] { "", "B", "BC", "BCD" },
                        new String[] { "", "B", "BC", "BCD" }, "[, B, BC, BCD]", "['','B','BC','BCD']" },
                { useObjsConf, "Array(Array(Array(UInt8)))", "[[3]],[[1,2],[2,1]],[[4,5],[5,4]]",
                        "ClickHouseArrayValue",
                        new UnsignedByte[][][] { { { UnsignedByte.valueOf((byte) 3) } },
                                { { UnsignedByte.ONE, UnsignedByte.valueOf((byte) 2) },
                                        { UnsignedByte.valueOf((byte) 2), UnsignedByte.ONE } },
                                { { UnsignedByte.valueOf((byte) 4), UnsignedByte.valueOf((byte) 5) },
                                        { UnsignedByte.valueOf((byte) 5), UnsignedByte.valueOf((byte) 4) } } },
                        new UnsignedByte[][][] { { { UnsignedByte.valueOf((byte) 3) } },
                                { { UnsignedByte.ONE, UnsignedByte.valueOf((byte) 2) },
                                        { UnsignedByte.valueOf((byte) 2), UnsignedByte.ONE } },
                                { { UnsignedByte.valueOf((byte) 4), UnsignedByte.valueOf((byte) 5) },
                                        { UnsignedByte.valueOf((byte) 5), UnsignedByte.valueOf((byte) 4) } } },
                        "[[[3]], [[1, 2], [2, 1]], [[4, 5], [5, 4]]]", "[[[3]],[[1,2],[2,1]],[[4,5],[5,4]]]" },
                { useObjsConf, "Array(Array(UInt64))", "[1,2,3],[3,2,1],[4,5]", "ClickHouseArrayValue",
                        new UnsignedLong[][] { { UnsignedLong.ONE, UnsignedLong.TWO, UnsignedLong.valueOf(3L) },
                                { UnsignedLong.valueOf(3L), UnsignedLong.TWO, UnsignedLong.ONE },
                                { UnsignedLong.valueOf(4L), UnsignedLong.valueOf(5L) } },
                        new UnsignedLong[][] { { UnsignedLong.ONE, UnsignedLong.TWO, UnsignedLong.valueOf(3L) },
                                { UnsignedLong.valueOf(3L), UnsignedLong.TWO, UnsignedLong.ONE },
                                { UnsignedLong.valueOf(4L), UnsignedLong.valueOf(5L) } },
                        "[[1, 2, 3], [3, 2, 1], [4, 5]]", "[[1,2,3],[3,2,1],[4,5]]" },
                { useObjsConf, "Array(Array(String))", "[foo,bar],[qaz,qux]", "ClickHouseArrayValue",
                        new String[][] { { "foo", "bar" }, { "qaz", "qux" } },
                        new String[][] { { "foo", "bar" }, { "qaz", "qux" } }, "[[foo, bar], [qaz, qux]]",
                        "[['foo','bar'],['qaz','qux']]" },

                { combinedConf, "Array(Bool)", "0,1", "ClickHouseArrayValue", new Boolean[] { false, true },
                        new Boolean[] { false, true }, "[false, true]", "[false,true]" },
                { combinedConf, "Array(Int8)", "0,1,-1", "ClickHouseArrayValue", new Byte[] { 0, 1, -1 },
                        new Byte[] { 0, 1, -1 }, "[0, 1, -1]", "[0,1,-1]" },
                { combinedConf, "Array(UInt8)", "0,1,-1", "ClickHouseArrayValue", new Short[] { 0, 1, 255 },
                        new Short[] { 0, 1, 255 }, "[0, 1, 255]", "[0,1,255]" },
                { combinedConf, "Array(Int16)", "0,1,-1", "ClickHouseArrayValue", new Short[] { 0, 1, -1 },
                        new Short[] { 0, 1, -1 }, "[0, 1, -1]", "[0,1,-1]" },
                { combinedConf, "Array(UInt16)", "0,1,-1", "ClickHouseArrayValue", new Integer[] { 0, 1, 65535 },
                        new Integer[] { 0, 1, 65535 }, "[0, 1, 65535]", "[0,1,65535]" },
                { combinedConf, "Array(Int32)", "0,1,-1", "ClickHouseArrayValue", new Integer[] { 0, 1, -1 },
                        new Integer[] { 0, 1, -1 }, "[0, 1, -1]", "[0,1,-1]" },
                { combinedConf, "Array(UInt32)", "0,1,-1", "ClickHouseArrayValue", new Long[] { 0L, 1L, 4294967295L },
                        new Long[] { 0L, 1L, 4294967295L }, "[0, 1, 4294967295]", "[0,1,4294967295]" },
                { combinedConf, "Array(Int64)", "0,1,-1", "ClickHouseArrayValue", new Long[] { 0L, 1L, -1L },
                        new Long[] { 0L, 1L, -1L }, "[0, 1, -1]", "[0,1,-1]" },
                { combinedConf, "Array(UInt64)", "0,1,-1", "ClickHouseArrayValue",
                        new UnsignedLong[] { UnsignedLong.ZERO, UnsignedLong.ONE, UnsignedLong.MAX_VALUE },
                        new UnsignedLong[] { UnsignedLong.ZERO, UnsignedLong.ONE, UnsignedLong.MAX_VALUE },
                        "[0, 1, 18446744073709551615]", "[0,1,18446744073709551615]" },
                { combinedConf, "Array(Float32)", "0,1,-1", "ClickHouseArrayValue", new Float[] { 0F, 1F, -1F },
                        new Float[] { 0F, 1F, -1F }, "[0.0, 1.0, -1.0]", "[0.0,1.0,-1.0]" },
                { combinedConf, "Array(Float64)", "0,1,-1", "ClickHouseArrayValue", new Double[] { 0D, 1D, -1D },
                        new Double[] { 0D, 1D, -1D }, "[0.0, 1.0, -1.0]", "[0.0,1.0,-1.0]" },
                { combinedConf, "Array(String)", "4bcd", "ClickHouseArrayValue", new String[] { "", "B", "BC", "BCD" },
                        new String[] { "", "B", "BC", "BCD" }, "[, B, BC, BCD]", "['','B','BC','BCD']" },
                { combinedConf, "Array(Array(Array(UInt8)))", "[[3]],[[1,2],[2,1]],[[4,5],[5,4]]",
                        "ClickHouseArrayValue",
                        new Short[][][] { { { 3 } }, { { 1, 2 }, { 2, 1 } }, { { 4, 5 }, { 5, 4 } } },
                        new short[][][] { { { 3 } }, { { 1, 2 }, { 2, 1 } }, { { 4, 5 }, { 5, 4 } } },
                        "[[[3]], [[1, 2], [2, 1]], [[4, 5], [5, 4]]]", "[[[3]],[[1,2],[2,1]],[[4,5],[5,4]]]" },
                { combinedConf, "Array(Array(UInt64))", "[1,2,3],[3,2,1],[4,5]", "ClickHouseArrayValue",
                        new UnsignedLong[][] { { UnsignedLong.ONE, UnsignedLong.TWO, UnsignedLong.valueOf(3L) },
                                { UnsignedLong.valueOf(3L), UnsignedLong.TWO, UnsignedLong.ONE },
                                { UnsignedLong.valueOf(4L), UnsignedLong.valueOf(5L) } },
                        new UnsignedLong[][] { { UnsignedLong.ONE, UnsignedLong.TWO, UnsignedLong.valueOf(3L) },
                                { UnsignedLong.valueOf(3L), UnsignedLong.TWO, UnsignedLong.ONE },
                                { UnsignedLong.valueOf(4L), UnsignedLong.valueOf(5L) } },
                        "[[1, 2, 3], [3, 2, 1], [4, 5]]", "[[1,2,3],[3,2,1],[4,5]]" },
                { combinedConf, "Array(Array(String))", "[foo,bar],[qaz,qux]", "ClickHouseArrayValue",
                        new String[][] { { "foo", "bar" }, { "qaz", "qux" } },
                        new String[][] { { "foo", "bar" }, { "qaz", "qux" } }, "[[foo, bar], [qaz, qux]]",
                        "[['foo','bar'],['qaz','qux']]" },
        };
    }

    @DataProvider(name = "nestedTypesForWrite")
    protected Object[][] getNestedTypesForWrite() {
        ClickHouseDataConfig defaultConf = new ClickHouseTestDataConfig();
        ClickHouseDataConfig binStrConf = new ClickHouseTestDataConfig() {
            @Override
            public boolean isUseBinaryString() {
                return true;
            }
        };
        ClickHouseDataConfig widenUtConf = new ClickHouseTestDataConfig() {
            @Override
            public boolean isWidenUnsignedTypes() {
                return true;
            }
        };
        ClickHouseDataConfig useObjsConf = new ClickHouseTestDataConfig() {
            @Override
            public boolean isUseObjectsInArray() {
                return true;
            }
        };
        ClickHouseDataConfig combinedConf = new ClickHouseTestDataConfig() {
            @Override
            public boolean isUseObjectsInArray() {
                return true;
            }

            @Override
            public boolean isWidenUnsignedTypes() {
                return true;
            }
        };

        return new Object[][] {
                { defaultConf, "Array(Nullable(Bool))", "0,1",
                        new ClickHouseValue[] { ClickHouseArrayValue.of(new Boolean[] { null, false, true }) } },
                { defaultConf, "Array(Bool)", "0,1",
                        new ClickHouseValue[] { ClickHouseBoolArrayValue.of(new boolean[] { false, true }),
                                ClickHouseArrayValue.of(new Boolean[] { false, true }) } },
                { defaultConf, "Array(Nullable(Int8))", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseArrayValue.of(new Byte[] { null, 0, 1, -1 }) } },
                { defaultConf, "Array(Int8)", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseByteArrayValue.of(new byte[] { 0, 1, -1 }),
                                ClickHouseArrayValue.of(new Byte[] { 0, 1, -1 }) } },
                { defaultConf, "Array(Nullable(UInt8))", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseArrayValue.of(new UnsignedByte[] { null, UnsignedByte.ZERO,
                                UnsignedByte.ONE, UnsignedByte.MAX_VALUE }) } },
                { defaultConf, "Array(UInt8)", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseByteArrayValue.ofUnsigned(new byte[] { 0, 1, -1 }),
                                ClickHouseArrayValue.of(new UnsignedByte[] { UnsignedByte.ZERO, UnsignedByte.ONE,
                                        UnsignedByte.MAX_VALUE }) } },
                { defaultConf, "Array(Nullable(Int16))", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseArrayValue.of(new Short[] { null, 0, 1, -1 }) } },
                { defaultConf, "Array(Int16)", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseShortArrayValue.of(new short[] { 0, 1, -1 }),
                                ClickHouseArrayValue.of(new Short[] { 0, 1, -1 }) } },
                { defaultConf, "Array(Nullable(UInt16))", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseArrayValue.of(new UnsignedShort[] { null, UnsignedShort.ZERO,
                                UnsignedShort.ONE, UnsignedShort.MAX_VALUE }) } },
                { defaultConf, "Array(UInt16)", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseShortArrayValue.ofUnsigned(new short[] { 0, 1, -1 }),
                                ClickHouseArrayValue.of(new UnsignedShort[] { UnsignedShort.ZERO, UnsignedShort.ONE,
                                        UnsignedShort.MAX_VALUE }) } },
                { defaultConf, "Array(Nullable(Int32))", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseArrayValue.of(new Integer[] { null, 0, 1, -1 }) } },
                { defaultConf, "Array(Int32)", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseIntArrayValue.of(new int[] { 0, 1, -1 }),
                                ClickHouseArrayValue.of(new Integer[] { 0, 1, -1 }) } },
                { defaultConf, "Array(Nullable(UInt32))", "0,1,-1",
                        new ClickHouseValue[] {
                                ClickHouseArrayValue.of(new UnsignedInteger[] { null, UnsignedInteger.ZERO,
                                        UnsignedInteger.ONE, UnsignedInteger.MAX_VALUE }) } },
                { defaultConf, "Array(UInt32)", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseIntArrayValue.ofUnsigned(new int[] { 0, 1, -1 }),
                                ClickHouseArrayValue
                                        .of(new UnsignedInteger[] { UnsignedInteger.ZERO, UnsignedInteger.ONE,
                                                UnsignedInteger.MAX_VALUE }) } },
                { defaultConf, "Array(Nullable(Int64))", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseArrayValue.of(new Long[] { null, 0L, 1L, -1L }) } },
                { defaultConf, "Array(Int64)", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseLongArrayValue.of(new long[] { 0L, 1L, -1L }),
                                ClickHouseArrayValue.of(new Long[] { 0L, 1L, -1L }) } },
                { defaultConf, "Array(Nullable(UInt64))", "0,1,-1",
                        new ClickHouseValue[] {
                                ClickHouseArrayValue.of(new UnsignedLong[] { null, UnsignedLong.ZERO,
                                        UnsignedLong.ONE, UnsignedLong.MAX_VALUE }) } },
                { defaultConf, "Array(UInt64)", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseLongArrayValue.ofUnsigned(new long[] { 0L, 1L, -1L }),
                                ClickHouseArrayValue
                                        .of(new UnsignedLong[] { UnsignedLong.ZERO, UnsignedLong.ONE,
                                                UnsignedLong.MAX_VALUE }) } },
                { defaultConf, "Array(Nullable(Float32))", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseArrayValue.of(new Float[] { null, 0F, 1F, -1F }) } },
                { defaultConf, "Array(Float32)", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseFloatArrayValue.of(new float[] { 0F, 1F, -1F }),
                                ClickHouseArrayValue.of(new Float[] { 0F, 1F, -1F }) } },
                { defaultConf, "Array(Nullable(Float64))", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseArrayValue.of(new Double[] { null, 0D, 1D, -1D }) } },
                { defaultConf, "Array(Float64)", "0,1,-1",
                        new ClickHouseValue[] { ClickHouseDoubleArrayValue.of(new double[] { 0D, 1D, -1D }),
                                ClickHouseArrayValue.of(new Double[] { 0D, 1D, -1D }) } },
                { defaultConf, "Array(Nullable(String))", "4bcd",
                        new ClickHouseValue[] {
                                ClickHouseArrayValue.of(new String[] { null, "", "B", "BC", "BCD" }) } },
                { defaultConf, "Array(String)", "4bcd",
                        new ClickHouseValue[] { ClickHouseArrayValue.of(new String[] { "", "B", "BC", "BCD" }) } },

                { binStrConf, "Array(Nullable(String))", "4bcd",
                        new ClickHouseValue[] {
                                ClickHouseArrayValue
                                        .of(new byte[][] { null, {}, { 66 }, { 66, 67 }, { 66, 67, 68 } }),
                                ClickHouseArrayValue
                                        .of(new Object[] { null, new byte[0], new byte[] { 66 }, new byte[] { 66, 67 },
                                                new byte[] { 66, 67, 68 } })
                        } },
                { binStrConf, "Array(String)", "4bcd",
                        new ClickHouseValue[] { ClickHouseArrayValue
                                .of(new byte[][] { {}, { 66 }, { 66, 67 }, { 66, 67, 68 } }),
                                ClickHouseArrayValue
                                        .of(new Object[] { new byte[0], new byte[] { 66 }, new byte[] { 66, 67 },
                                                new byte[] { 66, 67, 68 } }) } },

        };
    }

    @DataProvider(name = "simpleTypesForRead")
    protected Object[][] getSimpleTypesForRead() {
        ClickHouseDataConfig defaultConf = new ClickHouseTestDataConfig();
        ClickHouseDataConfig binStrConf = new ClickHouseTestDataConfig() {
            @Override
            public boolean isUseBinaryString() {
                return true;
            }
        };
        ClickHouseDataConfig widenUtConf = new ClickHouseTestDataConfig() {
            @Override
            public boolean isWidenUnsignedTypes() {
                return true;
            }
        };

        return new Object[][] {
                { defaultConf, "Nullable(Bool)", "null", ClickHouseBoolValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { defaultConf, "Nullable(Bool)", "0", ClickHouseBoolValue.class, false, "false",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { defaultConf, "Nullable(Bool)", "1", ClickHouseBoolValue.class, true, "true",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { defaultConf, "Bool", "0", ClickHouseBoolValue.class, false, "false",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { defaultConf, "Bool", "1", ClickHouseBoolValue.class, true, "true",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { defaultConf, "Nullable(Int8)", "null", ClickHouseByteValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { defaultConf, "Int8", "0", ClickHouseByteValue.class, (byte) 0, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { defaultConf, "Int8", "1", ClickHouseByteValue.class, (byte) 1, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { defaultConf, "Int8", "-1", ClickHouseByteValue.class, (byte) 255, "-1",
                        (Consumer<ClickHouseValue>) this::checkNegativeOne },
                { defaultConf, "Nullable(UInt8)", "null", UnsignedByteValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { defaultConf, "UInt8", "0", UnsignedByteValue.class, UnsignedByte.ZERO, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { defaultConf, "UInt8", "1", UnsignedByteValue.class, UnsignedByte.ONE, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { defaultConf, "UInt8", "-1", UnsignedByteValue.class, UnsignedByte.MAX_VALUE, "255",
                        (Consumer<ClickHouseValue>) this::checkMaxUnsignedByte },
                { defaultConf, "Nullable(Int16)", "null", ClickHouseShortValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { defaultConf, "Int16", "0", ClickHouseShortValue.class, (short) 0, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { defaultConf, "Int16", "1", ClickHouseShortValue.class, (short) 1, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { defaultConf, "Int16", "-1", ClickHouseShortValue.class, (short) 65535, "-1",
                        (Consumer<ClickHouseValue>) this::checkNegativeOne },
                { defaultConf, "Nullable(UInt16)", "null", UnsignedShortValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { defaultConf, "UInt16", "0", UnsignedShortValue.class, UnsignedShort.ZERO, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { defaultConf, "UInt16", "1", UnsignedShortValue.class, UnsignedShort.ONE, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { defaultConf, "UInt16", "-1", UnsignedShortValue.class, UnsignedShort.MAX_VALUE, "65535",
                        (Consumer<ClickHouseValue>) this::checkMaxUnsignedShort },
                { defaultConf, "Nullable(Int32)", "null", ClickHouseIntegerValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { defaultConf, "Int32", "0", ClickHouseIntegerValue.class, 0, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { defaultConf, "Int32", "1", ClickHouseIntegerValue.class, 1, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { defaultConf, "Int32", "-1", ClickHouseIntegerValue.class, -1, "-1",
                        (Consumer<ClickHouseValue>) this::checkNegativeOne },
                { defaultConf, "Nullable(UInt32)", "null", UnsignedIntegerValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { defaultConf, "UInt32", "0", UnsignedIntegerValue.class, UnsignedInteger.ZERO, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { defaultConf, "UInt32", "1", UnsignedIntegerValue.class, UnsignedInteger.ONE, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { defaultConf, "UInt32", "-1", UnsignedIntegerValue.class, UnsignedInteger.MAX_VALUE, "4294967295",
                        (Consumer<ClickHouseValue>) this::checkMaxUnsignedInt },
                { defaultConf, "Nullable(Int64)", "null", ClickHouseLongValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { defaultConf, "Int64", "0", ClickHouseLongValue.class, 0L, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { defaultConf, "Int64", "1", ClickHouseLongValue.class, 1L, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { defaultConf, "Int64", "-1", ClickHouseLongValue.class, -1L, "-1",
                        (Consumer<ClickHouseValue>) this::checkNegativeOne },
                { defaultConf, "Nullable(UInt64)", "null", UnsignedLongValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { defaultConf, "UInt64", "0", UnsignedLongValue.class, UnsignedLong.ZERO, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { defaultConf, "UInt64", "1", UnsignedLongValue.class, UnsignedLong.ONE, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { defaultConf, "UInt64", "-1", UnsignedLongValue.class, UnsignedLong.MAX_VALUE, "18446744073709551615",
                        (Consumer<ClickHouseValue>) this::checkMaxUnsignedLong },
                { defaultConf, "Nullable(Float32)", "null", ClickHouseFloatValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { defaultConf, "Float32", "0", ClickHouseFloatValue.class, 0F, "0.0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { defaultConf, "Float32", "1", ClickHouseFloatValue.class, 1F, "1.0",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { defaultConf, "Float32", "-1", ClickHouseFloatValue.class, -1F, "-1.0",
                        (Consumer<ClickHouseValue>) this::checkNegativeOne },
                { defaultConf, "Nullable(Float64)", "null", ClickHouseDoubleValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { defaultConf, "Float64", "0", ClickHouseDoubleValue.class, 0D, "0.0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { defaultConf, "Float64", "1", ClickHouseDoubleValue.class, 1D, "1.0",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { defaultConf, "Float64", "-1", ClickHouseDoubleValue.class, -1D, "-1.0",
                        (Consumer<ClickHouseValue>) this::checkNegativeOne },
                { defaultConf, "Nullable(String)", "null", ClickHouseStringValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { defaultConf, "String", "1", ClickHouseStringValue.class, "1", "1",
                        (Consumer<ClickHouseValue>) this::checkTextString },

                { binStrConf, "Nullable(String)", "null", ClickHouseStringValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { binStrConf, "String", "0", ClickHouseStringValue.class, "0", "0",
                        (Consumer<ClickHouseValue>) this::checkBinaryString },

                { widenUtConf, "Nullable(Bool)", "null", ClickHouseBoolValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { widenUtConf, "Nullable(Bool)", "0", ClickHouseBoolValue.class, false, "false",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { widenUtConf, "Nullable(Bool)", "1", ClickHouseBoolValue.class, true, "true",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { widenUtConf, "Bool", "0", ClickHouseBoolValue.class, false, "false",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { widenUtConf, "Bool", "1", ClickHouseBoolValue.class, true, "true",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { widenUtConf, "Nullable(Int8)", "null", ClickHouseByteValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { widenUtConf, "Int8", "0", ClickHouseByteValue.class, (byte) 0, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { widenUtConf, "Int8", "1", ClickHouseByteValue.class, (byte) 1, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { widenUtConf, "Int8", "-1", ClickHouseByteValue.class, (byte) 255, "-1",
                        (Consumer<ClickHouseValue>) this::checkNegativeOne },
                { widenUtConf, "Nullable(UInt8)", "null", ClickHouseShortValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { widenUtConf, "UInt8", "0", ClickHouseShortValue.class, (short) 0, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { widenUtConf, "UInt8", "1", ClickHouseShortValue.class, (short) 1, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { widenUtConf, "UInt8", "-1", ClickHouseShortValue.class, (short) 255, "255",
                        (Consumer<ClickHouseValue>) this::checkMaxUnsignedByte },
                { widenUtConf, "Nullable(Int16)", "null", ClickHouseShortValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { widenUtConf, "Int16", "0", ClickHouseShortValue.class, (short) 0, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { widenUtConf, "Int16", "1", ClickHouseShortValue.class, (short) 1, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { widenUtConf, "Int16", "-1", ClickHouseShortValue.class, (short) 65535, "-1",
                        (Consumer<ClickHouseValue>) this::checkNegativeOne },
                { widenUtConf, "Nullable(UInt16)", "null", ClickHouseIntegerValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { widenUtConf, "UInt16", "0", ClickHouseIntegerValue.class, 0, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { widenUtConf, "UInt16", "1", ClickHouseIntegerValue.class, 1, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { widenUtConf, "UInt16", "-1", ClickHouseIntegerValue.class, 65535, "65535",
                        (Consumer<ClickHouseValue>) this::checkMaxUnsignedShort },
                { widenUtConf, "Nullable(Int32)", "null", ClickHouseIntegerValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { widenUtConf, "Int32", "0", ClickHouseIntegerValue.class, 0, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { widenUtConf, "Int32", "1", ClickHouseIntegerValue.class, 1, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { widenUtConf, "Int32", "-1", ClickHouseIntegerValue.class, -1, "-1",
                        (Consumer<ClickHouseValue>) this::checkNegativeOne },
                { widenUtConf, "Nullable(UInt32)", "null", ClickHouseLongValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { widenUtConf, "UInt32", "0", ClickHouseLongValue.class, 0L, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { widenUtConf, "UInt32", "1", ClickHouseLongValue.class, 1L, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { widenUtConf, "UInt32", "-1", ClickHouseLongValue.class, 4294967295L, "4294967295",
                        (Consumer<ClickHouseValue>) this::checkMaxUnsignedInt },
                { widenUtConf, "Nullable(Int64)", "null", ClickHouseLongValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { widenUtConf, "Int64", "0", ClickHouseLongValue.class, 0L, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { widenUtConf, "Int64", "1", ClickHouseLongValue.class, 1L, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { widenUtConf, "Int64", "-1", ClickHouseLongValue.class, -1L, "-1",
                        (Consumer<ClickHouseValue>) this::checkNegativeOne },
                { widenUtConf, "Nullable(UInt64)", "null", UnsignedLongValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { widenUtConf, "UInt64", "0", UnsignedLongValue.class, UnsignedLong.ZERO, "0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { widenUtConf, "UInt64", "1", UnsignedLongValue.class, UnsignedLong.ONE, "1",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { widenUtConf, "UInt64", "-1", UnsignedLongValue.class, UnsignedLong.MAX_VALUE, "18446744073709551615",
                        (Consumer<ClickHouseValue>) this::checkMaxUnsignedLong },
                { widenUtConf, "Nullable(Float32)", "null", ClickHouseFloatValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { widenUtConf, "Float32", "0", ClickHouseFloatValue.class, 0F, "0.0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { widenUtConf, "Float32", "1", ClickHouseFloatValue.class, 1F, "1.0",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { widenUtConf, "Float32", "-1", ClickHouseFloatValue.class, -1F, "-1.0",
                        (Consumer<ClickHouseValue>) this::checkNegativeOne },
                { widenUtConf, "Nullable(Float64)", "null", ClickHouseDoubleValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { widenUtConf, "Float64", "0", ClickHouseDoubleValue.class, 0D, "0.0",
                        (Consumer<ClickHouseValue>) this::checkZero },
                { widenUtConf, "Float64", "1", ClickHouseDoubleValue.class, 1D, "1.0",
                        (Consumer<ClickHouseValue>) this::checkOne },
                { widenUtConf, "Float64", "-1", ClickHouseDoubleValue.class, -1D, "-1.0",
                        (Consumer<ClickHouseValue>) this::checkNegativeOne },
                { widenUtConf, "Nullable(String)", "null", ClickHouseStringValue.class, null, null,
                        (Consumer<ClickHouseValue>) this::checkNull },
                { widenUtConf, "String", "1", ClickHouseStringValue.class, "1", "1",
                        (Consumer<ClickHouseValue>) this::checkTextString },
        };
    }

    @DataProvider(name = "simpleTypesForWrite")
    protected Object[][] getSimpleTypesForWrite() {
        ClickHouseDataConfig defaultConf = new ClickHouseTestDataConfig();
        ClickHouseDataConfig binStrConf = new ClickHouseTestDataConfig() {
            @Override
            public boolean isUseBinaryString() {
                return true;
            }
        };
        ClickHouseDataConfig widenUtConf = new ClickHouseTestDataConfig() {
            @Override
            public boolean isWidenUnsignedTypes() {
                return true;
            }
        };

        return new Object[][] {
                { defaultConf, "Nullable(Bool)", "null", new ClickHouseValue[] { ClickHouseBoolValue.ofNull() } },
                { defaultConf, "Nullable(Bool)", "0", new ClickHouseValue[] { ClickHouseBoolValue.of(false) } },
                { defaultConf, "Nullable(Bool)", "1", new ClickHouseValue[] { ClickHouseBoolValue.of(true) } },
                { defaultConf, "Bool", "0", new ClickHouseValue[] { ClickHouseBoolValue.of(false) } },
                { defaultConf, "Bool", "1", new ClickHouseValue[] { ClickHouseBoolValue.of(true) } },
                { defaultConf, "Nullable(Int8)", "null",
                        new ClickHouseValue[] { ClickHouseByteValue.ofNull(), ClickHouseShortValue.ofNull() } },
                { defaultConf, "Int8", "0",
                        new ClickHouseValue[] { ClickHouseByteValue.of(0), ClickHouseByteValue.ofUnsigned(0) } },
                { defaultConf, "Int8", "1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(1), ClickHouseByteValue.ofUnsigned(1) } },
                { defaultConf, "Int8", "-1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(-1), ClickHouseShortValue.of(-1),
                                ClickHouseByteValue.of(255) } },
                { defaultConf, "Nullable(UInt8)", "null",
                        new ClickHouseValue[] { ClickHouseByteValue.ofUnsignedNull(), ClickHouseShortValue.ofNull() } },
                { defaultConf, "UInt8", "0",
                        new ClickHouseValue[] { ClickHouseByteValue.of(0), ClickHouseByteValue.ofUnsigned(0) } },
                { defaultConf, "UInt8", "1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(1), ClickHouseByteValue.ofUnsigned(1) } },
                { defaultConf, "UInt8", "-1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(255), ClickHouseShortValue.of(255),
                                ClickHouseByteValue.ofUnsigned(255) } },
                { defaultConf, "Nullable(Int16)", "null",
                        new ClickHouseValue[] { ClickHouseShortValue.ofNull(), ClickHouseIntegerValue.ofNull() } },
                { defaultConf, "Int16", "0",
                        new ClickHouseValue[] { ClickHouseByteValue.of(0), ClickHouseShortValue.of(0),
                                ClickHouseShortValue.ofUnsigned(0) } },
                { defaultConf, "Int16", "1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(1), ClickHouseShortValue.of(1),
                                ClickHouseShortValue.ofUnsigned(1) } },
                { defaultConf, "Int16", "-1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(-1), ClickHouseShortValue.of(-1),
                                ClickHouseShortValue.of(65535) } },
                { defaultConf, "Nullable(UInt16)", "null",
                        new ClickHouseValue[] { ClickHouseShortValue.ofUnsignedNull(),
                                ClickHouseIntegerValue.ofNull() } },
                { defaultConf, "UInt16", "0",
                        new ClickHouseValue[] { ClickHouseByteValue.of(0), ClickHouseShortValue.of(0),
                                ClickHouseShortValue.ofUnsigned(0) } },
                { defaultConf, "UInt16", "1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(1), ClickHouseShortValue.of(1),
                                ClickHouseShortValue.ofUnsigned(1) } },
                { defaultConf, "UInt16", "-1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(-1), ClickHouseShortValue.of(-1),
                                ClickHouseShortValue.ofUnsigned(-1), ClickHouseIntegerValue.of(65535) } },
                { defaultConf, "Nullable(Int32)", "null",
                        new ClickHouseValue[] { ClickHouseIntegerValue.ofNull(), ClickHouseLongValue.ofNull() } },
                { defaultConf, "Int32", "0",
                        new ClickHouseValue[] { ClickHouseByteValue.of(0), ClickHouseShortValue.of(0),
                                ClickHouseIntegerValue.of(0), ClickHouseIntegerValue.ofUnsigned(0) } },
                { defaultConf, "Int32", "1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(1), ClickHouseShortValue.of(1),
                                ClickHouseIntegerValue.of(1), ClickHouseIntegerValue.ofUnsigned(1) } },
                { defaultConf, "Int32", "-1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(-1), ClickHouseShortValue.of(-1),
                                ClickHouseIntegerValue.of(-1), ClickHouseIntegerValue.ofUnsigned(-1),
                                ClickHouseLongValue.of(4294967295L) } },
                { defaultConf, "Nullable(UInt32)", "null",
                        new ClickHouseValue[] { ClickHouseIntegerValue.ofUnsignedNull(),
                                ClickHouseLongValue.ofNull() } },
                { defaultConf, "UInt32", "0",
                        new ClickHouseValue[] { ClickHouseByteValue.of(0), ClickHouseShortValue.of(0),
                                ClickHouseIntegerValue.of(0), ClickHouseIntegerValue.ofUnsigned(0) } },
                { defaultConf, "UInt32", "1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(1), ClickHouseShortValue.of(1),
                                ClickHouseIntegerValue.of(1), ClickHouseIntegerValue.ofUnsigned(1) } },
                { defaultConf, "UInt32", "-1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(-1), ClickHouseShortValue.of(-1),
                                ClickHouseIntegerValue.of(-1), ClickHouseIntegerValue.ofUnsigned(-1),
                                ClickHouseLongValue.of(4294967295L) } },
                { defaultConf, "Nullable(Int64)", "null",
                        new ClickHouseValue[] { ClickHouseLongValue.ofNull(), ClickHouseLongValue.ofUnsignedNull() } },
                { defaultConf, "Int64", "0",
                        new ClickHouseValue[] { ClickHouseByteValue.of(0), ClickHouseShortValue.of(0),
                                ClickHouseIntegerValue.of(0), ClickHouseLongValue.of(0L),
                                ClickHouseLongValue.ofUnsigned(0L) } },
                { defaultConf, "Int64", "1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(1), ClickHouseShortValue.of(1),
                                ClickHouseIntegerValue.of(1), ClickHouseLongValue.of(1L),
                                ClickHouseLongValue.ofUnsigned(1L) } },
                { defaultConf, "Int64", "-1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(-1), ClickHouseShortValue.of(-1),
                                ClickHouseIntegerValue.of(-1), ClickHouseLongValue.of(-1L),
                                ClickHouseLongValue.ofUnsigned(-1L),
                                ClickHouseBigIntegerValue.of(new BigInteger("18446744073709551615"))
                        } },
                { defaultConf, "Nullable(UInt64)", "null",
                        new ClickHouseValue[] { ClickHouseLongValue.ofNull(), ClickHouseLongValue.ofUnsignedNull(),
                                ClickHouseBigIntegerValue.ofNull() } },
                { defaultConf, "UInt64", "0",
                        new ClickHouseValue[] { ClickHouseByteValue.of(0), ClickHouseShortValue.of(0),
                                ClickHouseIntegerValue.of(0), ClickHouseLongValue.of(0L),
                                ClickHouseLongValue.ofUnsigned(0L) } },
                { defaultConf, "UInt64", "1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(1), ClickHouseShortValue.of(1),
                                ClickHouseIntegerValue.of(1), ClickHouseLongValue.of(1L),
                                ClickHouseLongValue.ofUnsigned(1L) } },
                { defaultConf, "UInt64", "-1",
                        new ClickHouseValue[] { ClickHouseByteValue.of(-1), ClickHouseShortValue.of(-1),
                                ClickHouseIntegerValue.of(-1), ClickHouseLongValue.of(-1L),
                                ClickHouseLongValue.ofUnsigned(-1L),
                                ClickHouseBigIntegerValue.of(new BigInteger("18446744073709551615"))
                        } },
                { defaultConf, "Nullable(Float32)", "null",
                        new ClickHouseValue[] { ClickHouseFloatValue.ofNull(), ClickHouseDoubleValue.ofNull() } },
                { defaultConf, "Float32", "0", new ClickHouseValue[] { ClickHouseFloatValue.of(0F) } },
                { defaultConf, "Float32", "1", new ClickHouseValue[] { ClickHouseFloatValue.of(1F) } },
                { defaultConf, "Float32", "-1", new ClickHouseValue[] { ClickHouseFloatValue.of(-1F) } },
                { defaultConf, "Nullable(Float64)", "null",
                        new ClickHouseValue[] { ClickHouseDoubleValue.ofNull(), ClickHouseFloatValue.ofNull(),
                                ClickHouseBigDecimalValue.ofNull() } },
                { defaultConf, "Float64", "0",
                        new ClickHouseValue[] { ClickHouseFloatValue.of(0F), ClickHouseDoubleValue.of(0D) } },
                { defaultConf, "Float64", "1",
                        new ClickHouseValue[] { ClickHouseFloatValue.of(1F), ClickHouseDoubleValue.of(1D) } },
                { defaultConf, "Float64", "-1",
                        new ClickHouseValue[] { ClickHouseFloatValue.of(-1F), ClickHouseDoubleValue.of(-1D) } },
                { defaultConf, "Nullable(String)", "null",
                        new ClickHouseValue[] { ClickHouseStringValue.ofNull(), ClickHouseStringValue.of((byte[]) null),
                                ClickHouseStringValue.of((String) null), ClickHouseIntegerValue.ofNull(),
                                ClickHouseFloatValue.ofNull(), ClickHouseBigDecimalValue.ofNull() } },
                { defaultConf, "String", "1",
                        new ClickHouseValue[] { ClickHouseStringValue.of("1"),
                                ClickHouseStringValue.of(new byte[] { 49 }) }, },

                { binStrConf, "Nullable(String)", "null",
                        new ClickHouseValue[] { ClickHouseStringValue.ofNull(), ClickHouseStringValue.of((byte[]) null),
                                ClickHouseStringValue.of((String) null), ClickHouseIntegerValue.ofNull(),
                                ClickHouseFloatValue.ofNull(), ClickHouseBigDecimalValue.ofNull() } },
                { binStrConf, "String", "1",
                        new ClickHouseValue[] { ClickHouseStringValue.of("1"),
                                ClickHouseStringValue.of(new byte[] { 49 }) }, },
        };
    }

    protected ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseDataConfig config,
            ClickHouseColumn column, ClickHouseInputStream input) throws IOException {
        if (ref == null) {
            ref = column.newValue(config);
        }
        return getDataProcessor(config, column, input, null).getDeserializer(config, column).deserialize(ref, input);
    }

    protected void serialize(ClickHouseValue value, ClickHouseDataConfig config, ClickHouseColumn column,
            ClickHouseOutputStream output) throws IOException {
        ClickHouseDataProcessor p = getDataProcessor(config, column, null, output);
        ClickHouseValue v = column.newValue(config);
        if (v.getClass() != value.getClass()) {
            v.update(value);
        } else {
            v = value;
        }
        p.write(v);
    }

    @Test(dataProvider = "nestedTypesForRead", groups = { "unit" })
    public void testDeserializeNestedTypes(ClickHouseDataConfig config, String typeName, String dataKey,
            String valueClass, Object arrVal, Object objVal, String strVal, String sqlExpr) throws IOException {
        try (ClickHouseInputStream in = getInputData(typeName, dataKey)) {
            ClickHouseColumn column = ClickHouseColumn.of("a", typeName);
            ClickHouseValue value = deserialize(null, config, column, in);

            Assert.assertEquals(value.getClass().getSimpleName(), valueClass);
            if (column.isArray()) {
                Assert.assertEquals(value.asArray(), arrVal);
                Assert.assertEquals(value.asObject(), objVal);
                Assert.assertEquals(value.asString(), strVal);
            } else if (column.isMap()) {
                // Assert.assertEquals(value.asObject(), objVal);
            } else if (column.isTuple()) {
                Assert.assertEquals(value.asArray(), arrVal);
                Assert.assertEquals(value.asString(), strVal);
            }
            Assert.assertEquals(value.toSqlExpression(), sqlExpr);
        }
    }

    @Test(dataProvider = "nestedTypesForWrite", groups = { "unit" })
    public void testSerializeNestedTypes(ClickHouseDataConfig config, String typeName, String dataKey,
            ClickHouseValue... values) throws IOException {
        if (values == null || values.length == 0) {
            Assert.fail("At least one value must be provided for testing");
        }

        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        for (ClickHouseValue value : values) {
            bao.reset();
            try (ClickHouseOutputStream output = ClickHouseOutputStream.of(bao)) {
                serialize(value, config, ClickHouseColumn.of("a", typeName), output);
            }
            Assert.assertEquals(bao.toByteArray(), getRawData(typeName, dataKey));

            bao.reset();
            boolean isNullable = typeName.indexOf("Nullable(") > 0;
            String lowCardinalityTypeName = typeName;
            if (isNullable) {
                lowCardinalityTypeName = typeName.replaceFirst("Nullable\\(", "").replaceFirst("\\)", "");
            }
            int startIndex = lowCardinalityTypeName.lastIndexOf('(');
            int endIndex = lowCardinalityTypeName.indexOf(')');
            if (startIndex == -1 || endIndex == -1 || endIndex <= startIndex) {
                // not able to inject LowCardinality type
                continue;
            }
            lowCardinalityTypeName = lowCardinalityTypeName.substring(0, startIndex + 1) + "LowCardinality("
                    + (isNullable ? "Nullable(" : "") + lowCardinalityTypeName.substring(startIndex + 1, endIndex)
                    + (isNullable ? ")" : "") + ")" + lowCardinalityTypeName.substring(endIndex);
            try (ClickHouseOutputStream output = ClickHouseOutputStream.of(bao)) {
                serialize(value, config, ClickHouseColumn.of("a", lowCardinalityTypeName), output);
            }
            Assert.assertEquals(bao.toByteArray(), getRawData(lowCardinalityTypeName, dataKey));
        }
    }

    @Test(dataProvider = "simpleTypesForRead", groups = { "unit" })
    public void testDeserializeSimpleTypes(ClickHouseDataConfig config, String typeName, String dataKey,
            Class<?> valueClass, Object objVal, String strVal, Consumer<ClickHouseValue> customChecks)
            throws IOException {
        try (ClickHouseInputStream in = getInputData(typeName, dataKey)) {
            ClickHouseValue value = deserialize(null, config, ClickHouseColumn.of("a", typeName), in);
            Assert.assertEquals(value.getClass(), valueClass);
            Assert.assertEquals(value.asObject(), objVal);
            Assert.assertEquals(value.asString(), strVal);
            if (customChecks != null) {
                customChecks.accept(value);
            }
        }

        String lowCardinalityTypeName = "LowCardinality(" + typeName + ")";
        try (ClickHouseInputStream in = getInputData(lowCardinalityTypeName, dataKey)) {
            ClickHouseValue value = deserialize(null, config, ClickHouseColumn.of("a", lowCardinalityTypeName), in);
            Assert.assertEquals(value.getClass(), valueClass);
            Assert.assertEquals(value.asObject(), objVal);
            Assert.assertEquals(value.asString(), strVal);
            if (customChecks != null) {
                customChecks.accept(value);
            }
        }
    }

    @Test(dataProvider = "simpleTypesForWrite", groups = { "unit" })
    public void testSerializeSimpleTypes(ClickHouseDataConfig config, String typeName, String dataKey,
            ClickHouseValue... values) throws IOException {
        if (values == null || values.length == 0) {
            Assert.fail("At least one value must be provided for testing");
        }

        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        for (ClickHouseValue value : values) {
            bao.reset();
            try (ClickHouseOutputStream output = ClickHouseOutputStream.of(bao)) {
                serialize(value, config, ClickHouseColumn.of("a", typeName), output);
            }
            Assert.assertEquals(bao.toByteArray(), getRawData(typeName, dataKey));

            bao.reset();
            String lowCardinalityTypeName = "LowCardinality(" + typeName + ")";
            try (ClickHouseOutputStream output = ClickHouseOutputStream.of(bao)) {
                serialize(value, config, ClickHouseColumn.of("a", lowCardinalityTypeName), output);
            }
            Assert.assertEquals(bao.toByteArray(), getRawData(lowCardinalityTypeName, dataKey));
        }
    }
}
