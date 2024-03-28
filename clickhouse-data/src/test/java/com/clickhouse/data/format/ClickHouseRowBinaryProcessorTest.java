package com.clickhouse.data.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.testng.Assert;
import org.testng.annotations.Test;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseTestDataConfig;
import com.clickhouse.data.ClickHouseValue;
import com.clickhouse.data.value.array.ClickHouseByteArrayValue;
import com.clickhouse.data.value.array.ClickHouseShortArrayValue;
import com.clickhouse.data.value.BaseDataProcessorTest;
import com.clickhouse.data.value.ClickHouseArrayValue;
import com.clickhouse.data.value.ClickHouseMapValue;
import com.clickhouse.data.value.ClickHouseNestedValue;
import com.clickhouse.data.value.ClickHouseTupleValue;
import com.clickhouse.data.value.UnsignedByte;
import com.clickhouse.data.value.UnsignedInteger;
import com.clickhouse.data.value.UnsignedShort;

public class ClickHouseRowBinaryProcessorTest extends BaseDataProcessorTest {
    @Override
    protected ClickHouseDataProcessor getDataProcessor(ClickHouseDataConfig config, ClickHouseColumn column,
            ClickHouseInputStream input, ClickHouseOutputStream output) throws IOException {
        return new ClickHouseRowBinaryProcessor(config, input, output, Collections.singletonList(column), null);
    }

    // @Test(groups = { "unit" })
    // public void testDeserializeAggregateFunction() throws IOException {
    // ClickHouseConfig config = new ClickHouseConfig();
    // ClickHouseValue value =
    // deserialize(null, config,
    // ClickHouseColumn.of("a", "AggregateFunction(any, String)"),
    // BinaryStreamUtilsTest.generateInput(0xFF, 0xFF));
    // Assert.assertTrue(value instanceof ClickHouseStringValue);
    // Assert.assertEquals(value.asObject(), null);
    // }

    @Override
    protected byte[] getRawData(String typeName, String key) {
        byte[] data = null;
        while (typeName.indexOf("LowCardinality(") >= 0) {
            typeName = typeName.replaceFirst("LowCardinality\\(", "").replaceFirst("\\)", "");
        }
        if (typeName.startsWith("Nullable(")) {
            if ("null".equals(key)) {
                return toBytes(1);
            } else {
                return toBytes(new byte[1],
                        getRawData(typeName.replaceFirst("Nullable\\(", "").replaceFirst("\\)", ""), key));
            }
        }
        switch (typeName) {
            case "Array(Nullable(Bool))":
                if ("0,1".equals(key)) { // first one will be null
                    data = toBytes(3, 1, 0, 0, 0, 1);
                }
                break;
            case "Array(Bool)":
                if ("0,1".equals(key)) {
                    data = toBytes(2, 0, 1);
                }
                break;
            case "Array(Nullable(Int8))":
            case "Array(Nullable(UInt8))":
                if ("0,1,-1".equals(key)) {
                    data = toBytes(4, 1, 0, 0, 0, 1, 0, 255);
                }
                break;
            case "Array(Int8)":
            case "Array(UInt8)":
                if ("0,1,-1".equals(key)) {
                    data = toBytes(3, 0, 1, 255);
                }
                break;
            case "Array(Nullable(Int16))":
            case "Array(Nullable(UInt16))":
                if ("0,1,-1".equals(key)) {
                    data = toBytes(4, 1, 0, 0, 0, 0, 1, 0, 0, 255, 255);
                }
                break;
            case "Array(Int16)":
            case "Array(UInt16)":
                if ("0,1,-1".equals(key)) {
                    data = toBytes(3, 0, 0, 1, 0, 255, 255);
                }
                break;
            case "Array(Nullable(Int32))":
            case "Array(Nullable(UInt32))":
                if ("0,1,-1".equals(key)) {
                    data = toBytes(4, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 255, 255, 255, 255);
                }
                break;
            case "Array(Int32)":
            case "Array(UInt32)":
                if ("0,1,-1".equals(key)) {
                    data = toBytes(3, 0, 0, 0, 0, 1, 0, 0, 0, 255, 255, 255, 255);
                }
                break;
            case "Array(Nullable(Int64))":
            case "Array(Nullable(UInt64))":
                if ("0,1,-1".equals(key)) {
                    data = toBytes(4, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255,
                            255, 255, 255, 255);
                }
                break;
            case "Array(Int64)":
            case "Array(UInt64)":
                if ("0,1,-1".equals(key)) {
                    data = toBytes(3, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255,
                            255, 255);
                }
                break;
            case "Array(Nullable(Float32))":
                if ("0,1,-1".equals(key)) {
                    data = toBytes(4, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0x80, 0x3F, 0, 0, 0, 0x80, 0xBF);
                }
                break;
            case "Array(Float32)":
                if ("0,1,-1".equals(key)) {
                    data = toBytes(3, 0, 0, 0, 0, 0, 0, 0x80, 0x3F, 0, 0, 0x80, 0xBF);
                }
                break;
            case "Array(Nullable(Float64))":
                if ("0,1,-1".equals(key)) {
                    data = toBytes(4, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xF0, 0x3F, 0, 0, 0, 0, 0, 0,
                            0, 0xF0, 0xBF);
                }
                break;
            case "Array(Float64)":
                if ("0,1,-1".equals(key)) {
                    data = toBytes(3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xF0, 0x3F, 0, 0, 0, 0, 0, 0, 0xF0,
                            0xBF);
                }
                break;
            case "Array(Nullable(String))":
                if ("4bcd".equals(key)) {
                    data = toBytes(5, 1, 0, 0, 0, 1, 66, 0, 2, 66, 67, 0, 3, 66, 67, 68);
                }
                break;
            case "Array(String)":
                if ("4bcd".equals(key)) {
                    data = toBytes(4, 0, 1, 66, 2, 66, 67, 3, 66, 67, 68);
                }
                break;
            case "Array(Array(Array(UInt8)))":
                if ("[[3]],[[1,2],[2,1]],[[4,5],[5,4]]".equals(key)) {
                    data = toBytes(3, 1, 1, 3, 2, 2, 1, 2, 2, 2, 1, 2, 2, 4, 5, 2, 5, 4);
                }
                break;
            case "Array(Array(UInt64))":
                if ("[1,2,3],[3,2,1],[4,5]".equals(key)) {
                    data = toBytes(3, 3, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 3, 3,
                            0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 4, 0, 0, 0, 0, 0, 0,
                            0, 5, 0, 0, 0, 0, 0, 0, 0);
                }
                break;
            case "Map(String,Array(UInt8))":
                if ("[1,2,3],[3,2,1],[4,5]".equals(key)) {
                    data = toBytes(3, 1, 0x61, 3, 1, 2, 3, 1, 0x62, 3, 3, 2, 1, 1, 0x63, 2, 4, 5);
                }
                break;
            case "Tuple(Array(UInt8),Array(UInt8),Array(UInt8))":
                if ("[1,2,3],[3,2,1],[4,5]".equals(key)) {
                    data = toBytes(3, 1, 2, 3, 3, 3, 2, 1, 2, 4, 5);
                }
                break;
            case "Array(Array(String))":
                if ("[foo,bar],[qaz,qux]".equals(key)) {
                    data = toBytes(2, 2, 3, 0X66, 0X6F, 0X6F, 3, 0X62, 0X61, 0X72, 2, 3, 0X71, 0X61, 0X7A, 3, 0X71,
                            0X75, 0X78);
                }
                break;
            case "Bool":
            case "Int8":
            case "UInt8":
                data = toBytes(Integer.parseInt(key));
                break;
            case "Int16":
            case "UInt16":
                if ("0".equals(key)) {
                    data = toBytes(0, 0);
                } else if ("1".equals(key)) {
                    data = toBytes(1, 0);
                } else if ("-1".equals(key)) {
                    data = toBytes(0xFF, 0xFF);
                }
                break;
            case "Int32":
            case "UInt32":
                if ("0".equals(key)) {
                    data = toBytes(0, 0, 0, 0);
                } else if ("1".equals(key)) {
                    data = toBytes(1, 0, 0, 0);
                } else if ("-1".equals(key)) {
                    data = toBytes(0xFF, 0xFF, 0xFF, 0xFF);
                }
                break;
            case "Int64":
            case "UInt64":
                if ("0".equals(key)) {
                    data = toBytes(0, 0, 0, 0, 0, 0, 0, 0);
                } else if ("1".equals(key)) {
                    data = toBytes(1, 0, 0, 0, 0, 0, 0, 0);
                } else if ("-1".equals(key)) {
                    data = toBytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF);
                }
                break;
            case "Float32":
                if ("0".equals(key)) {
                    data = toBytes(0, 0, 0, 0);
                } else if ("1".equals(key)) {
                    data = toBytes(0, 0, 0x80, 0x3F);
                } else if ("-1".equals(key)) {
                    data = toBytes(0, 0, 0x80, 0xBF);
                }
                break;
            case "Float64":
                if ("0".equals(key)) {
                    data = toBytes(0, 0, 0, 0, 0, 0, 0, 0);
                } else if ("1".equals(key)) {
                    data = toBytes(0, 0, 0, 0, 0, 0, 0xF0, 0x3F);
                } else if ("-1".equals(key)) {
                    data = toBytes(0, 0, 0, 0, 0, 0, 0xF0, 0xBF);
                }
                break;
            case "String":
                if ("0".equals(key)) {
                    data = toBytes(1, 48);
                } else if ("1".equals(key)) {
                    data = toBytes(1, 49);
                }
                break;
            default:
                break;
        }

        if (data == null) {
            Assert.fail(String.format("No raw data defined for type=[%s] and key=[%s]", typeName, key));
        }
        return data;
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

    @Test(groups = { "unit" })
    public void testDeserializeArray() throws IOException {
        // widen_unsigned_types=false, use_objects_in_array=false
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();
        ClickHouseValue value = deserialize(null, config,
                ClickHouseColumn.of("a", "Array(UInt8)"), BinaryStreamUtilsTest.generateInput(2, 1, 2));
        Assert.assertTrue(value instanceof ClickHouseByteArrayValue); // UnsignedByteArrayValue
        Assert.assertEquals(value.asObject(), new byte[] { (byte) 1, (byte) 2 });
        Object[] ubyteArray = value.asArray();
        Assert.assertEquals(ubyteArray.length, 2);
        Assert.assertEquals(ubyteArray[0], UnsignedByte.ONE);
        Assert.assertEquals(ubyteArray[1], UnsignedByte.valueOf("2"));

        value = deserialize(null, config,
                ClickHouseColumn.of("a", "Array(Int8)"),
                BinaryStreamUtilsTest.generateInput(2, 1, 2));
        Assert.assertTrue(value instanceof ClickHouseByteArrayValue);
        Assert.assertEquals(value.asObject(), new byte[] { 1, 2 });
        Object[] byteArray = value.asArray();
        Assert.assertEquals(byteArray.length, 2);
        Assert.assertEquals(byteArray[0], Byte.valueOf("1"));
        Assert.assertEquals(byteArray[1], Byte.valueOf("2"));

        value = deserialize(null, config,
                ClickHouseColumn.of("a", "Array(Nullable(Int8))"),
                BinaryStreamUtilsTest.generateInput(2, 0, 1, 0, 2));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        Assert.assertEquals(value.asObject(), new Byte[] { (byte) 1, (byte) 2 });
        byteArray = value.asArray();
        Assert.assertEquals(byteArray.length, 2);
        Assert.assertEquals(byteArray[0], Byte.valueOf("1"));
        Assert.assertEquals(byteArray[1], Byte.valueOf("2"));

        value = deserialize(null, config,
                ClickHouseColumn.of("a", "Array(Array(UInt8))"),
                BinaryStreamUtilsTest.generateInput(1, 2, 1, 2));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        Assert.assertEquals(value.asObject(), new byte[][] { new byte[] { 1, 2 } });
        Object[] array = (Object[]) value.asObject();
        Assert.assertEquals(array.length, 1);
        Assert.assertEquals(array[0], new byte[] { 1, 2 });

        // SELECT arrayZip(['a', 'b', 'c'], [3, 2, 1])
        value = deserialize(null, config,
                ClickHouseColumn.of("a", "Array(Tuple(String, UInt8))"),
                BinaryStreamUtilsTest.generateInput(3, 1, 0x61, 3, 1, 0x62, 2, 1, 0x63, 1));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        array = (Object[]) value.asObject();
        Assert.assertEquals(array.length, 3);
        Assert.assertEquals(((List<Object>) array[0]).size(), 2);
        Assert.assertEquals(((List<Object>) array[0]).get(0), "a");
        Assert.assertEquals(((List<Object>) array[0]).get(1), UnsignedByte.valueOf((byte) 3));
        Assert.assertEquals(((List<Object>) array[1]).size(), 2);
        Assert.assertEquals(((List<Object>) array[1]).get(0), "b");
        Assert.assertEquals(((List<Object>) array[1]).get(1), UnsignedByte.valueOf((byte) 2));
        Assert.assertEquals(((List<Object>) array[2]).size(), 2);
        Assert.assertEquals(((List<Object>) array[2]).get(0), "c");
        Assert.assertEquals(((List<Object>) array[2]).get(1), UnsignedByte.valueOf((byte) 1));

        // insert into x values([{ 'a' : (null, 3), 'b' : (1, 2), 'c' : (2, 1)}])
        value = deserialize(null, config,
                ClickHouseColumn.of("a", "Array(Map(String, Tuple(Nullable(UInt8), UInt16)))"),
                BinaryStreamUtilsTest.generateInput(1, 3, 1, 0x61, 1, 3, 0, 1, 0x62, 0, 1, 2, 0, 1,
                        0x63, 0, 2, 1, 0));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        array = (Object[]) value.asObject();
        Assert.assertEquals(array.length, 1);
        Map<String, List<Object>> map = (Map<String, List<Object>>) array[0];
        Assert.assertEquals(map.size(), 3);
        List<Object> l = map.get("a");
        Assert.assertEquals(l.size(), 2);
        Assert.assertEquals(l.get(0), null);
        Assert.assertEquals(l.get(1), UnsignedShort.valueOf((short) 3));
        l = map.get("b");
        Assert.assertEquals(l.size(), 2);
        Assert.assertEquals(l.get(0), UnsignedByte.valueOf((byte) 1));
        Assert.assertEquals(l.get(1), UnsignedShort.valueOf((short) 2));
        l = map.get("c");
        Assert.assertEquals(l.size(), 2);
        Assert.assertEquals(l.get(0), UnsignedByte.valueOf((byte) 2));
        Assert.assertEquals(l.get(1), UnsignedShort.valueOf((short) 1));

        // now FixedString and String
        ClickHouseDataConfig binConf = new ClickHouseTestDataConfig() {
            @Override
            public boolean isUseBinaryString() {
                return true;
            }
        };
        value = deserialize(null, config,
                ClickHouseColumn.of("a", "Array(FixedString(2))"),
                BinaryStreamUtilsTest.generateInput(1, 0x41, 0x31));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        Assert.assertEquals(value.asObject(), new String[] { "A1" });
        value = deserialize(null, binConf,
                ClickHouseColumn.of("a", "Array(FixedString(2))"),
                BinaryStreamUtilsTest.generateInput(1, 0x41, 0x31));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        Assert.assertEquals(value.asArray().length, 1);
        Assert.assertEquals(value.asArray()[0], new byte[] { 65, 49 });

        value = deserialize(null, config,
                ClickHouseColumn.of("a", "Array(String)"),
                BinaryStreamUtilsTest.generateInput(1, 2, 0x41, 0x31));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        Assert.assertEquals(value.asObject(), new String[] { "A1" });
        value = deserialize(null, config,
                ClickHouseColumn.of("a", "Array(Array(Array(Nullable(String))))"),
                BinaryStreamUtilsTest.generateInput(1, 1, 1, 1, 1, 1, 2, 0x41, 0x31));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        Assert.assertNotNull(value.asArray());
        value = deserialize(null, config,
                ClickHouseColumn.of("a", "Array(Array(Array(Nullable(String))))"),
                BinaryStreamUtilsTest.generateInput(1, 0));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        //Assert.assertEquals(value.asObject(), new String[][][] { new String[][] { new String[] { "A1" } } });
        Assert.assertNotNull(value.asArray());
        value = deserialize(null, binConf,
                ClickHouseColumn.of("a", "Array(String)"),
                BinaryStreamUtilsTest.generateInput(1, 2, 0x41, 0x31));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        Assert.assertEquals(value.asArray().length, 1);
        Assert.assertEquals(value.asArray()[0], new byte[] { 65, 49 });
    }

    @Test(groups = { "unit" })
    public void testSerializeArray() throws IOException {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();
        ClickHouseValue value = ClickHouseShortArrayValue.of(new short[] { 1, 2 });
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ClickHouseOutputStream out = ClickHouseOutputStream.of(bas);
        serialize(value, config,
                ClickHouseColumn.of("a", "Array(UInt8)"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(2, 1, 2));

        value = ClickHouseByteArrayValue.of(new byte[] { 1, 2 });
        bas = new ByteArrayOutputStream();
        out = ClickHouseOutputStream.of(bas);
        serialize(value, config,
                ClickHouseColumn.of("a", "Array(Nullable(Int8))"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(2, 0, 1, 0, 2));

        value = ClickHouseArrayValue.of(new short[][] { new short[] { 1, 2 } });
        bas = new ByteArrayOutputStream();
        out = ClickHouseOutputStream.of(bas);
        serialize(value, config,
                ClickHouseColumn.of("a", "Array(Array(UInt8))"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(1, 2, 1, 2));

        ClickHouseDataConfig binConf = new ClickHouseTestDataConfig() {
            @Override
            public boolean isUseBinaryString() {
                return true;
            }
        };

        for (ClickHouseArrayValue<?> arrVal : new ClickHouseArrayValue[] {
                ClickHouseArrayValue.of(new Object[] { "A1" }),
                ClickHouseArrayValue.of(new Object[] { new byte[] { 65, 49 } })
        }) {
            for (ClickHouseDataConfig c : new ClickHouseDataConfig[] { config, binConf }) {
                bas = new ByteArrayOutputStream();
                try (ClickHouseOutputStream o = ClickHouseOutputStream.of(bas)) {
                    serialize(arrVal, c, ClickHouseColumn.of("a", "Array(FixedString(2))"), o);
                }
                Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(1, 0x41, 0x31));

                bas = new ByteArrayOutputStream();
                try (ClickHouseOutputStream o = ClickHouseOutputStream.of(bas)) {
                    serialize(arrVal, c, ClickHouseColumn.of("a", "Array(String)"), o);
                }
                Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(1, 2, 0x41, 0x31));
            }
        }

        // SELECT arrayZip(['a', 'b', 'c'], [3, 2, 1])
        value = ClickHouseArrayValue
                .of(new Object[] { Arrays.asList("a", (short) 3), Arrays.asList("b", (short) 2),
                        Arrays.asList("c", (short) 1) });
        bas = new ByteArrayOutputStream();
        out = ClickHouseOutputStream.of(bas);
        serialize(value, config,
                ClickHouseColumn.of("a", "Array(Tuple(String, UInt8))"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(),
                BinaryStreamUtilsTest.generateBytes(3, 1, 0x61, 3, 1, 0x62, 2, 1, 0x63, 1));

        // insert into x values([{ 'a' : (null, 3), 'b' : (1, 2), 'c' : (2, 1)}])
        value = ClickHouseArrayValue.of(new Object[] { new LinkedHashMap<String, List<Object>>() {
            {
                put("a", Arrays.asList((Short) null, 3));
                put("b", Arrays.asList(Short.valueOf("1"), 2));
                put("c", Arrays.asList(Short.valueOf("2"), 1));
            }
        } });
        bas = new ByteArrayOutputStream();
        out = ClickHouseOutputStream.of(bas);
        serialize(value, config,
                ClickHouseColumn.of("a", "Array(Map(String, Tuple(Nullable(UInt8), UInt16)))"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(),
                BinaryStreamUtilsTest.generateBytes(1, 3, 1, 0x61, 1, 3, 0, 1, 0x62, 0, 1, 2, 0, 1,
                        0x63, 0, 2, 1, 0));
    }

    @Test(groups = { "unit" })
    public void testSerializeBoolean() throws IOException {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();

        ClickHouseValue value = ClickHouseArrayValue.of(new Boolean[][] { new Boolean[] { true, false } });
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ClickHouseOutputStream out = ClickHouseOutputStream.of(bas);
        serialize(value, config,
                ClickHouseColumn.of("a", "Array(Array(Boolean))"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(1, 2, 1, 0));

        boolean[] nativeBoolArray = new boolean[3];
        nativeBoolArray[0] = true;
        nativeBoolArray[1] = false;
        nativeBoolArray[2] = true;

        ClickHouseValue value2 = ClickHouseArrayValue.of(new boolean[][] { nativeBoolArray });
        ByteArrayOutputStream bas2 = new ByteArrayOutputStream();
        ClickHouseOutputStream out2 = ClickHouseOutputStream.of(bas2);
        serialize(value2, config,
                ClickHouseColumn.of("a", "Array(Array(boolean))"), out2);
        out2.flush();
        Assert.assertEquals(bas2.toByteArray(), BinaryStreamUtilsTest.generateBytes(1, 3, 1, 0, 1));
    }

    @Test(groups = { "unit" })
    public void testDeserializeMap() throws IOException {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();
        ClickHouseValue value = deserialize(null, config,
                ClickHouseColumn.of("m", "Map(UInt8, UInt8)"),
                BinaryStreamUtilsTest.generateInput(2, 2, 2, 1, 1));
        Assert.assertTrue(value instanceof ClickHouseMapValue);
        Map<?, ?> map = (Map<?, ?>) value.asObject();
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(map.get(UnsignedByte.valueOf((byte) 2)), UnsignedByte.valueOf((byte) 2));
        Assert.assertEquals(map.get(UnsignedByte.ONE), UnsignedByte.ONE);

        value = deserialize(null, config,
                ClickHouseColumn.of("m", "Map(String, UInt32)"),
                BinaryStreamUtilsTest.generateInput(2, 1, 0x32, 2, 0, 0, 0, 1, 0x31, 1, 0, 0, 0));
        Assert.assertTrue(value instanceof ClickHouseMapValue);
        map = (Map<?, ?>) value.asObject();
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(map.get("2"), UnsignedInteger.TWO);
        Assert.assertEquals(map.get("1"), UnsignedInteger.ONE);
    }

    @Test(groups = { "unit" })
    public void testSerializeMap() throws IOException {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();
        ClickHouseValue value = ClickHouseMapValue.of(new LinkedHashMap<Short, Short>() {
            {
                put((short) 2, (short) 2);
                put((short) 1, (short) 1);
            }
        }, Short.class, Short.class);
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ClickHouseOutputStream out = ClickHouseOutputStream.of(bas);
        serialize(value, config,
                ClickHouseColumn.of("m", "Map(UInt8, UInt8)"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(2, 2, 2, 1, 1));

        value = ClickHouseMapValue.of(new LinkedHashMap<String, Long>() {
            {
                put("2", 2L);
                put("1", 1L);
            }
        }, String.class, Long.class);
        bas = new ByteArrayOutputStream();
        out = ClickHouseOutputStream.of(bas);
        serialize(value, config,
                ClickHouseColumn.of("m", "Map(String, UInt32)"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(),
                BinaryStreamUtilsTest.generateBytes(2, 1, 0x32, 2, 0, 0, 0, 1, 0x31, 1, 0, 0, 0));
    }

    @Test(groups = { "unit" })
    public void testDeserializeNested() throws IOException {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();
        ClickHouseValue value = deserialize(null, config,
                ClickHouseColumn.of("n", "Nested(n1 UInt8, n2 Nullable(String), n3 Tuple(x Int16))"),
                // two nested rows: values([(1,'a',(0)),(0,'b',(1))])
                BinaryStreamUtilsTest.generateInput(2, 1, 0, 1, 0x61, 0, 0, 0, 0, 1, 0x62, 1, 0));
        Assert.assertTrue(value instanceof ClickHouseNestedValue);

        List<ClickHouseColumn> columns = ((ClickHouseNestedValue) value).getColumns();
        Object[][] values = (Object[][]) value.asObject();
        Assert.assertEquals(columns.size(), 3);
        Assert.assertEquals(columns.get(0).getColumnName(), "n1");
        Assert.assertEquals(columns.get(1).getColumnName(), "n2");
        Assert.assertEquals(columns.get(2).getColumnName(), "n3");
        Assert.assertEquals(values.length, 2);
        Assert.assertEquals(values[0],
                new Object[] { UnsignedByte.ONE, "a", new ArrayList<>(Collections.singleton((short) 0)) });
        Assert.assertEquals(values[1],
                new Object[] { UnsignedByte.ZERO, "b", new ArrayList<>(Collections.singleton((short) 1)) });
    }

    @Test(groups = { "unit" })
    public void testSerializeNested() throws IOException {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();
        ClickHouseValue value = ClickHouseNestedValue.of(
                ClickHouseColumn.of("n", "Nested(n1 UInt8, n2 Nullable(String), n3 Tuple(x Int16))")
                        .getNestedColumns(),
                new Object[][] { { UnsignedByte.ONE, "a", new ArrayList<>(Collections.singleton((short) 0)) },
                        { UnsignedByte.ZERO, "b", new ArrayList<>(Collections.singleton((short) 1)) } });
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ClickHouseOutputStream out = ClickHouseOutputStream.of(bas);
        serialize(value, config,
                ClickHouseColumn.of("n", "Nested(n1 UInt8, n2 Nullable(String), n3 Tuple(x Int16))"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(),
                BinaryStreamUtilsTest.generateBytes(2, 1, 0, 1, 0x61, 0, 0, 0, 0, 1, 0x62, 1, 0));
    }

    @Test(groups = { "unit" })
    public void testDeserializeTuple() throws IOException {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();
        ClickHouseValue value = deserialize(null, config,
                ClickHouseColumn.of("t", "Tuple(UInt8, String)"),
                BinaryStreamUtilsTest.generateInput(1, 1, 0x61));
        Assert.assertTrue(value instanceof ClickHouseTupleValue);
        List<Object> values = (List<Object>) value.asObject();
        Assert.assertEquals(values.size(), 2);
        Assert.assertEquals(values.get(0), UnsignedByte.ONE);
        Assert.assertEquals(values.get(1), "a");

        value = deserialize(value, config,
                ClickHouseColumn.of("t", "Tuple(UInt32, Int128, Nullable(IPv4)))"),
                BinaryStreamUtilsTest.generateInput(
                        1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x05,
                        0xa8, 0xc0));
        Assert.assertTrue(value instanceof ClickHouseTupleValue);
        values = (List<Object>) value.asObject();
        Assert.assertEquals(values.size(), 3);
        Assert.assertEquals(values.get(0), UnsignedInteger.ONE);
        Assert.assertEquals(values.get(1), BigInteger.valueOf(2));
        Assert.assertEquals(values.get(2), InetAddress.getByName("192.168.5.1"));
    }

    @Test(groups = { "unit" })
    public void testSerializeTuple() throws IOException {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();
        ClickHouseValue value = ClickHouseTupleValue.of((byte) 1, "a");
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ClickHouseOutputStream out = ClickHouseOutputStream.of(bas);
        serialize(value, config,
                ClickHouseColumn.of("t", "Tuple(UInt8, String)"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(1, 1, 0x61));

        value = ClickHouseTupleValue.of(1L, BigInteger.valueOf(2), InetAddress.getByName("192.168.5.1"));
        bas = new ByteArrayOutputStream();
        out = ClickHouseOutputStream.of(bas);
        serialize(value, config,
                ClickHouseColumn.of("t", "Tuple(UInt32, Int128, Nullable(IPv4)))"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(),
                BinaryStreamUtilsTest.generateBytes(1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x05, 0xa8, 0xc0));
    }

    @Test(groups = { "unit" })
    public void testDeserializeJson() throws IOException {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();
        // '{"n":7,"s":"string","o":{"x":1,"y":[3,2,1]},"a":[{"a":1},{"b":{"c":2}}]}'
        // ([(1,(0)),(0,(2))],7,(1,[3,2,1]),'string')
        ClickHouseColumn column = ClickHouseColumn.of("t",
                "Tuple(a Nested(a Int8, b Tuple(c Int8)), n Int8, o Tuple(x Int8, y Array(Int8)), s String)");
        ClickHouseValue value = deserialize(null, config, column, BinaryStreamUtilsTest.generateInput(2, 1, 0, 0, 2, 7,
                1, 3, 3, 2, 1, 6, 0x73, 0x74, 0x72, 0x69, 0x6E, 0x67));
        Assert.assertTrue(value instanceof ClickHouseTupleValue);
        List<Object> values = (List<Object>) value.asObject();
        Assert.assertEquals(values.size(), 4);
        Object[] objs = (Object[]) values.get(0);
        Assert.assertEquals(objs.length, 2);
        Assert.assertEquals(objs[0], new Object[] { (byte) 1, Arrays.asList((byte) 0) });
        Assert.assertEquals(objs[1], new Object[] { (byte) 0, Arrays.asList((byte) 2) });
        Assert.assertEquals(values.get(1), (byte) 7);
        List<Object> list = (List<Object>) values.get(2);
        Assert.assertEquals(list.size(), 2);
        Assert.assertEquals(list.get(0), (byte) 1);
        Assert.assertEquals(list.get(1), new byte[] { 3, 2, 1 });
        Assert.assertEquals(values.get(3), "string");
    }

    @Test(groups = { "unit" })
    public void testSerializeJson() throws IOException {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();
        ClickHouseValue value = ClickHouseTupleValue.of(new Object[][] {
                { (byte) 1, Arrays.asList((byte) 0) },
                { (byte) 0, Arrays.asList((byte) 2) }
        }, (byte) 7, Arrays.asList((byte) 1, new byte[] { 3, 2, 1 }), "string");
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ClickHouseOutputStream out = ClickHouseOutputStream.of(bas);
        serialize(value, config,
                ClickHouseColumn.of("t",
                        "Tuple(a Nested(a Int8, b Tuple(c Int8)), n Int8, o Tuple(x Int8, y Array(Int8)), s String)"),
                out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(2, 1, 0, 0, 2, 7,
                1, 3, 3, 2, 1, 6, 0x73, 0x74, 0x72, 0x69, 0x6E, 0x67));
    }
}
