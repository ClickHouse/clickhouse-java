package com.clickhouse.client.data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseOutputStream;
import com.clickhouse.client.ClickHouseValue;
import com.clickhouse.client.data.array.ClickHouseByteArrayValue;
import com.clickhouse.client.data.array.ClickHouseShortArrayValue;

public class ClickHouseRowBinaryProcessorTest {
    private ClickHouseRowBinaryProcessor newProcessor(int... bytes) throws IOException {
        return new ClickHouseRowBinaryProcessor(new ClickHouseConfig(),
                BinaryStreamUtilsTest.generateInput(bytes),
                null, Collections.emptyList(), null);
    }

    // @Test(groups = { "unit" })
    // public void testDeserializeAggregateFunction() throws IOException {
    // ClickHouseConfig config = new ClickHouseConfig();
    // ClickHouseValue value =
    // ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
    // ClickHouseColumn.of("a", "AggregateFunction(any, String)"),
    // BinaryStreamUtilsTest.generateInput(0xFF, 0xFF));
    // Assert.assertTrue(value instanceof ClickHouseStringValue);
    // Assert.assertEquals(value.asObject(), null);
    // }

    @Test(groups = { "unit" })
    public void testDeserializeArray() throws IOException {
        ClickHouseConfig config = new ClickHouseConfig();
        ClickHouseValue value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ClickHouseColumn.of("a", "Array(UInt8)"), BinaryStreamUtilsTest.generateInput(2, 1, 2));
        Assert.assertTrue(value instanceof ClickHouseShortArrayValue);
        Assert.assertEquals(value.asObject(), new short[] { 1, 2 });
        Object[] shortArray = value.asArray();
        Assert.assertEquals(shortArray.length, 2);
        Assert.assertEquals(shortArray[0], Short.valueOf("1"));
        Assert.assertEquals(shortArray[1], Short.valueOf("2"));

        value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ClickHouseColumn.of("a", "Array(Int8)"),
                BinaryStreamUtilsTest.generateInput(2, 1, 2));
        Assert.assertTrue(value instanceof ClickHouseByteArrayValue);
        Assert.assertEquals(value.asObject(), new byte[] { 1, 2 });
        Object[] byteArray = value.asArray();
        Assert.assertEquals(byteArray.length, 2);
        Assert.assertEquals(byteArray[0], Byte.valueOf("1"));
        Assert.assertEquals(byteArray[1], Byte.valueOf("2"));

        value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ClickHouseColumn.of("a", "Array(Nullable(Int8))"),
                BinaryStreamUtilsTest.generateInput(2, 0, 1, 0, 2));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        Assert.assertEquals(value.asObject(), new Byte[] { (byte) 1, (byte) 2 });
        byteArray = value.asArray();
        Assert.assertEquals(byteArray.length, 2);
        Assert.assertEquals(byteArray[0], Byte.valueOf("1"));
        Assert.assertEquals(byteArray[1], Byte.valueOf("2"));

        value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ClickHouseColumn.of("a", "Array(Array(UInt8))"),
                BinaryStreamUtilsTest.generateInput(1, 2, 1, 2));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        Assert.assertEquals(value.asObject(), new short[][] { new short[] { 1, 2 } });
        Object[] array = (Object[]) value.asObject();
        Assert.assertEquals(array.length, 1);
        Assert.assertEquals(array[0], new short[] { 1, 2 });

        // SELECT arrayZip(['a', 'b', 'c'], [3, 2, 1])
        value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ClickHouseColumn.of("a", "Array(Tuple(String, UInt8))"),
                BinaryStreamUtilsTest.generateInput(3, 1, 0x61, 3, 1, 0x62, 2, 1, 0x63, 1));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        array = (Object[]) value.asObject();
        Assert.assertEquals(array.length, 3);
        Assert.assertEquals(((List<Object>) array[0]).size(), 2);
        Assert.assertEquals(((List<Object>) array[0]).get(0), "a");
        Assert.assertEquals(((List<Object>) array[0]).get(1), Short.valueOf("3"));
        Assert.assertEquals(((List<Object>) array[1]).size(), 2);
        Assert.assertEquals(((List<Object>) array[1]).get(0), "b");
        Assert.assertEquals(((List<Object>) array[1]).get(1), Short.valueOf("2"));
        Assert.assertEquals(((List<Object>) array[2]).size(), 2);
        Assert.assertEquals(((List<Object>) array[2]).get(0), "c");
        Assert.assertEquals(((List<Object>) array[2]).get(1), Short.valueOf("1"));

        // insert into x values([{ 'a' : (null, 3), 'b' : (1, 2), 'c' : (2, 1)}])
        value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
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
        Assert.assertEquals(l.get(1), 3);
        l = map.get("b");
        Assert.assertEquals(l.size(), 2);
        Assert.assertEquals(l.get(0), Short.valueOf("1"));
        Assert.assertEquals(l.get(1), 2);
        l = map.get("c");
        Assert.assertEquals(l.size(), 2);
        Assert.assertEquals(l.get(0), Short.valueOf("2"));
        Assert.assertEquals(l.get(1), 1);
    }

    @Test(groups = { "unit" })
    public void testSerializeArray() throws IOException {
        ClickHouseConfig config = new ClickHouseConfig();
        ClickHouseValue value = ClickHouseShortArrayValue.of(new short[] { 1, 2 });
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ClickHouseOutputStream out = ClickHouseOutputStream.of(bas);
        ClickHouseRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ClickHouseColumn.of("a", "Array(UInt8)"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(2, 1, 2));

        value = ClickHouseByteArrayValue.of(new byte[] { 1, 2 });
        bas = new ByteArrayOutputStream();
        out = ClickHouseOutputStream.of(bas);
        ClickHouseRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ClickHouseColumn.of("a", "Array(Nullable(Int8))"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(2, 0, 1, 0, 2));

        value = ClickHouseArrayValue.of(new short[][] { new short[] { 1, 2 } });
        bas = new ByteArrayOutputStream();
        out = ClickHouseOutputStream.of(bas);
        ClickHouseRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ClickHouseColumn.of("a", "Array(Array(UInt8))"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(1, 2, 1, 2));

        // SELECT arrayZip(['a', 'b', 'c'], [3, 2, 1])
        value = ClickHouseArrayValue
                .of(new Object[] { Arrays.asList("a", (short) 3), Arrays.asList("b", (short) 2),
                        Arrays.asList("c", (short) 1) });
        bas = new ByteArrayOutputStream();
        out = ClickHouseOutputStream.of(bas);
        ClickHouseRowBinaryProcessor.getMappedFunctions().serialize(value, config,
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
        ClickHouseRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ClickHouseColumn.of("a", "Array(Map(String, Tuple(Nullable(UInt8), UInt16)))"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(),
                BinaryStreamUtilsTest.generateBytes(1, 3, 1, 0x61, 1, 3, 0, 1, 0x62, 0, 1, 2, 0, 1,
                        0x63, 0, 2, 1, 0));
    }

    @Test(groups = { "unit" })
    public void testDeserializeMap() throws IOException {
        ClickHouseConfig config = new ClickHouseConfig();
        ClickHouseValue value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ClickHouseColumn.of("m", "Map(UInt8, UInt8)"),
                BinaryStreamUtilsTest.generateInput(2, 2, 2, 1, 1));
        Assert.assertTrue(value instanceof ClickHouseMapValue);
        Map<?, ?> map = (Map<?, ?>) value.asObject();
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(map.get((short) 2), (short) 2);
        Assert.assertEquals(map.get((short) 1), (short) 1);

        value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ClickHouseColumn.of("m", "Map(String, UInt32)"),
                BinaryStreamUtilsTest.generateInput(2, 1, 0x32, 2, 0, 0, 0, 1, 0x31, 1, 0, 0, 0));
        Assert.assertTrue(value instanceof ClickHouseMapValue);
        map = (Map<?, ?>) value.asObject();
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(map.get("2"), 2L);
        Assert.assertEquals(map.get("1"), 1L);
    }

    @Test(groups = { "unit" })
    public void testSerializeMap() throws IOException {
        ClickHouseConfig config = new ClickHouseConfig();
        ClickHouseValue value = ClickHouseMapValue.of(new LinkedHashMap<Short, Short>() {
            {
                put((short) 2, (short) 2);
                put((short) 1, (short) 1);
            }
        }, Short.class, Short.class);
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ClickHouseOutputStream out = ClickHouseOutputStream.of(bas);
        ClickHouseRowBinaryProcessor.getMappedFunctions().serialize(value, config,
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
        ClickHouseRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ClickHouseColumn.of("m", "Map(String, UInt32)"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(),
                BinaryStreamUtilsTest.generateBytes(2, 1, 0x32, 2, 0, 0, 0, 1, 0x31, 1, 0, 0, 0));
    }

    @Test(groups = { "unit" })
    public void testDeserializeNested() throws IOException {
        ClickHouseConfig config = new ClickHouseConfig();
        ClickHouseValue value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ClickHouseColumn.of("n", "Nested(n1 UInt8, n2 Nullable(String), n3 Int16)"),
                BinaryStreamUtilsTest.generateInput(1, 1, 1, 0, 1, 0x32, 1, 3, 0));
        Assert.assertTrue(value instanceof ClickHouseNestedValue);

        List<ClickHouseColumn> columns = ((ClickHouseNestedValue) value).getColumns();
        Object[][] values = (Object[][]) value.asObject();
        Assert.assertEquals(columns.size(), 3);
        Assert.assertEquals(columns.get(0).getColumnName(), "n1");
        Assert.assertEquals(columns.get(1).getColumnName(), "n2");
        Assert.assertEquals(columns.get(2).getColumnName(), "n3");
        Assert.assertEquals(values.length, 3);
        Assert.assertEquals(values[0], new Short[] { Short.valueOf("1") });
        Assert.assertEquals(values[1], new String[] { "2" });
        Assert.assertEquals(values[2], new Short[] { Short.valueOf("3") });
    }

    @Test(groups = { "unit" })
    public void testSerializeNested() throws IOException {
        ClickHouseConfig config = new ClickHouseConfig();
        ClickHouseValue value = ClickHouseNestedValue.of(
                ClickHouseColumn.of("n", "Nested(n1 UInt8, n2 Nullable(String), n3 Int16)")
                        .getNestedColumns(),
                new Object[][] { new Short[] { Short.valueOf("1") }, new String[] { "2" },
                        new Short[] { Short.valueOf("3") } });
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ClickHouseOutputStream out = ClickHouseOutputStream.of(bas);
        ClickHouseRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ClickHouseColumn.of("n", "Nested(n1 UInt8, n2 Nullable(String), n3 Int16)"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(),
                BinaryStreamUtilsTest.generateBytes(1, 1, 1, 0, 1, 0x32, 1, 3, 0));
    }

    @Test(groups = { "unit" })
    public void testDeserializeTuple() throws IOException {
        ClickHouseConfig config = new ClickHouseConfig();
        ClickHouseValue value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ClickHouseColumn.of("t", "Tuple(UInt8, String)"),
                BinaryStreamUtilsTest.generateInput(1, 1, 0x61));
        Assert.assertTrue(value instanceof ClickHouseTupleValue);
        List<Object> values = (List<Object>) value.asObject();
        Assert.assertEquals(values.size(), 2);
        Assert.assertEquals(values.get(0), (short) 1);
        Assert.assertEquals(values.get(1), "a");

        value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(value, config,
                ClickHouseColumn.of("t", "Tuple(UInt32, Int128, Nullable(IPv4)))"),
                BinaryStreamUtilsTest.generateInput(
                        1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x05,
                        0xa8, 0xc0));
        Assert.assertTrue(value instanceof ClickHouseTupleValue);
        values = (List<Object>) value.asObject();
        Assert.assertEquals(values.size(), 3);
        Assert.assertEquals(values.get(0), 1L);
        Assert.assertEquals(values.get(1), BigInteger.valueOf(2));
        Assert.assertEquals(values.get(2), InetAddress.getByName("192.168.5.1"));
    }

    @Test(groups = { "unit" })
    public void testSerializeTuple() throws IOException {
        ClickHouseConfig config = new ClickHouseConfig();
        ClickHouseValue value = ClickHouseTupleValue.of((byte) 1, "a");
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ClickHouseOutputStream out = ClickHouseOutputStream.of(bas);
        ClickHouseRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ClickHouseColumn.of("t", "Tuple(UInt8, String)"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(1, 1, 0x61));

        value = ClickHouseTupleValue.of(1L, BigInteger.valueOf(2), InetAddress.getByName("192.168.5.1"));
        bas = new ByteArrayOutputStream();
        out = ClickHouseOutputStream.of(bas);
        ClickHouseRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ClickHouseColumn.of("t", "Tuple(UInt32, Int128, Nullable(IPv4)))"), out);
        out.flush();
        Assert.assertEquals(bas.toByteArray(),
                BinaryStreamUtilsTest.generateBytes(1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x05, 0xa8, 0xc0));
    }
}
