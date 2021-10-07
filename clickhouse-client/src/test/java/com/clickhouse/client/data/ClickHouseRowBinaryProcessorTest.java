package com.clickhouse.client.data;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseValue;

public class ClickHouseRowBinaryProcessorTest {
    private ClickHouseRowBinaryProcessor newProcessor(int... bytes) throws IOException {
        return new ClickHouseRowBinaryProcessor(new ClickHouseConfig(), BinaryStreamUtilsTest.generateInput(bytes),
                null, Collections.emptyList(), null);
    }

    @Test(groups = { "unit" })
    public void testDeserializeArray() throws IOException {

        ClickHouseValue value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(
                ClickHouseColumn.of("a", "Array(UInt8)"), null, BinaryStreamUtilsTest.generateInput(2, 1, 2));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        Short[] shortArray = value.asObject(Short[].class);
        Assert.assertEquals(shortArray.length, 2);
        Assert.assertEquals(shortArray[0], Short.valueOf("1"));
        Assert.assertEquals(shortArray[1], Short.valueOf("2"));

        value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(
                ClickHouseColumn.of("a", "Array(Nullable(Int8))"), null,
                BinaryStreamUtilsTest.generateInput(2, 0, 1, 0, 2));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        Byte[] byteArray = value.asObject(Byte[].class);
        Assert.assertEquals(byteArray.length, 2);
        Assert.assertEquals(byteArray[0], Byte.valueOf("1"));
        Assert.assertEquals(byteArray[1], Byte.valueOf("2"));

        value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(
                ClickHouseColumn.of("a", "Array(Array(UInt8))"), null, BinaryStreamUtilsTest.generateInput(1, 2, 1, 2));
        Assert.assertTrue(value instanceof ClickHouseArrayValue);
        Object[] array = (Object[]) value.asObject();
        Assert.assertEquals(array.length, 1);
        shortArray = (Short[]) array[0];
        Assert.assertEquals(shortArray.length, 2);
        Assert.assertEquals(shortArray[0], Short.valueOf("1"));
        Assert.assertEquals(shortArray[1], Short.valueOf("2"));

        // SELECT arrayZip(['a', 'b', 'c'], [3, 2, 1])
        value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(
                ClickHouseColumn.of("a", "Array(Tuple(String, UInt8))"), null,
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
        value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(
                ClickHouseColumn.of("a", "Array(Map(String, Tuple(Nullable(UInt8), UInt16)))"), null,
                BinaryStreamUtilsTest.generateInput(1, 3, 1, 0x61, 1, 3, 0, 1, 0x62, 0, 1, 2, 0, 1, 0x63, 0, 2, 1, 0));
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
    public void testDeserializeMap() throws IOException {
        ClickHouseValue value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(
                ClickHouseColumn.of("m", "Map(UInt8, UInt8)"), null,
                BinaryStreamUtilsTest.generateInput(2, 2, 2, 1, 1));
        Assert.assertTrue(value instanceof ClickHouseMapValue);
        Map<?, ?> map = (Map<?, ?>) value.asObject();
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(map.get((short) 2), (short) 2);
        Assert.assertEquals(map.get((short) 1), (short) 1);

        value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(
                ClickHouseColumn.of("m", "Map(String, UInt32)"), null,
                BinaryStreamUtilsTest.generateInput(2, 1, 0x32, 2, 0, 0, 0, 1, 0x31, 1, 0, 0, 0, 0));
        Assert.assertTrue(value instanceof ClickHouseMapValue);
        map = (Map<?, ?>) value.asObject();
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(map.get("2"), 2L);
        Assert.assertEquals(map.get("1"), 1L);
    }

    @Test(groups = { "unit" })
    public void testDeserializeNested() throws IOException {
        ClickHouseValue value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(
                ClickHouseColumn.of("n", "Nested(n1 UInt8, n2 Nullable(String), n3 Int16)"), null,
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
    public void testDeserializeTuple() throws IOException {
        ClickHouseValue value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(
                ClickHouseColumn.of("t", "Tuple(UInt8, String)"), null,
                BinaryStreamUtilsTest.generateInput(1, 1, 0x61));
        Assert.assertTrue(value instanceof ClickHouseTupleValue);
        List<Object> values = (List<Object>) value.asObject();
        Assert.assertEquals(values.size(), 2);
        Assert.assertEquals(values.get(0), (short) 1);
        Assert.assertEquals(values.get(1), "a");

        value = ClickHouseRowBinaryProcessor.getMappedFunctions().deserialize(
                ClickHouseColumn.of("t", "Tuple(UInt32, Int128, Nullable(IPv4)))"), value,
                BinaryStreamUtilsTest.generateInput(1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
                        0x05, 0xa8, 0xc0));
        Assert.assertTrue(value instanceof ClickHouseTupleValue);
        values = (List<Object>) value.asObject();
        Assert.assertEquals(values.size(), 3);
        Assert.assertEquals(values.get(0), 1L);
        Assert.assertEquals(values.get(1), BigInteger.valueOf(2));
        Assert.assertEquals(values.get(2), InetAddress.getByName("192.168.5.1"));
    }
}
