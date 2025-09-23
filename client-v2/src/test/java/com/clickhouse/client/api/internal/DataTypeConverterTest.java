package com.clickhouse.client.api.internal;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.testng.Assert.assertEquals;

@Test(groups = {"unit"})
public class DataTypeConverterTest {

    @Test
    public void testArrayToString() {
        DataTypeConverter converter = new DataTypeConverter();

        ClickHouseColumn intColumn = ClickHouseColumn.of("v", "Array(Int32)");
        assertEquals(converter.arrayToString(new byte[]{1, 2, 3}, intColumn), "[1, 2, 3]");
        assertEquals(converter.arrayToString(new short[]{1, 2, 3}, intColumn), "[1, 2, 3]");
        assertEquals(converter.arrayToString(new int[]{1, 2, 3}, intColumn), "[1, 2, 3]");
        assertEquals(converter.arrayToString(new long[]{1L, 2L, 3L}, intColumn), "[1, 2, 3]");
        assertEquals(converter.arrayToString(new float[]{1.0f, 2.0f, 3.0f}, intColumn), "[1.0, 2.0, 3.0]");
        assertEquals(converter.arrayToString(new double[]{1.0d, 2.0d, 3.0d}, intColumn), "[1.0, 2.0, 3.0]");
        assertEquals(converter.arrayToString(new boolean[]{true, false, true}, intColumn), "[true, false, true]");


        ClickHouseColumn strColumn = ClickHouseColumn.of("v", "Array(String)");
        assertEquals(converter.arrayToString(new String[][]{{"a", null}, {"b", "c"}}, strColumn), "[['a', NULL], ['b', 'c']]");
        assertEquals(converter.arrayToString(new int[][]{{1, 2}, {3, 4}}, intColumn), "[[1, 2], [3, 4]]");
        assertEquals(converter.arrayToString(new int[][][]{{{1, 2}, {3, 4}}, {{5, 6}}}, intColumn), "[[[1, 2], [3, 4]], [[5, 6]]]");
        assertEquals(converter.arrayToString(new char[]{'a', 'b', 'c'}, strColumn), "['a', 'b', 'c']");
    }

    @Test
    public void testListToString() {
        DataTypeConverter converter = new DataTypeConverter();
        ClickHouseColumn column = ClickHouseColumn.of("field", "Array(Int32)");
        assertEquals(converter.arrayToString(Collections.emptyList(), column), "[]");
        assertEquals(converter.arrayToString(Arrays.asList(1, 2, 3), column), "[1, 2, 3]");
        assertEquals(converter.arrayToString(Arrays.asList(1, null, 3), column), "[1, NULL, 3]");
        assertEquals(converter.arrayToString(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4)), column), "[[1, 2], [3, 4]]");
        assertEquals(converter.arrayToString(Arrays.asList(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4)), Arrays.asList(Arrays.asList(5, 6))), column), "[[[1, 2], [3, 4]], [[5, 6]]]");
        assertEquals(converter.arrayToString(Arrays.asList(null, null, null), column), "[NULL, NULL, NULL]");
    }
}