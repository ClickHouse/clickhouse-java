package com.clickhouse.data;

import com.clickhouse.data.value.ClickHouseLongValue;
import com.clickhouse.data.value.ClickHouseStringValue;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClickHouseSimpleRecordTest {
    @Test(groups = { "unit" })
    public void testNullInput() {
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseSimpleRecord.of(null, null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseSimpleRecord.of(null, new ClickHouseValue[0]));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseSimpleRecord.of(Collections.emptyMap(), null));

        ClickHouseSimpleRecord record = new ClickHouseSimpleRecord(null, null);
        Assert.assertNull(record.getValues());
    }

    @Test(groups = { "unit" })
    public void testMismatchedColumnsAndValues() {

        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseSimpleRecord
                .of(Map.of("a", 0), new ClickHouseValue[0]));

        ClickHouseSimpleRecord record = new ClickHouseSimpleRecord(Map.of("a", 0),
                new ClickHouseValue[0]);
        Assert.assertEquals(record.getValues(), new ClickHouseValue[0]);
    }

    @Test(groups = { "unit" })
    public void testGetValueByIndex() {
        List<ClickHouseColumn> columnList = ClickHouseColumn.parse("a String, b UInt32");
        Map<String, Integer> columnIndex = IntStream.range(0, columnList.size()).boxed().collect(Collectors.toMap(i->columnList.get(i).getColumnName() , i -> i));

        ClickHouseSimpleRecord record = new ClickHouseSimpleRecord(columnIndex,
                new ClickHouseValue[] { ClickHouseStringValue.of("123"), ClickHouseLongValue.of(1L, true) });
        Assert.assertEquals(record.getValues(),
                new ClickHouseValue[] { ClickHouseStringValue.of("123"), ClickHouseLongValue.of(1L, true) });

        Assert.assertEquals(record.getValue(0), ClickHouseStringValue.of("123"));
        Assert.assertEquals(record.getValue(1), ClickHouseLongValue.of(1L, true));
        Assert.assertThrows(ArrayIndexOutOfBoundsException.class, () -> record.getValue(-1));
        Assert.assertThrows(ArrayIndexOutOfBoundsException.class, () -> record.getValue(2));

        int index = 0;
        for (ClickHouseValue v : record) {
            if (index == 0) {
                Assert.assertEquals(v, ClickHouseStringValue.of("123"));
            } else {
                Assert.assertEquals(v, ClickHouseLongValue.of(1L, true));
            }
            index++;
        }
    }

    @Test(groups = { "unit" })
    public void testGetValueByName() {
        List<ClickHouseColumn> columnList = ClickHouseColumn.parse("`a One` String, `x木哈哈x` UInt32, test Nullable(String)");
        Map<String, Integer> columnIndex = IntStream.range(0, columnList.size()).boxed().collect(Collectors.toMap(i->columnList.get(i).getColumnName() , i -> i));
        ClickHouseSimpleRecord record = new ClickHouseSimpleRecord(
                columnIndex,
                new ClickHouseValue[] { ClickHouseStringValue.of("123"), ClickHouseLongValue.of(1L, true),
                        ClickHouseStringValue.ofNull() });
        Assert.assertEquals(record.getValues(), new ClickHouseValue[] { ClickHouseStringValue.of("123"),
                ClickHouseLongValue.of(1L, true), ClickHouseStringValue.ofNull() });

        Assert.assertEquals(record.getValue("a One"), ClickHouseStringValue.of("123"));
        Assert.assertEquals(record.getValue("x木哈哈x"), ClickHouseLongValue.of(1L, true));
        Assert.assertEquals(record.getValue("test"), ClickHouseStringValue.ofNull());

        Assert.assertThrows(IllegalArgumentException.class, () -> record.getValue(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> record.getValue("non-exist"));
    }
}
