package com.clickhouse.client;

import java.util.LinkedList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseColumnTest {

    @Test(groups = { "unit" })
    public void testReadColumn() {
        String args = "AggregateFunction(max, UInt64), cc LowCardinality(Nullable(String)), a UInt8  null";
        List<ClickHouseColumn> list = new LinkedList<>();
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.indexOf("cc") - 2);
        Assert.assertEquals(list.size(), 1);
        list.clear();
        Assert.assertEquals(ClickHouseColumn.readColumn(args, args.indexOf("cc") + 3, args.length(), null, list),
                args.lastIndexOf(','));
        list.clear();
        Assert.assertEquals(ClickHouseColumn.readColumn(args, args.lastIndexOf('U'), args.length(), null, list),
                args.length() - 1);
        Assert.assertEquals(list.size(), 1);
        ClickHouseColumn column = list.get(0);
        Assert.assertNotNull(column);
        Assert.assertFalse(column.isLowCardinality());
        Assert.assertTrue(column.isNullable());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.UInt8);
        list.clear();

        args = "INT1 unsigned not null, b DateTime64(3) NULL";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.indexOf(','));
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertFalse(column.isNullable());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.UInt8);
        list.clear();

        Assert.assertEquals(ClickHouseColumn.readColumn(args, args.indexOf('D'), args.length(), null, list),
                args.length() - 1);
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertTrue(column.isNullable());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.DateTime64);
    }

    @Test(groups = { "unit" })
    public void testReadNestedColumn() {
        String args = "Array(Array(Nullable(UInt8)))";
        List<ClickHouseColumn> list = new LinkedList<>();
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.length());
        Assert.assertEquals(list.size(), 1);
        ClickHouseColumn column = list.get(0);
        Assert.assertEquals(column.getNestedColumns().size(), 1);
        Assert.assertEquals(column.getNestedColumns().get(0).getNestedColumns().size(), 1);
        list.clear();

        args = " Tuple(Nullable(FixedString(3)), Array(UInt8),String not null) ";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 1, args.length(), null, list), args.length() - 2);
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getOriginalTypeName(), args.trim());
        list.clear();

        args = "Map(UInt8 , UInt8)";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.length());
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getOriginalTypeName(), args);
        list.clear();

        args = "Map(String, FixedString(233))";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.length());
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getOriginalTypeName(), args);
        list.clear();

        args = "Map(String, Tuple(UInt8, Nullable(String), UInt16 null))";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.length());
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getOriginalTypeName(), args);
        Assert.assertEquals(column.getNestedColumns().size(), 2);
        Assert.assertEquals(column.getKeyInfo().getOriginalTypeName(), "String");
        Assert.assertEquals(column.getValueInfo().getOriginalTypeName(), "Tuple(UInt8, Nullable(String), UInt16 null)");
        Assert.assertEquals(column.getValueInfo().getNestedColumns().size(), 3);
        list.clear();

        args = "Nested(\na Array(Nullable(UInt8)), `b b` LowCardinality(Nullable(DateTime64(3))))";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.length());
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getOriginalTypeName(), args);
    }

    @Test(groups = { "unit" })
    public void testParse() throws Exception {
        ClickHouseColumn column = ClickHouseColumn.of("arr", "Nullable(Array(Nullable(UInt8))");
        Assert.assertNotNull(column);

        List<ClickHouseColumn> list = ClickHouseColumn.parse("a String not null, b String null");
        Assert.assertEquals(list.size(), 2);
        list = ClickHouseColumn.parse("a String not null, b Int8");
        Assert.assertEquals(list.size(), 2);
        list = ClickHouseColumn.parse("a String, b String null");
        Assert.assertEquals(list.size(), 2);
        list = ClickHouseColumn.parse("a String default 'cc', b String null");
        Assert.assertEquals(list.size(), 2);
    }
}
