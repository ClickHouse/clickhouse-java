package com.clickhouse.client;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHouseColumnTest {
    @DataProvider(name = "enumTypesProvider")
    private Object[][] getEnumTypes() {
        return new Object[][] { { "Enum" }, { "Enum8" }, { "Enum16" } };
    }

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

    @Test(groups = { "unit" })
    public void testAggregationFunction() throws Exception {
        ClickHouseColumn column = ClickHouseColumn.of("aggFunc", "AggregateFunction(groupBitmap, UInt32)");
        Assert.assertTrue(column.isAggregateFunction());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.AggregateFunction);
        Assert.assertEquals(column.getAggregateFunction(), ClickHouseAggregateFunction.groupBitmap);
        Assert.assertEquals(column.getFunction(), "groupBitmap");
        Assert.assertEquals(column.getNestedColumns(), Collections.singletonList(ClickHouseColumn.of("", "UInt32")));

        column = ClickHouseColumn.of("aggFunc", "AggregateFunction(quantiles(0.5, 0.9), Nullable(UInt64))");
        Assert.assertTrue(column.isAggregateFunction());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.AggregateFunction);
        Assert.assertEquals(column.getAggregateFunction(), ClickHouseAggregateFunction.quantiles);
        Assert.assertEquals(column.getFunction(), "quantiles(0.5,0.9)");
        Assert.assertEquals(column.getNestedColumns(),
                Collections.singletonList(ClickHouseColumn.of("", "Nullable(UInt64)")));
    }

    @Test(groups = { "unit" })
    public void testArray() throws Exception {
        ClickHouseColumn column = ClickHouseColumn.of("arr",
                "Array(Array(Array(Array(Array(Map(LowCardinality(String), Tuple(Array(UInt8),LowCardinality(String))))))))");
        Assert.assertTrue(column.isArray());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.Array);
        Assert.assertEquals(column.getArrayNestedLevel(), 5);
        Assert.assertEquals(column.getArrayBaseColumn().getOriginalTypeName(),
                "Map(LowCardinality(String), Tuple(Array(UInt8),LowCardinality(String)))");
        Assert.assertFalse(column.getArrayBaseColumn().isArray());

        Assert.assertEquals(column.getArrayBaseColumn().getArrayNestedLevel(), 0);
        Assert.assertEquals(column.getArrayBaseColumn().getArrayBaseColumn(), null);

        ClickHouseColumn c = ClickHouseColumn.of("arr", "Array(LowCardinality(Nullable(String)))");
        Assert.assertTrue(c.isArray());
        Assert.assertEquals(c.getDataType(), ClickHouseDataType.Array);
        Assert.assertEquals(c.getArrayNestedLevel(), 1);
        Assert.assertEquals(c.getArrayBaseColumn().getOriginalTypeName(), "LowCardinality(Nullable(String))");
        Assert.assertFalse(c.getArrayBaseColumn().isArray());
    }

    @Test(dataProvider = "enumTypesProvider", groups = { "unit" })
    public void testEnum(String typeName) throws Exception {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseColumn.of("e", typeName + "('Query''Start' = a)"));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseColumn.of("e", typeName + "(aa,1)"));
        ClickHouseColumn column = ClickHouseColumn.of("e", typeName + "('Query''Start' = 1, 'Query\\'Finish' = 10)");
        Assert.assertTrue(column.isEnum());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.of(typeName));
        Assert.assertThrows(IllegalArgumentException.class, () -> column.getEnumConstants().name(2));
        Assert.assertThrows(IllegalArgumentException.class, () -> column.getEnumConstants().value(""));
        Assert.assertEquals(column.getEnumConstants().name(1), "Query'Start");
        Assert.assertEquals(column.getEnumConstants().name(10), "Query'Finish");
        Assert.assertEquals(column.getEnumConstants().value("Query'Start"), 1);
        Assert.assertEquals(column.getEnumConstants().value("Query'Finish"), 10);
    }
}
