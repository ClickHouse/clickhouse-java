package com.clickhouse.data;

import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.clickhouse.data.value.ClickHouseArrayValue;
import com.clickhouse.data.value.ClickHouseLongValue;
import com.clickhouse.data.value.UnsignedLong;
import com.clickhouse.data.value.array.ClickHouseLongArrayValue;

public class ClickHouseColumnTest {
    @DataProvider(name = "enumTypesProvider")
    private Object[][] getEnumTypes() {
        return new Object[][] { { "Enum" }, { "Enum8" }, { "Enum16" } };
    }

    @DataProvider(name = "objectTypesProvider")
    private Object[][] getObjectTypes() {
        return new Object[][] {
                { "Tuple(not NChar Large Object)" },
                { "nchar Large Object" },
                { "Tuple(int Int32)" },
                { "a Tuple(i Int32)" },
                { "b Tuple(i1 Int32)" },
                { "Tuple(i Int32)" },
                { "Tuple(i1 Int32)" },
                { "Tuple(i Int32, a Array(Int32), m Map(LowCardinality(String), Int32))" },
                { "Int8" }, { "TINYINT SIGNED" },
                { "k1 Int8" }, { "k1 TINYINT SIGNED" },
                { "k1 Nullable(Int8)" }, { "k1 Nullable( Int8 )" }, { "k1 TINYINT SIGNED null" },
                { "k1 TINYINT SIGNED not null" },
                { "k1 LowCardinality(Nullable(String))" },
                { "k1 Tuple(k2 Int32, k3 Nullable(String), k4 TINYINT SIGNED not null, k5 Tuple (k6 UInt64))" }
        };
    }

    @Test(groups = { "unit" })
    public void testReadColumn() {
        String args = "AggregateFunction(max, UInt64), cc LowCardinality(Nullable(String)), a UInt8  null";
        List<ClickHouseColumn> list = new LinkedList<>();
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.indexOf("cc") - 2);
        Assert.assertEquals(list.size(), 1);
        Assert.assertFalse(list.get(0).isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(list.get(0).getEstimatedLength(), 1);
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
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
        list.clear();

        args = "INT1 unsigned not null, b DateTime64(3) NULL";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.indexOf(','));
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertFalse(column.isNullable());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.UInt8);
        Assert.assertTrue(column.isFixedLength(), "Should have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
        list.clear();

        Assert.assertEquals(ClickHouseColumn.readColumn(args, args.indexOf('D'), args.length(), null, list),
                args.length() - 1);
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertTrue(column.isNullable());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.DateTime64);
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
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
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
        list.clear();

        args = "Array(FixedString(2))";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.length());
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getNestedColumns().size(), 1);
        Assert.assertTrue(column.getArrayBaseColumn() == column.getNestedColumns().get(0),
                "Nested column should be same as base column of the array");
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
        column = column.getNestedColumns().get(0);
        Assert.assertTrue(column.isFixedLength(), "FixedString should have fixed length in byte");
        Assert.assertEquals(column.getNestedColumns().size(), 0);
        Assert.assertEquals(column.getEstimatedLength(), 2);
        list.clear();

        args = "Array(String)";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.length());
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getNestedColumns().size(), 1);
        Assert.assertTrue(column.getArrayBaseColumn() == column.getNestedColumns().get(0),
                "Nested column should be same as base column of the array");
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
        column = column.getNestedColumns().get(0);
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getNestedColumns().size(), 0);
        Assert.assertEquals(column.getEstimatedLength(), 1);
        list.clear();

        args = " Tuple(Nullable(FixedString(3)), Array(UInt8),String not null) ";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 1, args.length(), null, list), args.length() - 2);
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getOriginalTypeName(), args.trim());
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 3);
        list.clear();

        args = "Map(UInt8 , UInt8)";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.length());
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getOriginalTypeName(), args);
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
        list.clear();

        args = "Map(String, FixedString(233))";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.length());
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getOriginalTypeName(), args);
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
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
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
        list.clear();

        args = "Nested(\na Array(Nullable(UInt8)), `b b` LowCardinality(Nullable(DateTime64(3))))";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.length());
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getOriginalTypeName(), args);
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
    }

    @Test(groups = { "unit" })
    public void testParse() {
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
    public void testAggregationFunction() {
        ClickHouseColumn column = ClickHouseColumn.of("aggFunc", "AggregateFunction(groupBitmap, UInt32)");
        Assert.assertTrue(column.isAggregateFunction());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.AggregateFunction);
        Assert.assertEquals(column.getAggregateFunction(), ClickHouseAggregateFunction.groupBitmap);
        Assert.assertEquals(column.getFunction(), "groupBitmap");
        Assert.assertEquals(column.getNestedColumns(), Collections.singletonList(ClickHouseColumn.of("", "UInt32")));
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);

        column = ClickHouseColumn.of("aggFunc", "AggregateFunction(quantiles(0.5, 0.9), Nullable(UInt64))");
        Assert.assertTrue(column.isAggregateFunction());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.AggregateFunction);
        Assert.assertEquals(column.getAggregateFunction(), ClickHouseAggregateFunction.quantiles);
        Assert.assertEquals(column.getFunction(), "quantiles(0.5,0.9)");
        Assert.assertEquals(column.getNestedColumns(),
                Collections.singletonList(ClickHouseColumn.of("", "Nullable(UInt64)")));
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
    }

    @Test(groups = { "unit" })
    public void testArray() {
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
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);

        ClickHouseColumn c = ClickHouseColumn.of("arr", "Array(LowCardinality(Nullable(String)))");
        Assert.assertTrue(c.isArray());
        Assert.assertEquals(c.getDataType(), ClickHouseDataType.Array);
        Assert.assertEquals(c.getArrayNestedLevel(), 1);
        Assert.assertEquals(c.getArrayBaseColumn().getOriginalTypeName(), "LowCardinality(Nullable(String))");
        Assert.assertFalse(c.getArrayBaseColumn().isArray());
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
    }

    @Test(dataProvider = "enumTypesProvider", groups = { "unit" })
    public void testEnum(String typeName) {
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
        if (column.getDataType() != ClickHouseDataType.Enum) { // virtual type
            Assert.assertTrue(column.isFixedLength(), "Should have fixed length in byte");
        }
        Assert.assertEquals(column.getEstimatedLength(), column.getDataType().getByteLength());
    }

    @Test(dataProvider = "objectTypesProvider", groups = { "unit" })
    public void testObjectType(String typeName) {
        ClickHouseColumn.of("o", "Tuple(TINYINT SIGNED null)");
        for (String prefix : new String[] { "Tuple(", "Tuple (", "Tuple ( " }) {
            for (String suffix : new String[] { ")", " )", " ) " }) {
                ClickHouseColumn innerColumn = ClickHouseColumn.of("",
                        typeName + suffix.substring(0, suffix.lastIndexOf(')')));
                ClickHouseColumn column = ClickHouseColumn.of("o", prefix + typeName + suffix);
                Assert.assertTrue(column.isTuple());
                Assert.assertEquals(column.getNestedColumns().get(0), innerColumn);
            }
        }
    }

    @Test(groups = { "unit" })
    public void testSimpleAggregationFunction() {
        ClickHouseColumn c = ClickHouseColumn.of("a", "SimpleAggregateFunction(max, UInt64)");
        Assert.assertEquals(c.getDataType(), ClickHouseDataType.SimpleAggregateFunction);
        Assert.assertEquals(c.getNestedColumns().get(0).getDataType(), ClickHouseDataType.UInt64);

        // https://github.com/ClickHouse/clickhouse-java/issues/1389
        c = ClickHouseColumn.of("a", "SimpleAggregateFunction(anyLast, Nested(a String, b String))");
        Assert.assertEquals(c.getDataType(), ClickHouseDataType.SimpleAggregateFunction);
        Assert.assertEquals(c.getNestedColumns().get(0).getDataType(), ClickHouseDataType.Nested);
        Assert.assertEquals(c.getNestedColumns().get(0).getNestedColumns(),
                ClickHouseColumn.parse("a String, b String"));
        Assert.assertEquals(
                ClickHouseColumn.of("a", "SimpleAggregateFunction ( anyLast ,  Nested ( a String , b String ) )")
                        .getParameters(),
                c.getParameters());
        Assert.assertEquals(
                ClickHouseColumn.of("a",
                        "SimpleAggregateFunction(anyLast,Nested(a String,b String,`c c` Nested(d Int32, e Tuple(UInt32, Nullable(String)))))")
                        .getParameters(),
                ClickHouseColumn.of("a",
                        " SimpleAggregateFunction ( /** test **/anyLast -- test\n ,  Nested ( a String , b String,\n\t `c c` \t Nested(d Int32, e Tuple(UInt32, Nullable(String))) ) )")
                        .getParameters());
    }

    @Test(groups = { "unit" })
    public void testGetObjectClassForArray() {
        ClickHouseDataConfig defaultConfig = new ClickHouseTestDataConfig();
        ClickHouseDataConfig widenUnsignedConfig = new ClickHouseTestDataConfig() {
            @Override
            public boolean isWidenUnsignedTypes() {
                return true;
            };
        };
        ClickHouseDataConfig binStringConfig = new ClickHouseTestDataConfig() {
            @Override
            public boolean isUseBinaryString() {
                return true;
            };
        };
        ClickHouseDataConfig objArrayConfig = new ClickHouseTestDataConfig() {
            @Override
            public boolean isUseObjectsInArray() {
                return true;
            };
        };

        Assert.assertEquals(ClickHouseColumn.of("a", "UInt64").getObjectClassForArray(defaultConfig), long.class);
        Assert.assertEquals(
                ClickHouseColumn.of("a", "Array(UInt64)").getArrayBaseColumn().getObjectClassForArray(defaultConfig),
                long.class);
        Assert.assertEquals(ClickHouseColumn.of("a", "UInt64").getObjectClassForArray(widenUnsignedConfig), long.class);
        Assert.assertEquals(ClickHouseColumn.of("a", "Array(UInt64)").getArrayBaseColumn()
                .getObjectClassForArray(widenUnsignedConfig), long.class);
        Assert.assertEquals(ClickHouseColumn.of("a", "UInt64").getObjectClassForArray(objArrayConfig),
                UnsignedLong.class);
        Assert.assertEquals(
                ClickHouseColumn.of("a", "Array(UInt64)").getArrayBaseColumn().getObjectClassForArray(objArrayConfig),
                UnsignedLong.class);

        Assert.assertEquals(ClickHouseColumn.of("a", "FixedString(2)").getObjectClassForArray(defaultConfig),
                String.class);
        Assert.assertEquals(ClickHouseColumn.of("a", "Array(FixedString(2))").getArrayBaseColumn()
                .getObjectClassForArray(defaultConfig), String.class);
        Assert.assertEquals(ClickHouseColumn.of("a", "FixedString(2)").getObjectClassForArray(binStringConfig),
                Object.class);
        Assert.assertEquals(ClickHouseColumn.of("a", "Array(FixedString(2))").getArrayBaseColumn()
                .getObjectClassForArray(binStringConfig), Object.class);
        Assert.assertEquals(ClickHouseColumn.of("a", "String").getObjectClassForArray(defaultConfig),
                String.class);
        Assert.assertEquals(ClickHouseColumn.of("a", "Array(String)").getArrayBaseColumn()
                .getObjectClassForArray(defaultConfig), String.class);
        Assert.assertEquals(ClickHouseColumn.of("a", "String").getObjectClassForArray(binStringConfig),
                Object.class);
        Assert.assertEquals(ClickHouseColumn.of("a", "Array(String)").getArrayBaseColumn()
                .getObjectClassForArray(binStringConfig), Object.class);
    }

    @Test(groups = { "unit" })
    public void testNewArray() {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig() {
            @Override
            public boolean isWidenUnsignedTypes() {
                return true;
            };
        };
        ClickHouseValue v = ClickHouseColumn.of("a", "Array(UInt32)").newValue(config);
        Assert.assertEquals(v.update(new long[] { 1L }).asObject(), new long[] { 1L });
        v = ClickHouseColumn.of("a", "Array(Nullable(UInt64))").newValue(config);
        Assert.assertEquals(v.update(new Long[] { 1L }).asObject(), new Long[] { 1L });
        v = ClickHouseColumn.of("a", "Array(Array(UInt16))").newValue(config);
        Assert.assertEquals(v.asObject(), new int[0][]);
        v = ClickHouseColumn.of("a", "Array(UInt64)").newValue(config);
        Assert.assertEquals(v.asObject(), new UnsignedLong[0]);
        Assert.assertEquals(((ClickHouseLongArrayValue) v).allocate(1)
                .setValue(0, ClickHouseLongValue.of(1L)).asObject(), new long[] { 1L });
        Assert.assertEquals(((ClickHouseLongArrayValue) v).allocate(1)
                .setValue(0, ClickHouseLongValue.ofUnsigned(1L)).asObject(), new long[] { 1L });
        v = ClickHouseColumn.of("a", "Array(Array(UInt64))").newValue(config);
        Assert.assertEquals(v.asObject(), new long[0][]);
        Assert.assertEquals(((ClickHouseArrayValue<?>) v).allocate(1)
                .setValue(0, ClickHouseLongArrayValue.of(new long[] { 1L })).asObject(), new long[][] { { 1L } });
        v = ClickHouseColumn.of("a", "Array(Array(Array(UInt8)))").newValue(config);
        Assert.assertEquals(v.asObject(), new short[0][][]);
        v = ClickHouseColumn.of("a", "Array(Array(Array(Nullable(UInt8))))").newValue(config);
        Assert.assertEquals(v.update(new Short[][][] { new Short[][] { new Short[] { (short) 1 } } }).asObject(),
                new Short[][][] { new Short[][] { new Short[] { (short) 1 } } });
        v = ClickHouseColumn.of("a", "Array(Array(Array(Array(LowCardinality(String)))))").newValue(config);
        Assert.assertEquals(v.asObject(), new String[0][][][]);

        config = new ClickHouseTestDataConfig() {
            @Override
            public boolean isWidenUnsignedTypes() {
                return false;
            };
        };
        v = ClickHouseColumn.of("", "Array(UInt8)").newValue(config);
        Assert.assertEquals(v.update(new byte[] { Byte.MIN_VALUE, 0, Byte.MAX_VALUE }).asObject(),
                new byte[] { Byte.MIN_VALUE, 0, Byte.MAX_VALUE });
        Assert.assertEquals(v.update(new int[] { -1, 0, 1 }).asObject(), new byte[] { -1, 0, 1 });
        v = ClickHouseColumn.of("", "Array(UInt16)").newValue(config);
        Assert.assertEquals(v.update(new byte[] { -1, 0, 1 }).asObject(), new short[] { 255, 0, 1 });
        Assert.assertEquals(v.update(new int[] { -1, 0, 1 }).asObject(), new short[] { -1, 0, 1 });
        v = ClickHouseColumn.of("", "Array(UInt32)").newValue(config);
        Assert.assertEquals(v.update(new short[] { -1, 0, 1 }).asObject(), new int[] { 65535, 0, 1 });
        Assert.assertEquals(v.update(new int[] { -1, 0, 1 }).asObject(), new int[] { -1, 0, 1 });
        v = ClickHouseColumn.of("", "Array(UInt64)").newValue(config);
        Assert.assertEquals(v.update(new int[] { -1, 0, 1 }).asObject(), new long[] { 4294967295L, 0, 1 });
        Assert.assertEquals(v.update(new long[] { -1L, 0L, 1L }).asObject(), new long[] { -1L, 0L, 1L });
        Assert.assertEquals(
                v.update(new BigInteger[] { new BigInteger("18446744073709551615"), BigInteger.ZERO, BigInteger.ONE })
                        .asObject(),
                new long[] { -1L, 0L, 1L });
    }

    @Test(groups = { "unit" })
    public void testNewBasicValues() {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig() {
            @Override
            public boolean isWidenUnsignedTypes() {
                return true;
            };
        };
        for (ClickHouseDataType type : ClickHouseDataType.values()) {
            // skip advanced types
            if (type.isNested() || type == ClickHouseDataType.AggregateFunction
                    || type == ClickHouseDataType.SimpleAggregateFunction || type == ClickHouseDataType.Enum
                    || type == ClickHouseDataType.Nullable || type == ClickHouseDataType.BFloat16) {
                continue;
            }

            ClickHouseValue value = ClickHouseColumn.of("", type, false).newValue(config);
            Assert.assertNotNull(value);

            if (type == ClickHouseDataType.Point) {
                Assert.assertEquals(value.asObject(), new double[] { 0D, 0D });
            } else if (type == ClickHouseDataType.Ring) {
                Assert.assertEquals(value.asObject(), new double[0][]);
            } else if (type == ClickHouseDataType.Polygon) {
                Assert.assertEquals(value.asObject(), new double[0][][]);
            } else if (type == ClickHouseDataType.MultiPolygon) {
                Assert.assertEquals(value.asObject(), new double[0][][][]);
            } else {
                Assert.assertNull(value.asObject());
            }
        }
    }
}
