package com.clickhouse.client.testdata;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

public class GenerateTestDataSetTest {

    @DataProvider
    public Object[][] types() {
        return new Object[][] {
                {"Int8", "Int8"},
                {" UInt256 ", "UInt256"},
                {"Decimal(18)", "Decimal(18, 0)"},
                {"Decimal(18,4)", "Decimal(18, 4)"},
                {"Decimal64(3)", "Decimal64(3)"},
                {"BinaryString", "String"},
                {"FixedString(8)", "FixedString(8)"},
                {"Time64(6)", "Time64(6)"},
                {"DateTime", "DateTime('UTC')"},
                {"DateTime('Europe/Berlin')", "DateTime('Europe/Berlin')"},
                {"DateTime64(3)", "DateTime64(3, 'UTC')"},
                {"DateTime64(9, 'Asia/Kolkata')", "DateTime64(9, 'Asia/Kolkata')"},
                {"Enum8('a' = 1, 'b,\\'c' = -2)", "Enum8('a' = 1, 'b,\\'c' = -2)"},
                {"Nullable(Nothing)", "Nullable(Nothing)"},
                {"IntervalDay", "IntervalDay"},
                {"Array(Nullable(String))", "Array(Nullable(String))"},
                {"Tuple(Int32, Map(String, Array(UInt8)))", "Tuple(Int32, Map(String, Array(UInt8)))"},
                {"Tuple(a Int32, b DateTime64(3, 'UTC'))", "Tuple(`a` Int32, `b` DateTime64(3, 'UTC'))"},
                {"Tuple(`a b` Int32, `c\\`` String)", "Tuple(`a b` Int32, `c\\`` String)"},
                {"Nested(id UInt32, name LowCardinality(String))", "Nested(`id` UInt32, `name` LowCardinality(String))"},
                {"MultiPolygon", "MultiPolygon"},
        };
    }

    @Test(dataProvider = "types")
    public void testParseType(String expression, String expectedType) {
        Assert.assertEquals(GenerateTestDataSet.parseType(expression).getType(), expectedType);
    }

    @DataProvider
    public Object[][] invalidTypes() {
        return new Object[][] {{"Foo"}, {"Int8(1)"}, {"Array()"}, {"Array(Int8"}, {"Map(String)"},
                {"Decimal(x)"}, {"DateTime(UTC)"}, {"Tuple(a Int8, Int8)"}, {"Enum8('a')"}, {"IntervalCentury"}};
    }

    @Test(dataProvider = "invalidTypes", expectedExceptions = IllegalArgumentException.class)
    public void testParseInvalidType(String expression) {
        GenerateTestDataSet.parseType(expression);
    }

    @Test
    public void testWriteSql() {
        TestDataSet dataSet = TestDataSet.builder()
                .column("n", GenerateTestDataSet.parseType("Nested(x Int8)"))
                .build();
        StringWriter sql = new StringWriter();
        GenerateTestDataSet.write(dataSet, "sql", "t", new PrintWriter(sql));
        String[] lines = sql.toString().split("\n", 4);
        Assert.assertTrue(lines[0].startsWith("-- TestDataSet(n Nested(`x` Int8))"), lines[0]);
        Assert.assertEquals(lines[1], "SET flatten_nested = 0;");
        Assert.assertEquals(lines[2], dataSet.getCreateTableSql("t") + ";");
        Assert.assertTrue(lines[3].startsWith("INSERT INTO t (id, `n`) VALUES (0, []),"), lines[3]);
    }

    @Test
    public void testWriteTsv() {
        TestDataSet dataSet = TestDataSet.of(GenerateTestDataSet.parseType("Nullable(String)"));
        StringWriter tsv = new StringWriter();
        GenerateTestDataSet.write(dataSet, "tsv", "t", new PrintWriter(tsv));
        Assert.assertEquals(tsv.toString(), dataSet.getTsv());
        Assert.assertTrue(tsv.toString().startsWith("0\t\\N\n1\t\n2\ta\n"), tsv.toString());
    }
}
