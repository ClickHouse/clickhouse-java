package com.clickhouse.client.testdata;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
                {"Enum('a' = 1, 'b' = -2)", "Enum('a' = 1, 'b' = -2)"},
                {"Enum8('a' = 1, 'b,\\'c' = -2)", "Enum8('a' = 1, 'b,\\'c' = -2)"},
                {"Enum8Single", "Enum8('only' = 42)"},
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

    @Test(groups = {"unit"})
    public void testReadDataSetIndex() {
        List<GenerateTestDataSet.DataSetEntry> entries = GenerateTestDataSet.readDataSetIndex();
        List<String> storableTypes = GenerateTestDataSet.getStorableTypes();
        List<String> valuesOnlyTypes = GenerateTestDataSet.getValuesOnlyTypes();

        Assert.assertFalse(entries.isEmpty());
        Assert.assertEquals(entries.size(), storableTypes.size() + valuesOnlyTypes.size());

        Set<String> names = new HashSet<>();
        for (GenerateTestDataSet.DataSetEntry entry : entries) {
            Assert.assertTrue(names.add(entry.getName()), "Duplicate dataset name: " + entry.getName());
            Assert.assertEquals(entry.getName(), GenerateTestDataSet.datasetName(entry.getType()));
            DataTypeGenerator generator = GenerateTestDataSet.parseType(entry.getType());
            Assert.assertEquals(generator.getType(), entry.getClickHouseType());
            Assert.assertEquals(generator.isStorable(), entry.isStorable());
            Assert.assertNotNull(GenerateTestDataSet.class.getResource("/datasets/" + entry.getName() + ".tsv"),
                    "Missing TSV dataset: " + entry.getName());
            if (!entry.isStorable()) {
                Assert.assertNotNull(GenerateTestDataSet.class.getResource("/datasets/" + entry.getName() + ".txt"),
                        "Missing values dataset: " + entry.getName());
            }
        }

        Assert.assertTrue(entries.contains(new GenerateTestDataSet.DataSetEntry(
                "binary_string", "BinaryString", "String", GenerateTestDataSet.KIND_STORABLE)));
        Assert.assertTrue(entries.contains(new GenerateTestDataSet.DataSetEntry(
                "enum8_single", "Enum8Single", "Enum8('only' = 42)", GenerateTestDataSet.KIND_STORABLE)));
        Assert.assertTrue(entries.contains(new GenerateTestDataSet.DataSetEntry(
                "nothing", "Nothing", "Nullable(Nothing)", GenerateTestDataSet.KIND_VALUES_ONLY)));
    }

    @Test(groups = {"unit"})
    public void testReadDataSetIndexFromPath() throws IOException {
        Path tempFile = Files.createTempFile("dataset-index", ".tsv");
        try {
            String content = "name\ttype\tclickhouse_type\tkind\n"
                    + "int8\tInt8\tInt8\tstorable\n"
                    + "interval_day\tIntervalDay\tIntervalDay\tvalues-only\n";
            Files.write(tempFile, content.getBytes(StandardCharsets.UTF_8));
            List<GenerateTestDataSet.DataSetEntry> entries = GenerateTestDataSet.readDataSetIndex(tempFile);
            Assert.assertEquals(entries.size(), 2);
            Assert.assertEquals(entries.get(0), new GenerateTestDataSet.DataSetEntry(
                    "int8", "Int8", "Int8", GenerateTestDataSet.KIND_STORABLE));
            Assert.assertTrue(entries.get(0).isStorable());
            Assert.assertEquals(entries.get(1), new GenerateTestDataSet.DataSetEntry(
                    "interval_day", "IntervalDay", "IntervalDay", GenerateTestDataSet.KIND_VALUES_ONLY));
            Assert.assertFalse(entries.get(1).isStorable());
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @DataProvider
    public Object[][] invalidIndexes() {
        return new Object[][] {
                {""},
                {"wrong\theader\tclickhouse_type\tkind\n"},
                {"name\ttype\tclickhouse_type\tkind\nint8\tInt8\tInt8\n"},
                {"name\ttype\tclickhouse_type\tkind\nint8\t\tInt8\tstorable\n"},
                {"name\ttype\tclickhouse_type\tkind\nint8\tInt8\tInt8\tunknown\n"},
        };
    }

    @Test(groups = {"unit"}, dataProvider = "invalidIndexes", expectedExceptions = IllegalArgumentException.class)
    public void testReadInvalidDataSetIndex(String content) throws IOException {
        GenerateTestDataSet.readDataSetIndex(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
    }
}
