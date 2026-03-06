package com.clickhouse.client;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;


public class DatasetBuilder {


    private final ValueGenerators defaultGenerators = new ValueGenerators();

    public ValueGenerators getDefaultGenerators() {
        return defaultGenerators;
    }

    public static class DatasetSnapshot {
        private final InputStream inputStream;
        private final List<List<String>> values;

        public DatasetSnapshot(InputStream inputStream, List<List<String>> values) {
            this.inputStream = inputStream;
            this.values = values;
        }

        public InputStream getInputStream() {
            return inputStream;
        }

        public List<List<String>> getValues() {
            return values;
        }
    }

    public static class Dataset {
        private String createTable;
        private String tableName;
        private List<String> columns = new ArrayList<>();
        private List<ClickHouseColumn> chColumns = new ArrayList<>();
        private List<Function<Integer, String>> sqlValueGenerators = new ArrayList<>();

        public Dataset(String tableName, List<String> columns, List<ClickHouseColumn> chColumns, List<Function<Integer, String>> sqlValueGenerators) {
            this.tableName = tableName;
            this.columns = columns;
            this.chColumns = chColumns;
            this.sqlValueGenerators = sqlValueGenerators;
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (\n");
            for (int i = 0; i < columns.size(); i++) {
                sb.append("    ").append(columns.get(i));
                if (i < columns.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            sb.append(") ENGINE = ").append("MergeTree Order By(id)");
            createTable = sb.toString();
        }

        public String getCreateTable() {
            return createTable;
        }

        public String getTableName() {
            return tableName;
        }

        public List<ClickHouseColumn> getChColumns() {
            return chColumns;
        }

        /**
         * Generates TSV content for this dataset and returns a snapshot
         * containing both the InputStream over TSV bytes and the raw values.
         *
         * @param numRows number of rows to generate
         * @return snapshot with InputStream and values indexed by [row][column]
         */
        public DatasetSnapshot toTsvInputStream(int numRows) {
            List<List<String>> values = new ArrayList<>(numRows);
            StringBuilder sb = new StringBuilder();
            for (int row = 0; row < numRows; row++) {
                List<String> rowValues = new ArrayList<>(sqlValueGenerators.size());
                for (int col = 0; col < sqlValueGenerators.size(); col++) {
                    if (col > 0) {
                        sb.append('\t');
                    }
                    String value = sqlValueGenerators.get(col).apply(row);
                    rowValues.add(value);
                    sb.append(value);
                }
                sb.append('\n');
                values.add(rowValues);
            }
            byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            InputStream inputStream = new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.remaining());
            return new DatasetSnapshot(inputStream, values);
        }
    }

    /**
     * Builder that builds table schema and creates a dataset.
     * What is returned is a table definition depending on build method parameters.
     */
    public static class SchemaBasedBuilder {

        private final ValueGenerators generators;

        private List<String> columns = new ArrayList<>();
        private List<ClickHouseColumn> chColumns = new ArrayList<>();
        private List<Function<Integer, String>> sqlValueGenerators = new ArrayList<>();

        private int columnCount = 0;

        public SchemaBasedBuilder(ValueGenerators generators) {
            this.generators = generators;
        }

        private SchemaBasedBuilder addColumn(String prefix, String typeExpr, Function<Integer, String> generator) {
            String columnName = prefix + columnCount++;
            columns.add(columnName + " " + typeExpr);
            chColumns.add(ClickHouseColumn.of(columnName, typeExpr));
            sqlValueGenerators.add(generator);
            return this;
        }

        // ---- Integer types ----

        public SchemaBasedBuilder int8() {
            return addColumn("i8", ClickHouseDataType.Int8.name(), generators::int8);
        }

        public SchemaBasedBuilder int16() {
            return addColumn("i16", ClickHouseDataType.Int16.name(), generators::int16);
        }

        public SchemaBasedBuilder int32() {
            return addColumn("i32", ClickHouseDataType.Int32.name(), generators::int32);
        }

        public SchemaBasedBuilder int64() {
            return addColumn("i64", ClickHouseDataType.Int64.name(), generators::int64);
        }

        public SchemaBasedBuilder int128() {
            return addColumn("i128", ClickHouseDataType.Int128.name(), generators::int128);
        }

        public SchemaBasedBuilder int256() {
            return addColumn("i256", ClickHouseDataType.Int256.name(), generators::int256);
        }

        public SchemaBasedBuilder uint8() {
            return addColumn("u8", ClickHouseDataType.UInt8.name(), generators::uint8);
        }

        public SchemaBasedBuilder uint16() {
            return addColumn("u16", ClickHouseDataType.UInt16.name(), generators::uint16);
        }

        public SchemaBasedBuilder uint32() {
            return addColumn("u32", ClickHouseDataType.UInt32.name(), generators::uint32);
        }

        public SchemaBasedBuilder uint64() {
            return addColumn("u64", ClickHouseDataType.UInt64.name(), generators::uint64);
        }

        public SchemaBasedBuilder uint128() {
            return addColumn("u128", ClickHouseDataType.UInt128.name(), generators::uint128);
        }

        public SchemaBasedBuilder uint256() {
            return addColumn("u256", ClickHouseDataType.UInt256.name(), generators::uint256);
        }

        // ---- Float types ----

        public SchemaBasedBuilder bfloat16() {
            return addColumn("bf16", ClickHouseDataType.BFloat16.name(), generators::bfloat16);
        }

        public SchemaBasedBuilder float32() {
            return addColumn("f32", ClickHouseDataType.Float32.name(), generators::float32);
        }

        public SchemaBasedBuilder float64() {
            return addColumn("f64", ClickHouseDataType.Float64.name(), generators::float64);
        }

        // ---- Boolean ----

        public SchemaBasedBuilder bool() {
            return addColumn("bool", ClickHouseDataType.Bool.name(), generators::bool);
        }

        // ---- String types ----

        public SchemaBasedBuilder string() {
            return addColumn("str", ClickHouseDataType.String.name(), generators::string);
        }

        public SchemaBasedBuilder fixedString(int length) {
            return addColumn("fstr", ClickHouseDataType.FixedString.name() + "(" + length + ")", generators::fixedString);
        }

        // ---- Date / Time types ----

        public SchemaBasedBuilder date() {
            return addColumn("dt", ClickHouseDataType.Date.name(), generators::date);
        }

        public SchemaBasedBuilder date32() {
            return addColumn("dt32", ClickHouseDataType.Date32.name(), generators::date32);
        }

        public SchemaBasedBuilder dateTime() {
            return addColumn("dttm", ClickHouseDataType.DateTime.name(), generators::dateTime);
        }

        public SchemaBasedBuilder dateTime(String timezone) {
            return addColumn("dttm", ClickHouseDataType.DateTime.name() + "('" + timezone + "')", generators::dateTime);
        }

        public SchemaBasedBuilder dateTime32() {
            return addColumn("dttm32", ClickHouseDataType.DateTime32.name(), generators::dateTime32);
        }

        public SchemaBasedBuilder dateTime32(String timezone) {
            return addColumn("dttm32", ClickHouseDataType.DateTime32.name() + "('" + timezone + "')", generators::dateTime32);
        }

        public SchemaBasedBuilder dateTime64(int precision) {
            return addColumn("dttm64", ClickHouseDataType.DateTime64.name() + "(" + precision + ")", generators::dateTime64);
        }

        public SchemaBasedBuilder dateTime64(int precision, String timezone) {
            return addColumn("dttm64", ClickHouseDataType.DateTime64.name() + "(" + precision + ", '" + timezone + "')", generators::dateTime64);
        }

        // ---- Decimal types ----

        public SchemaBasedBuilder decimal(int precision, int scale) {
            return addColumn("dec", ClickHouseDataType.Decimal.name() + "(" + precision + ", " + scale + ")", generators::decimal);
        }

        public SchemaBasedBuilder decimal32(int scale) {
            return addColumn("dec32", ClickHouseDataType.Decimal32.name() + "(" + scale + ")", generators::decimal32);
        }

        public SchemaBasedBuilder decimal64(int scale) {
            return addColumn("dec64", ClickHouseDataType.Decimal64.name() + "(" + scale + ")", generators::decimal64);
        }

        public SchemaBasedBuilder decimal128(int scale) {
            return addColumn("dec128", ClickHouseDataType.Decimal128.name() + "(" + scale + ")", generators::decimal128);
        }

        public SchemaBasedBuilder decimal256(int scale) {
            return addColumn("dec256", ClickHouseDataType.Decimal256.name() + "(" + scale + ")", generators::decimal256);
        }

        // ---- Network types ----

        public SchemaBasedBuilder ipv4() {
            return addColumn("ipv4", ClickHouseDataType.IPv4.name(), generators::ipv4);
        }

        public SchemaBasedBuilder ipv6() {
            return addColumn("ipv6", ClickHouseDataType.IPv6.name(), generators::ipv6);
        }

        // ---- UUID ----

        public SchemaBasedBuilder uuid() {
            return addColumn("uuid", ClickHouseDataType.UUID.name(), generators::uuid);
        }

        // ---- Enum types ----

        /**
         * @param values pairs of (name, value) e.g. enum8("a", 1, "b", 2)
         */
        public SchemaBasedBuilder enum8(Object... values) {
            return addColumn("e8", ClickHouseDataType.Enum8.name() + "(" + enumDef(values) + ")", generators::enum8);
        }

        /**
         * @param values pairs of (name, value) e.g. enum16("a", 1, "b", 2)
         */
        public SchemaBasedBuilder enum16(Object... values) {
            return addColumn("e16", ClickHouseDataType.Enum16.name() + "(" + enumDef(values) + ")", generators::enum16);
        }

        // ---- Complex / Container types ----

        public SchemaBasedBuilder array(String elementType) {
            return addColumn("arr", ClickHouseDataType.Array.name() + "(" + elementType + ")", generators::array);
        }

        public SchemaBasedBuilder map(String keyType, String valueType) {
            return addColumn("map", ClickHouseDataType.Map.name() + "(" + keyType + ", " + valueType + ")", generators::map);
        }

        public SchemaBasedBuilder tuple(String... memberTypes) {
            return addColumn("tup", ClickHouseDataType.Tuple.name() + "(" + String.join(", ", memberTypes) + ")", generators::tuple);
        }

        public SchemaBasedBuilder nested(String... columnDefs) {
            return addColumn("nst", ClickHouseDataType.Nested.name() + "(" + String.join(", ", columnDefs) + ")", generators::nested);
        }

        // ---- Modifiers ----

        public SchemaBasedBuilder nullable(String type) {
            return addColumn("n", ClickHouseDataType.Nullable.name() + "(" + type + ")", generators::nullable);
        }

        public SchemaBasedBuilder lowCardinality(String type) {
            return addColumn("lc", ClickHouseDataType.LowCardinality.name() + "(" + type + ")", generators::lowCardinality);
        }

        // ---- Geo types ----

        public SchemaBasedBuilder point() {
            return addColumn("pt", ClickHouseDataType.Point.name(), generators::point);
        }

        public SchemaBasedBuilder ring() {
            return addColumn("ring", ClickHouseDataType.Ring.name(), generators::ring);
        }

        public SchemaBasedBuilder lineString() {
            return addColumn("lstr", ClickHouseDataType.LineString.name(), generators::lineString);
        }

        public SchemaBasedBuilder multiLineString() {
            return addColumn("mlstr", ClickHouseDataType.MultiLineString.name(), generators::multiLineString);
        }

        public SchemaBasedBuilder polygon() {
            return addColumn("poly", ClickHouseDataType.Polygon.name(), generators::polygon);
        }

        public SchemaBasedBuilder multiPolygon() {
            return addColumn("mpoly", ClickHouseDataType.MultiPolygon.name(), generators::multiPolygon);
        }

        // ---- JSON ----

        public SchemaBasedBuilder json() {
            return addColumn("json", ClickHouseDataType.JSON.name(), generators::json);
        }

        // ---- Special types ----

        public SchemaBasedBuilder nothing() {
            return addColumn("nth", ClickHouseDataType.Nothing.name(), generators::nothing);
        }

        public SchemaBasedBuilder simpleAggregateFunction(String function, String type) {
            return addColumn("saf", ClickHouseDataType.SimpleAggregateFunction.name() + "(" + function + ", " + type + ")", generators::simpleAggregateFunction);
        }

        public SchemaBasedBuilder aggregateFunction(String function, String... argTypes) {
            return addColumn("af", ClickHouseDataType.AggregateFunction.name() + "(" + function + ", " + String.join(", ", argTypes) + ")", generators::aggregateFunction);
        }

        /**
         * Adds a column with an arbitrary type expression for types not covered
         * by dedicated methods.
         */
        public SchemaBasedBuilder column(String prefix, String typeExpr) {
            return addColumn(prefix, typeExpr, index -> "");
        }

        // ---- Build ----

        public Dataset build(String tableName) {
            List<String> allColumns = new ArrayList<>();
            allColumns.add("id UInt64");
            allColumns.addAll(columns);

            List<ClickHouseColumn> allChColumns = new ArrayList<>();
            allChColumns.add(ClickHouseColumn.of("id", "UInt64"));
            allChColumns.addAll(chColumns);

            List<Function<Integer, String>> allGenerators = new ArrayList<>();
            allGenerators.add(index -> String.valueOf(index));
            allGenerators.addAll(sqlValueGenerators);

            return new Dataset(tableName, allColumns, allChColumns, allGenerators);
        }


        private final Random random = new Random();

        public Dataset build() {
            return build("test_table_" + Math.abs(random.nextLong()));
        }


        private static String enumDef(Object... values) {
            if (values.length % 2 != 0) {
                throw new IllegalArgumentException("Enum values must be pairs of (name, ordinal)");
            }
            return IntStream.range(0, values.length / 2)
                    .mapToObj(i -> "'" + values[i * 2] + "' = " + values[i * 2 + 1])
                    .collect(Collectors.joining(", "));
        }
    }


    static class ValueGenerators {

        private static final String DATASETS_DIR = "datasets/";

        private final Map<String, List<String>> datasets = new HashMap<>();

        public ValueGenerators() {
            String[] names = {
                    "Int8", "Int16", "Int32", "Int64", "Int128", "Int256",
                    "UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256",
                    "Float32", "Float64",
                    "Boolean",
                    "String",
                    "Date", "Date32",
                    "DateTime", "DateTime64",
                    "Decimal",
                    "IPv4", "IPv6",
                    "UUID"
            };
            for (String name : names) {
                List<String> values = loadDataset(name);
                if (!values.isEmpty()) {
                    datasets.put(name, values);
                }
            }
        }

        private List<String> loadDataset(String name) {
            String resourcePath = DATASETS_DIR + name + ".tsv";
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
                if (is == null) {
                    return Collections.emptyList();
                }
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                    return reader.lines()
                            .filter(line -> !line.isEmpty())
                            .collect(Collectors.toList());
                }
            } catch (IOException e) {
                return Collections.emptyList();
            }
        }

        private String valueAt(String datasetName, int index) {
            List<String> values = datasets.get(datasetName);
            if (values == null || values.isEmpty()) {
                return "";
            }
            return values.get(Math.floorMod(index, values.size()));
        }

        // ---- Integer types ----

        public String int8(int index) { return valueAt("Int8", index); }
        public String int16(int index) { return valueAt("Int16", index); }
        public String int32(int index) { return valueAt("Int32", index); }
        public String int64(int index) { return valueAt("Int64", index); }
        public String int128(int index) { return valueAt("Int128", index); }
        public String int256(int index) { return valueAt("Int256", index); }
        public String uint8(int index) { return valueAt("UInt8", index); }
        public String uint16(int index) { return valueAt("UInt16", index); }
        public String uint32(int index) { return valueAt("UInt32", index); }
        public String uint64(int index) { return valueAt("UInt64", index); }
        public String uint128(int index) { return valueAt("UInt128", index); }
        public String uint256(int index) { return valueAt("UInt256", index); }

        // ---- Float types ----

        public String bfloat16(int index) { return ""; }
        public String float32(int index) { return valueAt("Float32", index); }
        public String float64(int index) { return valueAt("Float64", index); }

        // ---- Boolean ----

        public String bool(int index) { return valueAt("Boolean", index); }

        // ---- String types ----

        public String string(int index) { return valueAt("String", index); }
        public String fixedString(int index) { return valueAt("String", index); }

        // ---- Date / Time types ----

        public String date(int index) { return valueAt("Date", index); }
        public String date32(int index) { return valueAt("Date32", index); }
        public String dateTime(int index) { return valueAt("DateTime", index); }
        public String dateTime32(int index) { return valueAt("DateTime", index); }
        public String dateTime64(int index) { return valueAt("DateTime64", index); }

        // ---- Decimal types ----

        public String decimal(int index) { return valueAt("Decimal", index); }
        public String decimal32(int index) { return valueAt("Decimal", index); }
        public String decimal64(int index) { return valueAt("Decimal", index); }
        public String decimal128(int index) { return valueAt("Decimal", index); }
        public String decimal256(int index) { return valueAt("Decimal", index); }

        // ---- Network types ----

        public String ipv4(int index) { return valueAt("IPv4", index); }
        public String ipv6(int index) { return valueAt("IPv6", index); }

        // ---- UUID ----

        public String uuid(int index) { return valueAt("UUID", index); }

        // ---- Enum types ----

        public String enum8(int index) { return ""; }
        public String enum16(int index) { return ""; }

        // ---- Complex / Container types ----

        public String array(int index) { return ""; }
        public String map(int index) { return ""; }
        public String tuple(int index) { return ""; }
        public String nested(int index) { return ""; }

        // ---- Modifiers ----

        public String nullable(int index) { return ""; }
        public String lowCardinality(int index) { return ""; }

        // ---- Geo types ----

        public String point(int index) { return ""; }
        public String ring(int index) { return ""; }
        public String lineString(int index) { return ""; }
        public String multiLineString(int index) { return ""; }
        public String polygon(int index) { return ""; }
        public String multiPolygon(int index) { return ""; }

        // ---- JSON ----

        public String json(int index) { return ""; }

        // ---- Special types ----

        public String nothing(int index) { return ""; }
        public String simpleAggregateFunction(int index) { return ""; }
        public String aggregateFunction(int index) { return ""; }
    }
}
