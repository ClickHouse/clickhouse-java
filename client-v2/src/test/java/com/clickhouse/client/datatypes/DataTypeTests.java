package com.clickhouse.client.datatypes;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.DataTypeUtils;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.sql.SQLUtils;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseVersion;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class DataTypeTests extends BaseIntegrationTest {

    private Client client;
    private InsertSettings settings;

    private boolean useClientCompression = false;

    private boolean useHttpCompression = false;

    private static final int EXECUTE_CMD_TIMEOUT = 10; // seconds

    public DataTypeTests(boolean useClientCompression, boolean useHttpCompression) {
        this.useClientCompression = useClientCompression;
        this.useHttpCompression = useHttpCompression;
    }

    public DataTypeTests() {
        this(false, false);
    }

    @BeforeMethod(groups = {"integration"})
    public void setUp() throws IOException {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .compressClientRequest(useClientCompression)
                .useHttpCompression(useHttpCompression)
                .build();
    }

    @AfterMethod(groups = {"integration"})
    public void tearDown() {
        client.close();
    }

    private <T> void writeReadVerify(String table, String tableDef, Class<T> dtoClass, List<T> data,
                                     BiConsumer<List<T>, T> rowVerifier) throws Exception {
        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDef);

        final TableSchema tableSchema = client.getTableSchema(table);
        client.register(dtoClass, tableSchema);
        client.insert(table, data);
        final AtomicInteger rowCount = new AtomicInteger(0);
        client.queryAll("SELECT * FROM " + table, dtoClass, tableSchema).forEach(dto -> {
            rowVerifier.accept(data, dto);
            rowCount.incrementAndGet();
        });

        Assert.assertEquals(rowCount.get(), data.size());
    }

    @Test(groups = {"integration"})
    public void testNestedDataTypes() throws Exception {
        final String table = "test_nested_types";
        writeReadVerify(table,
                NestedTypesDTO.tblCreateSQL(table),
                NestedTypesDTO.class,
                Arrays.asList(new NestedTypesDTO(0, new Object[]{(short) 127, "test 1"}, new double[]{0.3d, 0.4d})),
                (data, dto) -> {
                    NestedTypesDTO dataDto = data.get(dto.getRowId());
                    Assert.assertEquals(dto.getTuple1(), dataDto.getTuple1());
                    Assert.assertEquals(dto.getPoint1(), dataDto.getPoint1());
                });
    }

    @Test(groups = {"integration"})
    public void testArrays() throws Exception {
        final String table = "test_arrays";
        writeReadVerify(table,
                DTOForArraysTests.tblCreateSQL(table),
                DTOForArraysTests.class,
                Arrays.asList(new DTOForArraysTests(
                        0, Arrays.asList("db", "fast"), new int[]{1, 2, 3}, new String[]{"a", "b", "c"})),
                (data, dto) -> {
                    DTOForArraysTests dataDto = data.get(dto.getRowId());
                    Assert.assertEquals(dto.getWords(), dataDto.getWords());
                    Assert.assertEquals(dto.getLetters(), dataDto.getLetters());
                    Assert.assertEquals(dto.getNumbers(), dataDto.getNumbers());
                });
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DTOForArraysTests {
        private int rowId;

        private List<String> words;

        private int[] numbers;

        private String[] letters;

        public static String tblCreateSQL(String table) {
            return tableDefinition(table, "rowId Int16", "words Array(String)", "numbers Array(Int32)",
                    "letters Array(String)");
        }
    }

    @Test(groups = {"integration"})
    public void testVariantWithSimpleDataTypes() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        final String table = "test_variant_primitives";
        final DataTypesTestingPOJO sample = new DataTypesTestingPOJO();

        dataTypesLoop:
        for (ClickHouseDataType dataType : ClickHouseDataType.values()) {
            client.execute("DROP TABLE IF EXISTS " + table).get();
            StringBuilder b = new StringBuilder(" CREATE TABLE ");
            b.append(table).append(" ( rowId Int64, field Variant(String, ").append(dataType.name());

            switch (dataType) {
                case BFloat16:
                    // TODO: add support
                    continue dataTypesLoop;
                    // skipped
                case String:
                case FixedString:
                case Nothing:
                case Variant:
                case JSON:
                case Object:
                case Dynamic:
                    // no tests or tested in other tests
                case Decimal:
                case Decimal32:
                case Decimal64:
                case Decimal128:
                case Decimal256:
                case Array:
                case Map:
                case Nested:
                case Tuple:
                case SimpleAggregateFunction:
                case AggregateFunction:
                case Enum8:
                case Enum16:
                case Enum:
                case Nullable: // virtual type
                case LowCardinality: // virtual type
                case LineString: // same as Ring
                case MultiLineString: // same as MultiPolygon
                case Time:
                case Time64:
                    // tested separately
                    continue dataTypesLoop;

            }
            b.append(")) Engine = MergeTree ORDER BY ()");

            client.execute(b.toString(),
                    (CommandSettings) new CommandSettings().serverSetting("allow_experimental_variant_type", "1"));
            client.register(DTOForVariantPrimitivesTests.class, client.getTableSchema(table));

            Object value = null;
            for (Method m : sample.getClass().getDeclaredMethods()) {
                if (m.getName().equalsIgnoreCase("get" + dataType.name())) {
                    value = m.invoke(sample);
                    break;
                }
            }
            Assert.assertNotNull(value);

            List<DTOForVariantPrimitivesTests> data = new ArrayList<>();
            data.add(new DTOForVariantPrimitivesTests(0, value));
            client.insert(table, data).get().close();

            List<GenericRecord> rows = client.queryAll("SELECT * FROM " + table);
            for (GenericRecord row : rows) {
                String strValue = row.getString("field");
                switch (dataType) {
                    case Date:
                    case Date32:
                        strValue = row.getLocalDate("field").toString();
                        break;
                    case DateTime64:
                    case DateTime:
                    case DateTime32:
                        strValue = row.getLocalDateTime("field").truncatedTo(ChronoUnit.SECONDS).toString();
                        value = ((LocalDateTime) value).truncatedTo(ChronoUnit.SECONDS).toString();
                        break;
                    case Point:
                        strValue = row.getGeoPoint("field").toString();
                        break;
                    case Ring:
                        strValue = row.getGeoRing("field").toString();
                        break;
                    case Polygon:
                        strValue = row.getGeoPolygon("field").toString();
                        break;
                    case MultiPolygon:
                        strValue = row.getGeoMultiPolygon("field").toString();
                        break;
                    case IntervalMicrosecond:
                    case IntervalMillisecond:
                    case IntervalSecond:
                    case IntervalMinute:
                    case IntervalHour:
                        strValue = String.valueOf(row.getTemporalAmount("field"));
                        break;
                    case IntervalDay:
                    case IntervalWeek:
                    case IntervalMonth:
                    case IntervalQuarter:
                    case IntervalYear:
                        Period period = (Period) value;
                        long days = (period).getDays() + Math.round((period.toTotalMonths() * 30));
                        value = Period.ofDays((int) days);
                        period = (Period) row.getTemporalAmount("field");
                        days = (period).getDays() + Math.round((period.toTotalMonths() * 30));
                        strValue = Period.ofDays((int) days).toString();
                        break;

                }
                if (value.getClass().isPrimitive()) {
                    Assert.assertEquals(strValue, String.valueOf(value));
                } else {
                    Assert.assertEquals(strValue, String.valueOf(value));
                }
            }
        }
    }

    @Data
    @AllArgsConstructor
    public static class DTOForVariantPrimitivesTests {
        private int rowId;
        private Object field;
    }

    @Test(groups = {"integration"})
    public void testVariantWithDecimals() throws Exception {
        testVariantWith("decimals", new String[]{"field Variant(String, Decimal(4, 4))"},
                new Object[]{
                        "10.2",
                        10.2d, // TODO: when f it gives 10.199
                },
                new String[]{
                        "10.2",
                        "10.2000",
                });
        testVariantWith("decimal32", new String[]{"field Variant(String, Decimal32(4))"},
                new Object[]{
                        "10.202",
                        10.1233d,
                },
                new String[]{
                        "10.202",
                        "10.1233",
                });
    }

    @Test(groups = {"integration"})
    public void testVariantWithArrays() throws Exception {
        testVariantWith("arrays", new String[]{"field Variant(String, Array(String))"},
                new Object[]{
                        "a,b",
                        new String[]{"a", "b"},
                        Arrays.asList("c", "d")
                },
                new String[]{
                        "a,b",
                        "[a, b]",
                        "[c, d]"
                });
        testVariantWith("arrays", new String[]{"field Variant(Array(String), Array(Int32))"},
                new Object[]{
                        new int[]{1, 2},
                        new String[]{"a", "b"},
                        Arrays.asList("c", "d")
                },
                new String[]{
                        "[1, 2]",
                        "[a, b]",
                        "[c, d]",
                });

        testVariantWith("arrays", new String[]{"field Variant(Array(Array(String)), Array(Array(Int32)))"},
                new Object[]{
                        new int[][]{ new int[] {1, 2}, new int[] { 3, 4}},
                        new String[][]{new String[]{"a", "b"}, new String[]{"c", "d"}},
                        Arrays.asList(Arrays.asList("e", "f"), Arrays.asList("j", "h"))
                },
                new String[]{
                        "[[1, 2], [3, 4]]",
                        "[[a, b], [c, d]]",
                        "[[e, f], [j, h]]",
                });
    }

    @Test(groups = {"integration"})
    public void testVariantWithMaps() throws Exception {
        Map<String, Byte> map1 = new HashMap<>();
        map1.put("key1", (byte) 1);
        map1.put("key2", (byte) 2);
        map1.put("key3", (byte) 3);

        testVariantWith("maps", new String[]{"field Variant(Map(String, String), Map(String, Int128))"},
                new Object[]{
                        map1
                },
                new String[]{
                        "{key1=1, key2=2, key3=3}",
                });


        Map<Integer, String> map2 = new HashMap<>();
        map2.put(1, "a");
        map2.put(2, "b");

        Map<String, String> map3 = new HashMap<>();
        map3.put("1", "a");
        map3.put("2", "b");

        testVariantWith("maps", new String[]{"field Variant(Map(Int32, String), Map(String, String))"},
                new Object[]{
                        map2,
                        map3
                },
                new String[]{
                        "{1=a, 2=b}",
                        "{1=a, 2=b}",
                });
    }

    @Test(groups = {"integration"})
    public void testVariantWithEnums() throws Exception {
        testVariantWith("enums", new String[]{"field Variant(Bool, Enum('stopped' = 1, 'running' = 2))"},
                new Object[]{
                        "stopped",
                        1,
                        "running",
                        2,
                        true,
                        false
                },
                new String[]{
                        "stopped",
                        "stopped",
                        "running",
                        "running",
                        "true",
                        "false"
                });
    }

    @Test(groups = {"integration"}, enabled = false)
    public void testVariantWithTuple() throws Exception {
        // TODO: same as array
        testVariantWith("arrays", new String[]{"field Variant(String, Tuple(Int32, Float32))"},
                new Object[]{
                        "10,0.34",
                        new Object[]{10, 0.34f}
                },
                new String[]{
                        "10,0.34",
                        "(10,0.34)",
                });
    }

    @Test(groups = {"integration"})
    public void testVariantWithTime64Types() throws Exception {
        if (isVersionMatch("(,25.5]")) {
            return; // time64 was introduced in 25.6
        }
        testVariantWith("Time", new String[]{"field Variant(Time, String)"},
                new Object[]{
                        "30:33:30",
                        TimeUnit.HOURS.toSeconds(100) + TimeUnit.MINUTES.toSeconds(10) + 30
                },
                new String[]{
                        "30:33:30",
                        "360630", // Time stored as integer by default
                });

        testVariantWith("Time64", new String[]{"field Variant(Time64, String)"},
                new Object[]{
                        "30:33:30",
                        TimeUnit.HOURS.toSeconds(100) + TimeUnit.MINUTES.toSeconds(10) + 30
                },
                new String[]{
                        "30:33:30",
                        "360630",
                });
    }

    @Test(groups = {"integration"})
    public void testDynamicWithPrimitives() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        final String table = "test_dynamic_primitives";
        final DataTypesTestingPOJO sample = new DataTypesTestingPOJO();

        client.execute("DROP TABLE IF EXISTS " + table).get();
        String createTableStatement = " CREATE TABLE " + table + "( rowId Int64, field Dynamic ) " +
                "Engine = MergeTree ORDER BY ()";

        client.execute(createTableStatement, (CommandSettings) new CommandSettings().serverSetting("allow_experimental_dynamic_type", "1"));
        client.register(DTOForDynamicPrimitivesTests.class, client.getTableSchema(table));

        int rowId = 0;
        for (ClickHouseDataType dataType : ClickHouseDataType.values()) {
            switch (dataType) {
                case BFloat16:
                    // TODO: add support
                    continue;
                case Array:
                case Map:
                case AggregateFunction:
                case SimpleAggregateFunction:
                    // tested separately
                    continue;
                case Dynamic:
                case Nothing: // array tests
                case Object: // deprecated
                case JSON:
                case Nested:
                case Tuple:
                case Variant:
                case Decimal: // virtual type
                case Nullable: // virtual type
                case LowCardinality: // virtual type
                case Enum: // virtual type
                case LineString: // same as Ring
                case MultiLineString: // same as MultiPolygon
                case Time:
                case Time64:
                    // no tests or tested in other tests
                    continue;
                default:
            }

            Object value = null;
            if (dataType == ClickHouseDataType.Enum8) {
                value = sample.getSmallEnum();
            } else if (dataType == ClickHouseDataType.Enum16) {
                value = sample.getLargeEnum();
            } else {
                for (Method m : sample.getClass().getDeclaredMethods()) {
                    if (m.getName().equalsIgnoreCase("get" + dataType.name())) {
                        value = m.invoke(sample);
                        break;
                    }
                }
            }

            Assert.assertNotNull(value, "Value for " + dataType.name() + " should not be null.");

            List<DTOForDynamicPrimitivesTests> data = new ArrayList<>();
            data.add(new DTOForDynamicPrimitivesTests(rowId++, value));
            client.insert(table, data).get().close();
            List<GenericRecord> rows = client.queryAll("SELECT * FROM " + table + " ORDER BY rowId DESC  ");
            GenericRecord row = rows.get(0);
                String strValue = row.getString("field");
                switch (dataType) {
                    case Date:
                    case Date32:
                        strValue = row.getLocalDate("field").toString();
                        break;
                    case DateTime64:
                    case DateTime:
                    case DateTime32:
                        strValue = row.getLocalDateTime("field").truncatedTo(ChronoUnit.SECONDS).toString();
                        value = ((LocalDateTime) value).truncatedTo(ChronoUnit.SECONDS).toString();
                        break;
                    case Point:
                        strValue = row.getGeoPoint("field").toString();
                        break;
                    case Ring:
                        strValue = row.getGeoRing("field").toString();
                        break;
                    case Polygon:
                        strValue = row.getGeoPolygon("field").toString();
                        break;
                    case MultiPolygon:
                        strValue = row.getGeoMultiPolygon("field").toString();
                        break;
                    case Decimal32:
                    case Decimal64:
                    case Decimal128:
                    case Decimal256:
                        BigDecimal tmpDec = row.getBigDecimal("field").stripTrailingZeros();
                        strValue = tmpDec.toPlainString();
                        break;
                    case IntervalMicrosecond:
                    case IntervalMillisecond:
                    case IntervalSecond:
                    case IntervalMinute:
                    case IntervalHour:
                        strValue = String.valueOf(row.getTemporalAmount("field"));
                        break;
                    case IntervalDay:
                    case IntervalWeek:
                    case IntervalMonth:
                    case IntervalQuarter:
                    case IntervalYear:
                        strValue = String.valueOf(row.getTemporalAmount("field"));
                        Period period = (Period) value;
                        long days = (period).getDays() + Math.round((period.toTotalMonths() / 12d) * 360);
                        value = Period.ofDays((int) days);
                        break;
                }
                if (value.getClass().isPrimitive()) {
                    Assert.assertEquals(strValue, String.valueOf(value));
                } else {
                    Assert.assertEquals(strValue, String.valueOf(value));
                }
        }
    }

    @Test(groups = {"integration"})
    public void testDynamicWithArrays() throws Exception {
        testDynamicWith("arrays",
                new Object[]{
                        "a,b",
                        new String[]{"a", null, "b"},
                        Arrays.asList("c", "d"),
                        new Integer[]{1, null, 2, null, 3}

                },
                new String[]{
                        "a,b",
                        "[a, NULL, b]",
                        "[c, d]",
                        "[1, NULL, 2, NULL, 3]"
                });
        testDynamicWith("arrays",
                new Object[]{
                        new int[]{1, 2},
                        new String[]{"a", "b"},
                        Arrays.asList("c", "d"),
                        Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4)),
                        Arrays.asList(Arrays.asList(1, 2), Collections.emptyList()),
                        Arrays.asList(Arrays.asList(1, 2), null, Arrays.asList(3, 4))
                },
                new String[]{
                        "[1, 2]",
                        "[a, b]",
                        "[c, d]",
                        "[[1, 2], [3, 4]]",
                        "[[1, 2], []]",
                        "[[1, 2], [], [3, 4]]"
                });
    }

    @Test(groups = {"integration"})
    public void testDynamicWithMaps() throws Exception {
        Map<String, Byte> map1 = new HashMap<>();
        map1.put("key1", (byte) 1);
        map1.put("key2", (byte) 2);
        map1.put("key3", (byte) 3);

        testDynamicWith("maps",
                new Object[]{
                        map1
                },
                new String[]{
                        "{key1=1, key2=2, key3=3}",
                });


        Map<Integer, String> map2 = new HashMap<>();
        map2.put(1, "a");
        map2.put(2, "b");

        Map<String, String> map3 = new HashMap<>();
        map3.put("1", "a");
        map3.put("2", "b");

        testDynamicWith("maps",
                new Object[]{
                        map2,
                        map3
                },
                new String[]{
                        "{1=a, 2=b}",
                        "{1=a, 2=b}",
                });
    }

    @Test(groups = {"integration"})
    public void testDynamicWithTime64Types() throws Exception {
        if (isVersionMatch("(,25.5]")) {
            return; // time64 was introduced in 25.6
        }

        long _999_hours = TimeUnit.HOURS.toSeconds(999);
        testDynamicWith("Time",
                new Object[]{
                        _999_hours
                },
                new String[]{
                        String.valueOf(_999_hours),
                });

        Instant maxTime64 = Instant.ofEpochSecond(TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59,
                999999999);

        testDynamicWith("Time64",
                new Object[]{
                        maxTime64,
                },
                new String[]{
                        "3958241016481971977"
                });
    }

    @Data
    @AllArgsConstructor
    public static class DTOForDynamicPrimitivesTests {
        private int rowId;
        private Object field;
    }

    @Test(groups = {"integration"})
    public void testAllDataTypesKnown() {
        List<GenericRecord> dbTypes = client.queryAll("SELECT * FROM system.data_type_families");
        Set<String> unknowTypes = new HashSet<>();
        for (GenericRecord dbType : dbTypes) {
            String aliasFor = dbType.getString("alias_to");
            String typeToCheck;
            if (StringUtils.isNoneBlank(aliasFor)) {
                typeToCheck = aliasFor;
            } else {
                typeToCheck = dbType.getString("name");
            }

            try {
                ClickHouseDataType.valueOf(typeToCheck);
            } catch (Exception e) {
                unknowTypes.add(typeToCheck);
            }
        }

        Assert.assertTrue(unknowTypes.isEmpty(), "There are some unknown types: " + unknowTypes);
    }

    @Test(groups = {"integration"})
    public void testTimeDataType() throws Exception {
        if (isVersionMatch("(,25.5]")) {
            return; // time64 was introduced in 25.6
        }


        String table = "test_time_type";
        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table, "o_num UInt32", "time Time"), (CommandSettings) new CommandSettings().serverSetting("allow_experimental_time_time64_type", "1")).get();

        String insertSQL = "INSERT INTO " + table + " VALUES (1, '999:00:00'), (2, '999:59:59'), (3, '000:00:00'), (4, '-999:59:59')";
        try (QueryResponse response = client.query(insertSQL).get()) {}


        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);

        GenericRecord record = records.get(0);
        Assert.assertEquals(record.getInteger("o_num"), 1);
        Assert.assertEquals(record.getInteger("time"), TimeUnit.HOURS.toSeconds(999));
        Assert.assertEquals(record.getInstant("time"), Instant.ofEpochSecond(TimeUnit.HOURS.toSeconds(999)));

        record = records.get(1);
        Assert.assertEquals(record.getInteger("o_num"), 2);
        Assert.assertEquals(record.getInteger("time"), TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59);
        Assert.assertEquals(record.getInstant("time"), Instant.ofEpochSecond(TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59));

        record = records.get(2);
        Assert.assertEquals(record.getInteger("o_num"), 3);
        Assert.assertEquals(record.getInteger("time"), 0);
        Assert.assertEquals(record.getInstant("time"), Instant.ofEpochSecond(0));

        record = records.get(3);
        Assert.assertEquals(record.getInteger("o_num"), 4);
        Assert.assertEquals(record.getInteger("time"), - (TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59));
        Assert.assertEquals(record.getInstant("time"), Instant.ofEpochSecond(-
                (TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59)));
    }

    @Test(groups = {"integration"})
    public void testTime64() throws Exception {
        if (isVersionMatch("(,25.5]")) {
            return; // time64 was introduced in 25.6
        }

        String table = "test_time64_type";
        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table, "o_num UInt32", "t_sec Time64(0)",  "t_ms Time64(3)", "t_us Time64(6)", "t_ns Time64(9)"),
                (CommandSettings) new CommandSettings().serverSetting("allow_experimental_time_time64_type", "1")).get();

        String[][] values = new String[][] {
                {"00:01:00.123", "00:01:00.123", "00:01:00.123456", "00:01:00.123456789"},
                {"-00:01:00.123", "-00:01:00.123", "-00:01:00.123456", "-00:01:00.123456789"},
                {"-999:59:59.999", "-999:59:59.999", "-999:59:59.999999", "-999:59:59.999999999"},
                {"999:59:59.999", "999:59:59.999", "999:59:59.999999", "999:59:59.999999999"},
        };

        Long[][] expectedValues = new Long[][] {
                {timeToSec(0, 1,0), timeToMs(0, 1,0) + 123, timeToUs(0, 1,0) + 123456, timeToNs(0, 1,0) + 123456789},
                {-timeToSec(0, 1,0), -(timeToMs(0, 1,0) + 123), -(timeToUs(0, 1,0) + 123456), -(timeToNs(0, 1,0) + 123456789)},
                {-timeToSec(999,59, 59), -(timeToMs(999,59, 59) + 999),
                    -(timeToUs(999, 59, 59) + 999999), -(timeToNs(999, 59, 59) + 999999999)},
                {timeToSec(999,59, 59), timeToMs(999,59, 59) + 999,
                    timeToUs(999, 59, 59) + 999999, timeToNs(999, 59, 59) + 999999999},
        };
        
        String[][] expectedInstantStrings = new String[][] {
                {"1970-01-01T00:01:00Z",
                "1970-01-01T00:01:00.123Z",
                "1970-01-01T00:01:00.123456Z",
                "1970-01-01T00:01:00.123456789Z"},

                {"1969-12-31T23:59:00Z",
                "1969-12-31T23:58:59.877Z",
                "1969-12-31T23:58:59.876544Z",
                "1969-12-31T23:58:59.876543211Z"},

                {"1969-11-20T08:00:01Z",
                "1969-11-20T08:00:00.001Z",
                "1969-11-20T08:00:00.000001Z",
                "1969-11-20T08:00:00.000000001Z"},


                {"1970-02-11T15:59:59Z",
                "1970-02-11T15:59:59.999Z",
                "1970-02-11T15:59:59.999999Z",
                "1970-02-11T15:59:59.999999999Z"},
        };
        
        for  (int i = 0; i < values.length; i++) {
            StringBuilder insertSQL = new StringBuilder("INSERT INTO " + table + " VALUES (" + i + ", ");
            for (int j = 0; j < values[i].length; j++) {
                insertSQL.append("'").append(values[i][j]).append("', ");
            }
            insertSQL.setLength(insertSQL.length() - 2);
            insertSQL.append(");");

            client.query(insertSQL.toString()).get().close();

            List<GenericRecord> records = client.queryAll("SELECT * FROM " + table);

            GenericRecord record = records.get(0);
            Assert.assertEquals(record.getInteger("o_num"), i);
            for (int j = 0; j < values[i].length; j++) {
                Assert.assertEquals(record.getLong(j + 2), expectedValues[i][j], "failed at value "  +j);
                Instant actualInstant = record.getInstant(j + 2);
                Assert.assertEquals(actualInstant.toString(), expectedInstantStrings[i][j], "failed at value "  +j);
            }

            client.execute("TRUNCATE TABLE " + table).get();
        }
    }
    
    private static long timeToSec(int hours, int minutes, int seconds) {
        return TimeUnit.HOURS.toSeconds(hours) + TimeUnit.MINUTES.toSeconds(minutes) + seconds;
    }

    private static long timeToMs(int hours, int minutes, int seconds) {
        return TimeUnit.HOURS.toMillis(hours) + TimeUnit.MINUTES.toMillis(minutes) + TimeUnit.SECONDS.toMillis(seconds);
    }

    private static long timeToUs(int hours, int minutes, int seconds) {
        return TimeUnit.HOURS.toMicros(hours) + TimeUnit.MINUTES.toMicros(minutes) + TimeUnit.SECONDS.toMicros(seconds);
    }

    private static long timeToNs(int hours, int minutes, int seconds) {
        return TimeUnit.HOURS.toNanos(hours) + TimeUnit.MINUTES.toNanos(minutes) + TimeUnit.SECONDS.toNanos(seconds);
    }

    private void testDynamicWith(String withWhat, Object[] values, String[] expectedStrValues) throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        String table = "test_dynamic_with_" + withWhat;
        client.execute("DROP TABLE IF EXISTS " + table).get();

        CommandSettings createTableSettings = (CommandSettings) new CommandSettings().serverSetting("allow_experimental_dynamic_type", "1");
        if (isVersionMatch("[25.6,)")) {
            createTableSettings.serverSetting("allow_experimental_time_time64_type", "1"); // time64 was introduced in 25.6
        }
        client.execute(tableDefinition(table, "rowId Int32", "field Dynamic"),createTableSettings).get();

        client.register(DTOForDynamicPrimitivesTests.class, client.getTableSchema(table));

        List<DTOForDynamicPrimitivesTests> data = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            data.add(new DTOForDynamicPrimitivesTests(i, values[i]));
        }
        client.insert(table, data).get().close();

        List<GenericRecord> rows = client.queryAll("SELECT * FROM " + table);
        for (GenericRecord row : rows) {
            Assert.assertEquals(row.getString("field"), expectedStrValues[row.getInteger("rowId")]);
        }
    }

    private void testVariantWith(String withWhat, String[] fields, Object[] values, String[] expectedStrValues) throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        String table = "test_variant_with_" + withWhat;
        String[] actualFields = new String[fields.length + 1];
        actualFields[0] = "rowId Int32";
        System.arraycopy(fields, 0, actualFields, 1, fields.length);
        client.execute("DROP TABLE IF EXISTS " + table).get();


        CommandSettings createTableSettings = (CommandSettings) new CommandSettings().serverSetting("allow_experimental_variant_type", "1");
        if (isVersionMatch("[25.6,)")) {
            createTableSettings.serverSetting("allow_experimental_time_time64_type", "1"); // time64 was introduced in 25.6
        }
        client.execute(tableDefinition(table, actualFields),createTableSettings).get();

        client.register(DTOForVariantPrimitivesTests.class, client.getTableSchema(table));

        List<DTOForVariantPrimitivesTests> data = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            data.add(new DTOForVariantPrimitivesTests(i, values[i]));
        }
        client.insert(table, data).get().close();

        List<GenericRecord> rows = client.queryAll("SELECT * FROM " + table);
        for (GenericRecord row : rows) {
            Assert.assertEquals(row.getString("field"), expectedStrValues[row.getInteger("rowId")]);
        }
    }

    @Test(groups = {"integration"}, dataProvider = "testJSONBinaryFormat_dp")
    public void testJSONBinaryFormat(String jsonDef) throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        final String table = "test_json_binary_format";
        final String jsonCol = "value " + jsonDef;
        final String jsonValue = "{\"count\": 1000, \"stat\": {\"float\": 0.999, \"name\": \"temp\" }}";

        client.execute("DROP TABLE IF EXISTS " + table).get().close();
        client.execute(tableDefinition(table, jsonCol),
                (CommandSettings) new CommandSettings()
                        .serverSetting("enable_json_type", "1")
                        .serverSetting("allow_experimental_json_type", "1")).get().close();
        client.execute("INSERT INTO " + table + " VALUES (" + SQLUtils.enquoteLiteral(jsonValue) + ")").get().close();

        try (QueryResponse  queryResponse = client.query("SELECT * FROM " + table + " LIMIT 1").get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(queryResponse);
            Map<String, Object> row = reader.next();
            Object value = row.get("value");
            Assert.assertNotNull(value);
        }
    }

    @DataProvider
    public Object[][] testJSONBinaryFormat_dp() {

        return new Object[][] {
                {"JSON"},
                {"JSON()"},
                {"JSON(stat.name String, count Int32)"},
                {"JSON(stat.name String, `comments` String)"},
                {"JSON(max_dynamic_paths=3, stat.name String, SKIP alt_count)"},
                {"JSON(max_dynamic_paths=3, stat.name String, SKIP REGEXP '^-.*')"},
                {"JSON(max_dynamic_paths=3,SKIP REGEXP '^-.*',SKIP ff,   flags Array(Array(Array(Int8))), SKIP alt_count)"},
        };
    }

    @Test(groups = {"integration"}, dataProvider = "testDataTypesAsStringDP")
    public void testDataTypesAsString(String sql, String[] expectedStrValues) throws Exception {

        try (QueryResponse resp = client.query(sql).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(resp);
            reader.next();
            for (int i = 0; i < expectedStrValues.length; i++) {
                Assert.assertEquals(reader.getString(i + 1), expectedStrValues[i]);
            }
        }
    }

    @DataProvider
    public static Object[][] testDataTypesAsStringDP() {
        return new Object[][] {
                {"SELECT '192.168.1.1'::IPv4, '2001:db8::1'::IPv6, '192.168.1.1'::IPv6",
                    new String[] {"192.168.1.1", "2001:db8:0:0:0:0:0:1", "192.168.1.1"}},
                {"SELECT '2024-10-04'::Date32, '2024-10-04 12:34:56'::DateTime32, '2024-10-04 12:34:56.789'::DateTime64(3), " +
                        " '2024-10-04 12:34:56.789012'::DateTime64(6), '2024-10-04 12:34:56.789012345'::DateTime64(9)",
                    new String[] {"2024-10-04", "2024-10-04 12:34:56", "2024-10-04 12:34:56.789", "2024-10-04 12:34:56.789012",
                            "2024-10-04 12:34:56.789012345"}},
                {"SELECT 1::Enum16('one' = 1, 'two' = 2)", "one"},
                {"SELECT 2::Enum8('one' = 1, 'two' = 2)", "two"},
                {"SELECT 3::Enum('one' = 1, 'two' = 2, 'three' = 3)", "three"},
        };
    }

    public static String tableDefinition(String table, String... columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE " + table + " ( ");
        Arrays.stream(columns).forEach(s -> {
            sb.append(s).append(", ");
        });
        sb.setLength(sb.length() - 2);
        sb.append(") Engine = MergeTree ORDER BY ()");
        return sb.toString();
    }

    private boolean isVersionMatch(String versionExpression) {
        List<GenericRecord> serverVersion = client.queryAll("SELECT version()");
        return ClickHouseVersion.of(serverVersion.get(0).getString(1)).check(versionExpression);
    }
}
