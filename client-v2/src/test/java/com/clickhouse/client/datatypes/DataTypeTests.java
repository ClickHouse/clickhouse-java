package com.clickhouse.client.datatypes;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
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
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
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
                case QBit:
                case Geometry:
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

        testVariantWith("arrays", new String[]{"field Array(Variant(String, Int32))"},
                new Object[]{
                        new Object[]{1, 2},
                        new Object[]{"a", 3},
                        new Object[]{3, "d"}
                },
                new String[]{
                        "[1, 2]",
                        "[a, 3]",
                        "[3, d]",
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

        LocalDateTime epochZero = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);

        testVariantWith("Time", new String[]{"field Variant(Time, String)"},
                new Object[]{
                        "30:33:30",
                        TimeUnit.HOURS.toSeconds(100) + TimeUnit.MINUTES.toSeconds(10) + 30
                },
                new String[]{
                        "30:33:30",
                        epochZero.plusHours(100).plusMinutes(10).plusSeconds(30).toString()
                });

        testVariantWith("Time64", new String[]{"field Variant(Time64(0), String)"},
                new Object[]{
                        "30:33:30",
                        TimeUnit.HOURS.toSeconds(100) + TimeUnit.MINUTES.toSeconds(10) + 30
                },
                new String[]{
                        "30:33:30",
                        epochZero.plusHours(100).plusMinutes(10).plusSeconds(30).toString()
                });

        testVariantWith("Time64", new String[]{"field Variant(Time64, String)"},
                new Object[]{
                        "30:33:30",
                        TimeUnit.HOURS.toMillis(100) + TimeUnit.MINUTES.toMillis(10) + TimeUnit.SECONDS.toMillis(30)
                },
                new String[]{
                        "30:33:30",
                        epochZero.plusHours(100).plusMinutes(10).plusSeconds(30).toString()
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
                case QBit:
                case Geometry:
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
                        if (tmpDec.divide((BigDecimal)value, RoundingMode.UNNECESSARY).equals(BigDecimal.ONE)) {
                            continue;
                        }
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

        Instant time64 = Instant.ofEpochSecond(TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59);
        long time64Value = time64.getEpochSecond() * 1_000_000_000 + time64.getNano();
        testDynamicWith("Time64",
                new Object[]{
                        time64Value
                },
                new String[]{
                        String.valueOf(time64Value)
                }
        );

        Instant maxTime64 = Instant.ofEpochSecond(TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59,
                123456789);
        testDynamicWith("Time64",
                new Object[]{
                        maxTime64,
                },
                new String[]{
                        LocalDateTime.ofInstant(maxTime64, ZoneId.of("UTC")).toString()
                });
    }

    @Test(groups = {"integration"})
    public void testDynamicWithNestedTypes() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        List<GenericRecord> records = client.queryAll("SELECT (1, 'row1', 0.1)::Tuple(rowId Int32, name String, value Float64)::Dynamic AS row, 10::Int32 AS num");

        Object[] tuple = (Object[]) records.get(0).getObject("row");
        Assert.assertEquals(tuple[0], 1);
        Assert.assertEquals(tuple[1], "row1");
        Assert.assertEquals(tuple[2], 0.1);
        Assert.assertEquals(records.get(0).getInteger("num"), 10);
    }

    @Test(groups = {"integration"})
    public void testDynamicWithFixedString() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }
        List<GenericRecord> records = client.queryAll("SELECT 'row1'::FixedString(4)::Dynamic AS str, 10::Int32 AS num");
        Assert.assertEquals("row1", records.get(0).getString("str"));
        Assert.assertEquals(records.get(0).getInteger("num"), 10); // added to check if reading further is not affected
    }

    @Test(groups = {"integration"}, dataProvider = "testDynamicWithJSON_dp")
    public void testDynamicWithJSON(String type, String json, Object expected) throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }
        List<GenericRecord> records = client.queryAll("SELECT '" + json + "'::" + type + "::Dynamic AS val");
        GenericRecord row = records.get(0);
        Object val = row.getObject("val");
        Assert.assertEquals(val, expected);
    }

    @DataProvider
    public Object[][] testDynamicWithJSON_dp() {
        Map<String, Object> map1 = new HashMap<>();
        map1.put("name", "row1");
        map1.put("value", 0.1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("name", "row1");
        map2.put("value", 0.1f);
        Map<String, Object> map3 = new HashMap<>();
        map3.put("a.b", "c");
        map3.put("a.d", "e");
        Map<String, Object> map4 = new HashMap<>();
        map4.put("a.d", "e");

        return new Object[][] {
                { "JSON(max_dynamic_paths=100, max_dynamic_types=100)", "{\"name\": \"row1\", \"value\": 0.1}", map1},
                { "JSON(value Float32)", "{\"name\": \"row1\", \"value\": 0.1}", map2},
                { "JSON", "{ \"a\" :  { \"b\" : \"c\", \"d\" : \"e\" } }", map3},
                { "JSON(SKIP a.b)", "{ \"a\" :  { \"b\" : \"c\", \"d\" : \"e\" } }", map4},
                { "JSON(SKIP REGEXP \'a\\.b\')", "{ \"a\" :  { \"b\" : \"c\", \"d\" : \"e\" } }", map4},

        };
    }

    @Test(groups = {"integration"})
    public void testDynamicWithJSONWithArrays() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        String json = "{ \"array\": [ {\"a\": 100 }, {\"b\": \"name\"}]}";
        String type = "JSON(max_dynamic_paths=100, max_dynamic_types=100)";
        List<GenericRecord> records = client.queryAll("SELECT '" + json + "'::" + type + "::Dynamic AS val");
        GenericRecord row = records.get(0);
        HashMap<String, Object> val = (HashMap<String, Object>) row.getObject("val");
        BinaryStreamReader.ArrayValue array = (BinaryStreamReader.ArrayValue) val.get("array");
        List<HashMap<String, Object>> items = array.asList();

        Assert.assertEquals(items.size(), 2);
        Assert.assertEquals(items.get(0).get("a"), 100L);
        Assert.assertEquals(items.get(1).get("b"), "name");

    }

    @Test(groups = {"integration"})
    public void testDynamicWithVariant() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        List<GenericRecord> records = client.queryAll("select arrayJoin([1, 'a', 3]::Array(Variant(String, Int32)))::Dynamic as val");

        GenericRecord row = records.get(0);
        Object val = row.getObject("val");
        Assert.assertEquals(val, 1);

        row = records.get(1);
        val = row.getObject("val");
        Assert.assertEquals(val, "a");

        row = records.get(2);
        val = row.getObject("val");
        Assert.assertEquals(val, 3);
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
        Assert.assertEquals(record.getLocalDateTime("time").toEpochSecond(ZoneOffset.UTC), TimeUnit.HOURS.toSeconds(999));
        Assert.assertEquals(record.getInstant("time"), Instant.ofEpochSecond(TimeUnit.HOURS.toSeconds(999)));

        record = records.get(1);
        Assert.assertEquals(record.getInteger("o_num"), 2);
        Assert.assertEquals(record.getLocalDateTime("time").toEpochSecond(ZoneOffset.UTC), TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59);
        Assert.assertEquals(record.getInstant("time"), Instant.ofEpochSecond(TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59));

        record = records.get(2);
        Assert.assertEquals(record.getInteger("o_num"), 3);
        Assert.assertEquals(record.getLocalDateTime("time").toEpochSecond(ZoneOffset.UTC), 0);
        Assert.assertEquals(record.getInstant("time"), Instant.ofEpochSecond(0));

        record = records.get(3);
        Assert.assertEquals(record.getInteger("o_num"), 4);
        Assert.assertEquals(record.getLocalDateTime("time").toEpochSecond(ZoneOffset.UTC), - (TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59));
        Assert.assertEquals(record.getInstant("time"), Instant.ofEpochSecond(-
                (TimeUnit.HOURS.toSeconds(999) + TimeUnit.MINUTES.toSeconds(59) + 59)));
    }

    @Test(groups = {"integration"}, dataProvider = "testTimeData")
    public void testTime(String column, String value, LocalDateTime expectedDt) throws Exception {
        if (isVersionMatch("(,25.5]")) {
            return; // time64 was introduced in 25.6
        }

        QuerySettings settings = new QuerySettings().serverSetting("allow_experimental_time_time64_type", "1");
        List<GenericRecord> records = client.queryAll("SELECT \'" + value + "\'::" + column, settings);
        LocalDateTime dt = records.get(0).getLocalDateTime(1);
        Assert.assertEquals(dt, expectedDt);
    }

    @DataProvider
    public static Object[][] testTimeData() {

        return new Object[][] {
                {"Time64", "00:01:00.123", LocalDateTime.parse("1970-01-01T00:01:00.123")},
                {"Time64(3)","00:01:00.123", LocalDateTime.parse("1970-01-01T00:01:00.123")},
                {"Time64(6)","00:01:00.123456", LocalDateTime.parse("1970-01-01T00:01:00.123456")},
                {"Time64(9)","00:01:00.123456789", LocalDateTime.parse("1970-01-01T00:01:00.123456789")},
                {"Time64","-00:01:00.123", LocalDateTime.parse("1969-12-31T23:59:00.123")},
                {"Time64(3)","-00:01:00.123", LocalDateTime.parse("1969-12-31T23:59:00.123")},
                {"Time64(6)","-00:01:00.123456", LocalDateTime.parse("1969-12-31T23:59:00.123456")},
                {"Time64(9)","-00:01:00.123456789", LocalDateTime.parse("1969-12-31T23:59:00.123456789")},
                {"Time64","-999:59:59.999", LocalDateTime.parse("1969-11-20T08:00:01.999")},
                {"Time64(3)","-999:59:59.999", LocalDateTime.parse("1969-11-20T08:00:01.999")},
                {"Time64(6)","-999:59:59.999999", LocalDateTime.parse("1969-11-20T08:00:01.999999")},
                {"Time64(9)","-999:59:59.999999999", LocalDateTime.parse("1969-11-20T08:00:01.999999999")},
                {"Time64","999:59:59.999", LocalDateTime.parse("1970-02-11T15:59:59.999")},
                {"Time64(3)","999:59:59.999", LocalDateTime.parse("1970-02-11T15:59:59.999")},
                {"Time64(6)","999:59:59.999999", LocalDateTime.parse("1970-02-11T15:59:59.999999")},
                {"Time64(9)","999:59:59.999999999", LocalDateTime.parse("1970-02-11T15:59:59.999999999")},
        };
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

    @Test(groups = {"integration"})
    public void testDates() throws Exception {
        LocalDate date = LocalDate.of(2024, 1, 15);

        String sql = "SELECT toDate('" + date + "') AS d, toDate32('" + date + "') AS d32";
        ZoneId laZone = ZoneId.of("America/Los_Angeles");
        ZoneId tokyoZone = ZoneId.of("Asia/Tokyo");
        ZoneId utcZone = ZoneId.of("UTC");

        // Los Angeles
        LocalDate laDate;
        LocalDate laDate32;
        QuerySettings laSettings = new QuerySettings()
                .setUseServerTimeZone(true)
                .serverSetting("session_timezone", laZone.getId());
        try (QueryResponse response = client.query(sql, laSettings).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            reader.next();

            laDate = reader.getLocalDate("d");
            laDate32 = reader.getLocalDate("d32");

            Assert.assertThrows(ClientException.class, () -> reader.getZonedDateTime("d"));
            Assert.assertThrows(ClientException.class, () -> reader.getZonedDateTime("d32"));
            Assert.assertThrows(ClientException.class, () -> reader.getLocalDateTime("d"));
            Assert.assertThrows(ClientException.class, () -> reader.getLocalDateTime("d32"));
            Assert.assertThrows(ClientException.class, () -> reader.getOffsetDateTime("d"));
            Assert.assertThrows(ClientException.class, () -> reader.getOffsetDateTime("d32"));

        }

        // Tokyo
        LocalDate tokyoDate;
        LocalDate tokyoDate32;
        QuerySettings tokyoSettings = new QuerySettings()
                .setUseServerTimeZone(true)
                .serverSetting("session_timezone", tokyoZone.getId());
        try (QueryResponse response = client.query(sql, tokyoSettings).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            reader.next();
            tokyoDate = reader.getLocalDate("d");
            tokyoDate32 = reader.getLocalDate("d32");
        }


        // Check dates - should be equal
        Assert.assertEquals(laDate, date);
        Assert.assertEquals(laDate32, date);
        Assert.assertEquals(tokyoDate, date);
        Assert.assertEquals(tokyoDate32, date);


        // Verify with session time differ from use timezone - no effect on date
        try (Client tzClient = newClient()
                .useTimeZone(utcZone.getId())
                .build()) {
            QuerySettings tzSettings = new QuerySettings()
                    .serverSetting("session_timezone", laZone.getId());
            try (QueryResponse response = tzClient.query(sql, tzSettings).get()) {
                ClickHouseBinaryFormatReader reader = tzClient.newBinaryFormatReader(response);
                reader.next();
                Assert.assertEquals(reader.getLocalDate("d"), date);
                Assert.assertEquals(reader.getLocalDate("d32"), date);
            }

            LocalDate minDate = LocalDate.of(1970, 1, 1);
            LocalDate maxDate = LocalDate.of(2149, 6, 6);
            LocalDate minDate32 = LocalDate.of(1900, 1, 1);
            LocalDate maxDate32 = LocalDate.of(2299, 12, 31);
            String extremesSql = "SELECT toDate('" + minDate + "') AS d_min, toDate('" + maxDate + "') AS d_max, "
                    + "toDate32('" + minDate32 + "') AS d32_min, toDate32('" + maxDate32 + "') AS d32_max";
            try (QueryResponse response = tzClient.query(extremesSql, tzSettings).get()) {
                ClickHouseBinaryFormatReader reader = tzClient.newBinaryFormatReader(response);
                reader.next();
                Assert.assertEquals(reader.getLocalDate("d_min"), minDate);
                Assert.assertEquals(reader.getLocalDate("d_max"), maxDate);
                Assert.assertEquals(reader.getLocalDate("d32_min"), minDate32);
                Assert.assertEquals(reader.getLocalDate("d32_max"), maxDate32);
            }
        }
    }

    @Test(groups = {"integration"}, dataProvider = "testNestedArrays_dp")
    public void testNestedArrays(String columnDef, String insertValues, String[] expectedStrValues, 
                                 String[] expectedListValues) throws Exception {
        final String table = "test_nested_arrays";
        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute(tableDefinition(table, "rowId Int32", columnDef)).get();

        client.execute("INSERT INTO " + table + " VALUES " + insertValues).get().close();

        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table + " ORDER BY rowId");
        Assert.assertEquals(records.size(), expectedStrValues.length);

        for (GenericRecord record : records) {
            int rowId = record.getInteger("rowId");
            
            // Check getString() - includes quotes for string values
            String actualValue = record.getString("arr");
            Assert.assertEquals(actualValue, expectedStrValues[rowId - 1], 
                    "getString() mismatch at row " + rowId + " for " + columnDef);
            
            // Check getObject() - should return an ArrayValue
            Object objValue = record.getObject("arr");
            Assert.assertNotNull(objValue, "getObject() returned null at row " + rowId);
            Assert.assertTrue(objValue instanceof BinaryStreamReader.ArrayValue,
                    "getObject() should return ArrayValue at row " + rowId + ", got: " + objValue.getClass().getName());
            BinaryStreamReader.ArrayValue arrayValue = (BinaryStreamReader.ArrayValue) objValue;
            Assert.assertEquals(arrayValue.asList().toString(), expectedListValues[rowId - 1],
                    "getObject().asList() mismatch at row " + rowId + " for " + columnDef);
            
            // Check getList() - should return a List representation (no quotes for strings)
            List<?> listValue = record.getList("arr");
            Assert.assertNotNull(listValue, "getList() returned null at row " + rowId);
            Assert.assertEquals(listValue.toString(), expectedListValues[rowId - 1],
                    "getList() mismatch at row " + rowId + " for " + columnDef);
        }
    }

    @DataProvider
    public Object[][] testNestedArrays_dp() {
        return new Object[][] {
                // 2D arrays of integers - Array(Array(Int32))
                {
                        "arr Array(Array(Int32))",
                        "(1, [[1, 2], [3, 4]]), (2, [[5, 6, 7]]), (3, [[]]), (4, [[8], [], [9, 10]]), " +
                        "(5, [[11]]), (6, [[12, 13], [14, 15]]), (7, [[100, 200]]), (8, [[16]]), (9, [[17, 18]]), (10, [[19, 20, 21]])",
                        new String[] {
                                "[[1, 2], [3, 4]]", "[[5, 6, 7]]", "[[]]", "[[8], [], [9, 10]]",
                                "[[11]]", "[[12, 13], [14, 15]]", "[[100, 200]]", "[[16]]", "[[17, 18]]", "[[19, 20, 21]]"
                        },
                        new String[] {
                                "[[1, 2], [3, 4]]", "[[5, 6, 7]]", "[[]]", "[[8], [], [9, 10]]",
                                "[[11]]", "[[12, 13], [14, 15]]", "[[100, 200]]", "[[16]]", "[[17, 18]]", "[[19, 20, 21]]"
                        }
                },
                // 2D arrays of strings - Array(Array(String))
                {
                        "arr Array(Array(String))",
                        "(1, [['a', 'b'], ['c', 'd']]), (2, [['hello', 'world']]), (3, [[]]), (4, [['x'], [], ['y', 'z']]), " +
                        "(5, [['test']]), (6, [['foo', 'bar']]), (7, [['single']]), (8, [['alpha', 'beta']]), (9, [['one']]), (10, [['end']])",
                        new String[] {  // getString() format - with quotes
                                "[['a', 'b'], ['c', 'd']]", "[['hello', 'world']]", "[[]]", "[['x'], [], ['y', 'z']]",
                                "[['test']]", "[['foo', 'bar']]", "[['single']]", "[['alpha', 'beta']]", "[['one']]", "[['end']]"
                        },
                        new String[] {  // getList() format - no quotes
                                "[[a, b], [c, d]]", "[[hello, world]]", "[[]]", "[[x], [], [y, z]]",
                                "[[test]]", "[[foo, bar]]", "[[single]]", "[[alpha, beta]]", "[[one]]", "[[end]]"
                        }
                },
                // 3D arrays of integers - Array(Array(Array(Int32)))
                {
                        "arr Array(Array(Array(Int32)))",
                        "(1, [[[1, 2], [3]]]), (2, [[[4], [5, 6]], [[7]]]), (3, [[[]]]), (4, [[[8, 9]]]), " +
                        "(5, [[[10], [11, 12]]]), (6, [[[13]]]), (7, [[[14, 15], [16]]]), (8, [[[17]]]), (9, [[[18, 19]]]), (10, [[[]]])",
                        new String[] {
                                "[[[1, 2], [3]]]", "[[[4], [5, 6]], [[7]]]", "[[[]]]", "[[[8, 9]]]",
                                "[[[10], [11, 12]]]", "[[[13]]]", "[[[14, 15], [16]]]", "[[[17]]]", "[[[18, 19]]]", "[[[]]]"
                        },
                        new String[] {
                                "[[[1, 2], [3]]]", "[[[4], [5, 6]], [[7]]]", "[[[]]]", "[[[8, 9]]]",
                                "[[[10], [11, 12]]]", "[[[13]]]", "[[[14, 15], [16]]]", "[[[17]]]", "[[[18, 19]]]", "[[[]]]"
                        }
                },
                // 2D arrays of floats - Array(Array(Float64))
                {
                        "arr Array(Array(Float64))",
                        "(1, [[1.1, 2.2], [3.3]]), (2, [[4.4]]), (3, [[5.5, 6.6, 7.7]]), (4, [[]]), " +
                        "(5, [[8.8]]), (6, [[9.9, 10.1]]), (7, [[11.2]]), (8, [[12.3, 13.4]]), (9, [[14.5]]), (10, [[15.6, 16.7]])",
                        new String[] {
                                "[[1.1, 2.2], [3.3]]", "[[4.4]]", "[[5.5, 6.6, 7.7]]", "[[]]",
                                "[[8.8]]", "[[9.9, 10.1]]", "[[11.2]]", "[[12.3, 13.4]]", "[[14.5]]", "[[15.6, 16.7]]"
                        },
                        new String[] {
                                "[[1.1, 2.2], [3.3]]", "[[4.4]]", "[[5.5, 6.6, 7.7]]", "[[]]",
                                "[[8.8]]", "[[9.9, 10.1]]", "[[11.2]]", "[[12.3, 13.4]]", "[[14.5]]", "[[15.6, 16.7]]"
                        }
                },
                // 3D arrays of strings - Array(Array(Array(String)))
                {
                        "arr Array(Array(Array(String)))",
                        "(1, [[['a', 'b']]]), (2, [[['c'], ['d', 'e']]]), (3, [[[]]]), (4, [[['f']]]), " +
                        "(5, [[['g', 'h']]]), (6, [[['i']]]), (7, [[['a', 'b'], ['c']], [['d', 'e', 'f']]]), (8, [[[]]]), (9, [[['m']]]), (10, [[['n', 'o']]])",
                        new String[] {  // getString() format - with quotes
                                "[[['a', 'b']]]", "[[['c'], ['d', 'e']]]", "[[[]]]", "[[['f']]]",
                                "[[['g', 'h']]]", "[[['i']]]", "[[['a', 'b'], ['c']], [['d', 'e', 'f']]]", "[[[]]]", "[[['m']]]", "[[['n', 'o']]]"
                        },
                        new String[] {  // getList() format - no quotes
                                "[[[a, b]]]", "[[[c], [d, e]]]", "[[[]]]", "[[[f]]]",
                                "[[[g, h]]]", "[[[i]]]", "[[[a, b], [c]], [[d, e, f]]]", "[[[]]]", "[[[m]]]", "[[[n, o]]]"
                        }
                },
                // 2D arrays of nullable integers - Array(Array(Nullable(Int32)))
                {
                        "arr Array(Array(Nullable(Int32)))",
                        "(1, [[1, NULL, 2]]), (2, [[NULL]]), (3, [[3, 4, NULL]]), (4, [[]]), " +
                        "(5, [[NULL, NULL]]), (6, [[5]]), (7, [[6, NULL]]), (8, [[NULL, 7]]), (9, [[8, 9]]), (10, [[NULL]])",
                        new String[] {
                                "[[1, NULL, 2]]", "[[NULL]]", "[[3, 4, NULL]]", "[[]]",
                                "[[NULL, NULL]]", "[[5]]", "[[6, NULL]]", "[[NULL, 7]]", "[[8, 9]]", "[[NULL]]"
                        },
                        new String[] {
                                "[[1, null, 2]]", "[[null]]", "[[3, 4, null]]", "[[]]",
                                "[[null, null]]", "[[5]]", "[[6, null]]", "[[null, 7]]", "[[8, 9]]", "[[null]]"
                        }
                },
                // 4D arrays of integers - Array(Array(Array(Array(Int32))))
                {
                        "arr Array(Array(Array(Array(Int32))))",
                        "(1, [[[[1, 2]]]]), (2, [[[[3], [4, 5]]]]), (3, [[[[]]]]), (4, [[[[6]]]]), " +
                        "(5, [[[[7, 8]]]]), (6, [[[[9]]]]), (7, [[[[10, 11]]]]), (8, [[[[]]]]), (9, [[[[12]]]]), (10, [[[[13, 14]]]])",
                        new String[] {
                                "[[[[1, 2]]]]", "[[[[3], [4, 5]]]]", "[[[[]]]]", "[[[[6]]]]",
                                "[[[[7, 8]]]]", "[[[[9]]]]", "[[[[10, 11]]]]", "[[[[]]]]", "[[[[12]]]]", "[[[[13, 14]]]]"
                        },
                        new String[] {
                                "[[[[1, 2]]]]", "[[[[3], [4, 5]]]]", "[[[[]]]]", "[[[[6]]]]",
                                "[[[[7, 8]]]]", "[[[[9]]]]", "[[[[10, 11]]]]", "[[[[]]]]", "[[[[12]]]]", "[[[[13, 14]]]]"
                        }
                }
        };
    }

    @Test(groups = {"integration"})
    public void testGetObjectArrayMethods() throws Exception {
        final String table = "test_get_object_array_methods";
        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute("CREATE TABLE " + table + " (" +
                "rowId Int32, " +
                "uint64_arr Array(UInt64), " +
                "enum_arr Array(Enum8('abc' = 1, 'cde' = 2, 'xyz' = 3)), " +
                "dt_arr Array(DateTime('UTC')), " +
                "fstr_arr Array(FixedString(4)), " +
                "str_arr Array(String), " +
                "int_arr Array(Int32), " +
                "arr2d Array(Array(Int64)), " +
                "arr3d Array(Array(Array(Int32)))" +
                ") Engine = MergeTree ORDER BY rowId").get();

        client.execute("INSERT INTO " + table + " VALUES " +
                "(1, " +
                "[100, 200, 18000044073709551615], " +
                "['abc', 'cde'], " +
                "['2030-10-09 08:07:06', '2031-10-09 08:07:06'], " +
                "['abcd', 'efgh'], " +
                "['hello', 'world'], " +
                "[10, 20, 30], " +
                "[[1, 2, 3], [4, 5]], " +
                "[[[1, 2], [3]], [[4, 5, 6]]]" +
                "), " +
                "(2, " +
                "[], " +
                "['xyz'], " +
                "[], " +
                "[], " +
                "[], " +
                "[], " +
                "[[]], " +
                "[[[]]]" +
                ")").get();

        List<GenericRecord> records = client.queryAll("SELECT * FROM " + table + " ORDER BY rowId");
        Assert.assertEquals(records.size(), 2);

        // --- Row 1: non-empty arrays ---
        GenericRecord row1 = records.get(0);
        Assert.assertEquals(row1.getInteger("rowId"), 1);

        // Array(UInt64) -> getObjectArray returns BigInteger[]
        Object[] uint64Arr = row1.getObjectArray("uint64_arr");
        Assert.assertNotNull(uint64Arr);
        Assert.assertEquals(uint64Arr.length, 3);
        Assert.assertTrue(uint64Arr[0] instanceof java.math.BigInteger);
        Assert.assertEquals(uint64Arr[0], java.math.BigInteger.valueOf(100));
        Assert.assertEquals(uint64Arr[1], java.math.BigInteger.valueOf(200));
        Assert.assertEquals(uint64Arr[2], new java.math.BigInteger("18000044073709551615"));

        // Array(Enum8) -> getObjectArray returns EnumValue[]
        Object[] enumArr = row1.getObjectArray("enum_arr");
        Assert.assertNotNull(enumArr);
        Assert.assertEquals(enumArr.length, 2);
        Assert.assertTrue(enumArr[0] instanceof BinaryStreamReader.EnumValue);
        Assert.assertEquals(enumArr[0].toString(), "abc");
        Assert.assertEquals(enumArr[1].toString(), "cde");

        // Array(DateTime) -> getObjectArray returns ZonedDateTime[]
        Object[] dtArr = row1.getObjectArray("dt_arr");
        Assert.assertNotNull(dtArr);
        Assert.assertEquals(dtArr.length, 2);
        Assert.assertTrue(dtArr[0] instanceof java.time.ZonedDateTime);
        java.time.ZonedDateTime zdt1 = (java.time.ZonedDateTime) dtArr[0];
        java.time.ZonedDateTime zdt2 = (java.time.ZonedDateTime) dtArr[1];
        Assert.assertEquals(zdt1.getYear(), 2030);
        Assert.assertEquals(zdt1.getMonthValue(), 10);
        Assert.assertEquals(zdt1.getDayOfMonth(), 9);
        Assert.assertEquals(zdt1.getHour(), 8);
        Assert.assertEquals(zdt1.getMinute(), 7);
        Assert.assertEquals(zdt1.getSecond(), 6);
        Assert.assertEquals(zdt2.getYear(), 2031);

        // Array(FixedString) -> getObjectArray returns String[]
        Object[] fstrArr = row1.getObjectArray("fstr_arr");
        Assert.assertNotNull(fstrArr);
        Assert.assertEquals(fstrArr.length, 2);
        Assert.assertEquals(fstrArr[0], "abcd");
        Assert.assertEquals(fstrArr[1], "efgh");

        // Array(FixedString) -> getStringArray
        String[] fstrStrings = row1.getStringArray("fstr_arr");
        Assert.assertEquals(fstrStrings, new String[]{"abcd", "efgh"});

        // Array(String) -> getObjectArray returns String[]
        Object[] strArr = row1.getObjectArray("str_arr");
        Assert.assertNotNull(strArr);
        Assert.assertEquals(strArr[0], "hello");
        Assert.assertEquals(strArr[1], "world");

        // Array(Int32) -> getObjectArray returns boxed Integer[]
        Object[] intArr = row1.getObjectArray("int_arr");
        Assert.assertNotNull(intArr);
        Assert.assertEquals(intArr.length, 3);
        Assert.assertEquals(intArr[0], 10);
        Assert.assertEquals(intArr[1], 20);
        Assert.assertEquals(intArr[2], 30);

        // Array(Array(Int64)) 2D -> getObjectArray returns nested Object[]
        Object[] arr2d = row1.getObjectArray("arr2d");
        Assert.assertNotNull(arr2d);
        Assert.assertEquals(arr2d.length, 2);
        Assert.assertTrue(arr2d[0] instanceof Object[]);
        Assert.assertTrue(arr2d[1] instanceof Object[]);
        Object[] inner2d_0 = (Object[]) arr2d[0];
        Assert.assertEquals(inner2d_0.length, 3);
        Assert.assertEquals(inner2d_0[0], 1L);
        Assert.assertEquals(inner2d_0[1], 2L);
        Assert.assertEquals(inner2d_0[2], 3L);
        Object[] inner2d_1 = (Object[]) arr2d[1];
        Assert.assertEquals(inner2d_1.length, 2);
        Assert.assertEquals(inner2d_1[0], 4L);
        Assert.assertEquals(inner2d_1[1], 5L);

        // Array(Array(Array(Int32))) 3D -> getObjectArray returns 3-level nested Object[]
        Object[] arr3d = row1.getObjectArray("arr3d");
        Assert.assertNotNull(arr3d);
        Assert.assertEquals(arr3d.length, 2);

        // [[[1, 2], [3]], [[4, 5, 6]]]
        Object[] dim1_0 = (Object[]) arr3d[0];
        Assert.assertEquals(dim1_0.length, 2);
        Object[] dim2_0_0 = (Object[]) dim1_0[0];
        Assert.assertEquals(dim2_0_0.length, 2);
        Assert.assertEquals(dim2_0_0[0], 1);
        Assert.assertEquals(dim2_0_0[1], 2);
        Object[] dim2_0_1 = (Object[]) dim1_0[1];
        Assert.assertEquals(dim2_0_1.length, 1);
        Assert.assertEquals(dim2_0_1[0], 3);

        Object[] dim1_1 = (Object[]) arr3d[1];
        Assert.assertEquals(dim1_1.length, 1);
        Object[] dim2_1_0 = (Object[]) dim1_1[0];
        Assert.assertEquals(dim2_1_0.length, 3);
        Assert.assertEquals(dim2_1_0[0], 4);
        Assert.assertEquals(dim2_1_0[1], 5);
        Assert.assertEquals(dim2_1_0[2], 6);

        // --- Row 2: edge cases (empty arrays, single elements) ---
        GenericRecord row2 = records.get(1);
        Assert.assertEquals(row2.getInteger("rowId"), 2);

        // Empty arrays
        Object[] emptyUint64 = row2.getObjectArray("uint64_arr");
        Assert.assertNotNull(emptyUint64);
        Assert.assertEquals(emptyUint64.length, 0);

        Object[] emptyDt = row2.getObjectArray("dt_arr");
        Assert.assertNotNull(emptyDt);
        Assert.assertEquals(emptyDt.length, 0);

        Object[] emptyStr = row2.getObjectArray("str_arr");
        Assert.assertNotNull(emptyStr);
        Assert.assertEquals(emptyStr.length, 0);

        Object[] emptyInt = row2.getObjectArray("int_arr");
        Assert.assertNotNull(emptyInt);
        Assert.assertEquals(emptyInt.length, 0);

        // Single-element enum array
        Object[] singleEnum = row2.getObjectArray("enum_arr");
        Assert.assertEquals(singleEnum.length, 1);
        Assert.assertEquals(singleEnum[0].toString(), "xyz");

        String[] singleEnumStr = row2.getStringArray("enum_arr");
        Assert.assertEquals(singleEnumStr, new String[]{"xyz"});

        // 2D with inner empty: [[]]
        Object[] arr2dEmpty = row2.getObjectArray("arr2d");
        Assert.assertNotNull(arr2dEmpty);
        Assert.assertEquals(arr2dEmpty.length, 1);
        Assert.assertTrue(arr2dEmpty[0] instanceof Object[]);
        Assert.assertEquals(((Object[]) arr2dEmpty[0]).length, 0);

        // 3D with inner empty: [[[]]]
        Object[] arr3dEmpty = row2.getObjectArray("arr3d");
        Assert.assertNotNull(arr3dEmpty);
        Assert.assertEquals(arr3dEmpty.length, 1);
        Object[] arr3dInner = (Object[]) arr3dEmpty[0];
        Assert.assertEquals(arr3dInner.length, 1);
        Assert.assertTrue(arr3dInner[0] instanceof Object[]);
        Assert.assertEquals(((Object[]) arr3dInner[0]).length, 0);
    }

    @Test(groups = {"integration"})
    public void testGetStringArrayAndGetObjectArrayWhenValueIsList() throws Exception {
        final String table = "test_get_string_array_and_object_array_when_value_is_list";
        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute("CREATE TABLE " + table + " (" +
                "rowId Int32, " +
                "str_arr Array(String), " +
                "arr2d Array(Array(Int32))" +
                ") Engine = MergeTree ORDER BY rowId").get();

        client.execute("INSERT INTO " + table + " VALUES " +
                "(1, ['hello', 'world'], [[1, 2], [3]])").get();

        try (Client listClient = newClient()
                .typeHintMapping(Collections.singletonMap(ClickHouseDataType.Array, Object.class))
                .build()) {
            // Reader path: arrays are decoded as List due to Array -> Object type hint mapping.
            try (QueryResponse response = listClient.query("SELECT * FROM " + table).get()) {
                ClickHouseBinaryFormatReader reader = listClient.newBinaryFormatReader(response);
                Assert.assertNotNull(reader.next());

                Object[] strObjectArr = reader.getObjectArray("str_arr");
                Assert.assertNotNull(strObjectArr);
                Assert.assertEquals(strObjectArr, new Object[] {"hello", "world"});

                Object[] arr2dObjectArr = reader.getObjectArray("arr2d");
                Assert.assertNotNull(arr2dObjectArr);
                Assert.assertEquals(arr2dObjectArr.length, 2);
                Assert.assertTrue(arr2dObjectArr[0] instanceof List<?>);
                Assert.assertTrue(arr2dObjectArr[1] instanceof List<?>);
                Assert.assertEquals((List<?>) arr2dObjectArr[0], Arrays.asList(1, 2));
                Assert.assertEquals((List<?>) arr2dObjectArr[1], Collections.singletonList(3));

                Assert.expectThrows(ClientException.class, () -> reader.getStringArray("str_arr"));
            }

            // queryAll path (MapBackedRecord): also list-backed values.
            List<GenericRecord> records = listClient.queryAll("SELECT * FROM " + table + " ORDER BY rowId");
            Assert.assertEquals(records.size(), 1);

            GenericRecord row = records.get(0);
            Object[] strObjectArr = row.getObjectArray("str_arr");
            Assert.assertNotNull(strObjectArr);
            Assert.assertEquals(strObjectArr, new Object[] {"hello", "world"});

            Object[] arr2dObjectArr = row.getObjectArray("arr2d");
            Assert.assertNotNull(arr2dObjectArr);
            Assert.assertEquals(arr2dObjectArr.length, 2);
            Assert.assertTrue(arr2dObjectArr[0] instanceof List<?>);
            Assert.assertTrue(arr2dObjectArr[1] instanceof List<?>);
            Assert.assertEquals((List<?>) arr2dObjectArr[0], Arrays.asList(1, 2));
            Assert.assertEquals((List<?>) arr2dObjectArr[1], Collections.singletonList(3));

            Assert.expectThrows(ClientException.class, () -> row.getStringArray("str_arr"));
        }
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

    private Client.Builder newClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .compressClientRequest(useClientCompression)
                .useHttpCompression(useHttpCompression);
    }
}
