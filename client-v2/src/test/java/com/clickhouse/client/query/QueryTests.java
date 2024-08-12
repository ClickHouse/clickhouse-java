package com.clickhouse.client.query;


import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.DataTypeUtils;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.NativeFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesFormatReader;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.NullValueException;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.BaseStream;

public class QueryTests extends BaseIntegrationTest {

    private final static Random RANDOM = new Random();

    private Client client;

    private boolean useServerCompression = false;

    private boolean useHttpCompression = false;

    QueryTests(){
    }

    public QueryTests(boolean useServerCompression, boolean useHttpCompression) {
        this.useServerCompression = useServerCompression;
        this.useHttpCompression = useHttpCompression;
    }

    @BeforeMethod(groups = {"integration"})
    public void setUp() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), false)
                .setUsername("default")
                .setPassword("")
                .compressClientRequest(false)
                .compressServerResponse(useServerCompression)
                .useHttpCompression(useHttpCompression)
                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "false").equals("true"))
                .build();

        delayForProfiler(0);
        System.out.println("Real port: " + node.getPort());
    }

    @AfterMethod(groups = {"integration"})
    public void tearDown() {
        client.close();
    }

    private static void delayForProfiler(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test(groups = {"integration"})
    public void testSimpleQueryWithTSV() {
        prepareSimpleDataSet();

        QuerySettings settings = new QuerySettings()
                .setFormat(ClickHouseFormat.TabSeparated);

        Future<QueryResponse> response = client.query("SELECT * FROM " + DATASET_TABLE, settings);

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(response.get().getInputStream()));
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (InterruptedException | ExecutionException e) {
            Assert.fail("failed to get response", e);
        } catch (IOException e) {
            Assert.fail("failed to read response", e);
        }
    }

    @Test(groups = {"integration"})
    public void testReadRecords() throws Exception {
        prepareDataSet(DATASET_TABLE, DATASET_COLUMNS, DATASET_VALUE_GENERATORS, 10);

        Records records = client.queryRecords("SELECT * FROM " + DATASET_TABLE).get(3, TimeUnit.SECONDS);
        Assert.assertEquals(records.getResultRows(), 10, "Unexpected number of rows");
        for (GenericRecord record : records) {
            System.out.println(record.getLong(1)); // UInt32 column col1
            System.out.println(record.getInteger(2)); // Int32 column col2
            System.out.println(record.getString(3)); // string column col3
            System.out.println(record.getLong(4)); // Int64 column col4
            System.out.println(record.getString(5)); // string column col5
        }
    }

    @Test(groups = {"integration"})
    public void testReadRecordsGetFirstRecord() throws Exception {
        prepareDataSet(DATASET_TABLE, DATASET_COLUMNS, DATASET_VALUE_GENERATORS, 10);
        Records records = client.queryRecords("SELECT hostname()").get(3, TimeUnit.SECONDS);

        Iterator<GenericRecord> iter = records.iterator();
        Assert.expectThrows(IllegalStateException.class, records::iterator);
        iter.next();
        Assert.assertFalse(iter.hasNext());
    }

    @Test(groups = {"integration"})
    public void testReadRecordsNoResult() throws Exception {
        Records records = client.queryRecords("CREATE DATABASE IF NOT EXISTS test_db").get(3, TimeUnit.SECONDS);

        Iterator<GenericRecord> iter = records.iterator();
        Assert.assertFalse(iter.hasNext());
    }

    @Test(groups = {"integration"})
    public void testQueryAll() throws Exception {
        testQueryAll(10);
    }
    public void testQueryAll(int numberOfRecords) throws Exception {
        prepareDataSet(DATASET_TABLE, DATASET_COLUMNS, DATASET_VALUE_GENERATORS, numberOfRecords);
        GenericRecord hostnameRecord = client.queryAll("SELECT hostname()").stream().findFirst().get();
        Assert.assertNotNull(hostnameRecord);
    }

    @Test(groups = {"integration"})
    public void testQueryAllNoResult() throws Exception {
        List<GenericRecord> records = client.queryAll("CREATE DATABASE IF NOT EXISTS test_db");
        Assert.assertTrue(records.isEmpty());
    }

    @Test(groups = {"integration"})
    public void testQueryJSON() throws ExecutionException, InterruptedException {
        Map<String, Object> datasetRecord = prepareSimpleDataSet();
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.JSONEachRow);
        Future<QueryResponse> response = client.query("SELECT * FROM " + DATASET_TABLE, settings);
        final ObjectMapper objectMapper = new ObjectMapper();
        try (QueryResponse queryResponse = response.get(); MappingIterator<JsonNode> jsonIter = objectMapper.readerFor(JsonNode.class)
                .readValues(queryResponse.getInputStream())) {


            while (jsonIter.hasNext()) {
                JsonNode node = jsonIter.next();
                System.out.println(node);
                long col1 = node.get("col1").asLong();
                Assert.assertEquals(col1, datasetRecord.get("col1"));
                int col2 = node.get("col2").asInt();
                Assert.assertEquals(col2, datasetRecord.get("col2"));
                String col3 = node.get("col3").asText();
                Assert.assertEquals(col3, datasetRecord.get("col3"));
                long col4 = node.get("col4").asLong();
                Assert.assertEquals(col4, datasetRecord.get("col4"));
            }
        } catch (Exception e) {
            Assert.fail("failed to read response", e);
        }
    }

    @DataProvider(name = "rowBinaryFormats")
    ClickHouseFormat[] getRowBinaryFormats() {
        return new ClickHouseFormat[]{
                ClickHouseFormat.RowBinaryWithNamesAndTypes,
                ClickHouseFormat.Native,
                ClickHouseFormat.RowBinaryWithNames,
                ClickHouseFormat.RowBinary
        };
    }

    @Test(groups = {"integration"}, dataProvider = "rowBinaryFormats")
    public void testRowBinaryQueries(ClickHouseFormat format)
            throws ExecutionException, InterruptedException {
        final int rows = 3;
        // TODO: replace with dataset with all primitive types of data
        // TODO: reusing same table name may lead to a conflict in tests?

        List<Map<String, Object>> data = prepareDataSet(DATASET_TABLE + "_" + format.name(), DATASET_COLUMNS,
                DATASET_VALUE_GENERATORS, rows);
        QuerySettings settings = new QuerySettings().setFormat(format);
        Future<QueryResponse> response = client.query("SELECT * FROM " + DATASET_TABLE + "_" + format.name(), settings);
        QueryResponse queryResponse = response.get();

        TableSchema tableSchema = client.getTableSchema(DATASET_TABLE + "_" + format.name());
        ClickHouseBinaryFormatReader reader = createBinaryFormatReader(queryResponse, settings, tableSchema);

        Iterator<Map<String, Object>> dataIterator = data.iterator();
        int rowsCount = 0;
        while (dataIterator.hasNext()) {
            Map<String, Object> expectedRecord = dataIterator.next();
            Map<String, Object> actualRecord = reader.next();
            Assert.assertEquals(actualRecord, expectedRecord);
            rowsCount++;
        }

        Assert.assertEquals(rowsCount, rows);
    }

    private static ClickHouseBinaryFormatReader createBinaryFormatReader(QueryResponse response, QuerySettings settings,
                                                                         TableSchema schema) {
        ClickHouseBinaryFormatReader reader = null;
        switch (response.getFormat()) {
            case Native:
                reader = new NativeFormatReader(response.getInputStream(), settings);
                break;
            case RowBinaryWithNamesAndTypes:
                reader = new RowBinaryWithNamesAndTypesFormatReader(response.getInputStream(), settings);
                break;
            case RowBinaryWithNames:
                reader = new RowBinaryWithNamesFormatReader(response.getInputStream(), settings, schema);
                break;
            case RowBinary:
                reader = new RowBinaryFormatReader(response.getInputStream(), settings, schema);
                break;
            default:
                Assert.fail("unsupported format: " + response.getFormat().name());
        }
        return reader;
    }

    @Test(groups = {"integration"})
    public void testBinaryStreamReader() throws Exception {
        final String table = "dynamic_schema_test_table";
        List<Map<String, Object>> data = prepareDataSet(table, DATASET_COLUMNS,
                DATASET_VALUE_GENERATORS, 10);
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        Future<QueryResponse> response = client.query("SELECT col1, col3, hostname() as host FROM " + table, settings);
        QueryResponse queryResponse = response.get();

        TableSchema schema = new TableSchema();
        schema.addColumn("col1", "UInt32");
        schema.addColumn("col3", "String");
        schema.addColumn("host", "String");
        ClickHouseBinaryFormatReader reader = createBinaryFormatReader(queryResponse, settings, schema);
        int rowsCount = 0;
        while (reader.next() != null) {
            String hostName = reader.readValue("host");
            Long col1 = reader.readValue("col1");
            String col3 = reader.readValue("col3");

            System.out.println("host: " + hostName + ", col1: " + col1 + ", col3: " + col3);

            Assert.assertEquals(reader.readValue(1), col1);
            Assert.assertEquals(reader.readValue(2), col3);
            Assert.assertEquals(reader.readValue(3), hostName);
            rowsCount++;
        }
        Assert.assertEquals(rowsCount, 10);
        Assert.assertFalse(reader.hasNext());
        Assert.assertNull(reader.next());
    }

    @Test(groups = {"integration"})
    public void testRowStreamReader() throws Exception {
        final String table = "dynamic_schema_row_test_table";
        final int rows = 10;
        List<Map<String, Object>> data = prepareDataSet(table, DATASET_COLUMNS, DATASET_VALUE_GENERATORS, rows);
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        Future<QueryResponse> response = client.query("SELECT col1, col3, hostname() as host FROM " + table, settings);

        QueryResponse queryResponse = response.get();
        TableSchema schema = new TableSchema();
        schema.addColumn("col1", "UInt32");
        schema.addColumn("col3", "String");
        schema.addColumn("host", "String");
        ClickHouseBinaryFormatReader reader = new RowBinaryWithNamesAndTypesFormatReader(queryResponse.getInputStream(), settings);

        Map<String, Object> record;
        for (int i = 0; i < rows; i++) {
            record = reader.next();
            Assert.assertNotNull(record);
        }
    }

    private final static List<String> ARRAY_COLUMNS = Arrays.asList(
            "col1 Array(UInt32)",
            "col2 Array(Array(Int32))"
    );

    private final static List<Function<String, Object>> ARRAY_VALUE_GENERATORS = Arrays.asList(
            c ->
                    RANDOM.ints(10, 0, 100),
            c -> {
                List<List<Integer>> values = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    values.add(Arrays.asList(1, 2, 3));
                }
                return values;
            }
    );


    @Test(groups = {"integration"})
    public void testArrayValues() throws Exception {
        final String table = "array_values_test_table";
        final int rows = 1;
        List<Map<String, Object>> data = prepareDataSet(table, ARRAY_COLUMNS, ARRAY_VALUE_GENERATORS, rows);

        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        Future<QueryResponse> response = client.query("SELECT * FROM " + table, settings);
        TableSchema schema = client.getTableSchema(table);

        QueryResponse queryResponse = response.get();
        ClickHouseBinaryFormatReader reader = createBinaryFormatReader(queryResponse, settings, schema);


        Map<String, Object> record = reader.next();
        Assert.assertNotNull(record);
        long[] col1Values = reader.getLongArray("col1");
        System.out.println("col1: " + Arrays.toString(col1Values));
        System.out.println("Record: " + record);
    }

    private final static List<String> MAP_COLUMNS = Arrays.asList(
            "col1 Map(String, Int8)",
            "col2 Map(String, String)"
    );

    private final static List<Function<String, Object>> MAP_VALUE_GENERATORS = Arrays.asList(
            c -> {
                Map<String, Byte> values = new HashMap<>();
                values.put("key1", (byte) 1);
                values.put("key2", (byte) 2);
                values.put("key3", (byte) 3);
                return values;
            },

            c -> {
                Map<String, String> values = new HashMap<>();
                values.put("key1", "value1");
                values.put("key2", "value2");
                values.put("key3", "value3");
                return values;
            }
    );


    @Test
    public void testMapValues() throws Exception {
        final String table = "map_values_test_table";
        final int rows = 1;
        List<Map<String, Object>> data = prepareDataSet(table, MAP_COLUMNS, MAP_VALUE_GENERATORS, rows);

        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        Future<QueryResponse> response = client.query("SELECT * FROM " + table, settings);
        TableSchema schema = client.getTableSchema(table);

        QueryResponse queryResponse = response.get();
        ClickHouseBinaryFormatReader reader = createBinaryFormatReader(queryResponse, settings, schema);

        Map<String, Object> record = reader.next();
        Assert.assertNotNull(record);
//        System.out.println("col1: " + Arrays.toString(col1Values));
        System.out.println("Record: " + record);
    }


    @Test(groups = {"integration"})
    public void testQueryExceptionHandling() throws Exception {

        try {
            client.queryRecords("SELECT * FROM unknown_table").get(3, TimeUnit.SECONDS);
            Assert.fail("expected exception");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof ClientException);
        }
    }


    private final static List<String> NULL_DATASET_COLUMNS = Arrays.asList(
            "id UInt32",
            "col1 UInt32 NULL",
            "col2 Int32 NULL DEFAULT 1000",
            "col3 String NULL",
            "col5 String NULL DEFAULT 'default_value'"
    );


    private final static List<Function<String, Object>> NULL_DATASET_VALUE_GENERATORS = Arrays.asList(
            c -> 1,
            c -> null,
            c -> null,
            c -> null,
            c -> null
    );

    @Test(groups = {"integration"})
    public void testNullValues() throws Exception {
        final String table = "null_values_test_table";
        final int rows = 1;
        List<Map<String, Object>> data = prepareDataSet(table, NULL_DATASET_COLUMNS, NULL_DATASET_VALUE_GENERATORS, 1);

        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        Future<QueryResponse> response = client.query("SELECT * FROM " + table, settings);
        TableSchema schema = client.getTableSchema(table);

        QueryResponse queryResponse = response.get();
        ClickHouseBinaryFormatReader reader = createBinaryFormatReader(queryResponse, settings, schema);

        Map<String, Object> record = reader.next();
        Assert.assertNotNull(record);
        System.out.println("Record: " + record);
        int i = 0;
        for (String columns : NULL_DATASET_COLUMNS) {
            String columnName = columns.split(" ")[0];

            if (columnName.equals("id")) {
                Assert.assertTrue(record.containsKey(columnName));
                Assert.assertEquals(record.get(columnName), 1L);
                Assert.assertTrue(reader.hasValue("id"));
                Assert.assertTrue(reader.hasValue(i), "No value for column " + i);

            } else {
                Assert.assertFalse(record.containsKey(columnName));
                Assert.assertNull(record.get(columnName));
                Assert.assertFalse(reader.hasValue(columnName));
                Assert.assertFalse(reader.hasValue(i));

                if (columnName.equals("col1") || columnName.equals("col2")) {
                    Assert.expectThrows(NullValueException.class, () -> reader.getLong(columnName));
                }
            }

            i++;
        }
    }

    @Test
    public void testDateTimeDataTypes() {
        final List<String> columns = Arrays.asList(
                "min_date Date",
                "max_date Date",
                "min_dateTime DateTime",
                "max_dateTime DateTime"
        );

        final LocalDate minDate = LocalDate.parse("1970-01-01");
        final LocalDate maxDate = LocalDate.parse("2149-06-06");
        final LocalDateTime minDateTime = LocalDateTime.parse("1970-01-01T00:00:00");
        final LocalDateTime maxDateTime = LocalDateTime.parse("2106-02-07T06:28:15");
        final List<Supplier<String>> valueGenerators = Arrays.asList(
                () -> sq(minDate.toString()),
                () -> sq(maxDate.toString()),
                () -> sq(minDateTime.format(DataTypeUtils.DATE_TIME_FORMATTER)),
                () -> sq(maxDateTime.format(DataTypeUtils.DATE_TIME_FORMATTER))
        );

        final List<Consumer<ClickHouseBinaryFormatReader>> verifiers = new ArrayList<>();
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("min_date"), "No value for column min_date found");
            Assert.assertEquals(r.getLocalDate("min_date"), minDate);
            Assert.assertEquals(r.getLocalDate(1), minDate);
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("max_date"), "No value for column max_date found");
            Assert.assertEquals(r.getLocalDate("max_date"), maxDate);
            Assert.assertEquals(r.getLocalDate(2), maxDate);
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("min_dateTime"), "No value for column min_dateTime found");
            Assert.assertEquals(r.getLocalDateTime("min_dateTime"), minDateTime);
            Assert.assertEquals(r.getLocalDateTime(3), minDateTime);
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("max_dateTime"), "No value for column max_dateTime found");
            Assert.assertEquals(r.getLocalDateTime("max_dateTime"), maxDateTime);
            Assert.assertEquals(r.getLocalDateTime(4), maxDateTime);
        });

        testDataTypes(columns, valueGenerators, verifiers);
    }

    private Consumer<ClickHouseBinaryFormatReader> createNumberVerifier(String columnName, int columnIndex,
                                                                        int bits, boolean isSigned,
                                                                        BigInteger expectedValue) {
        return r -> {
            Assert.assertTrue(r.hasValue(columnName), "No value for column " + columnName + " found");
            if (bits == 8 && isSigned) {
                Assert.assertEquals(r.getByte(columnName), expectedValue.byteValueExact(), "Failed for column " + columnName);
                Assert.assertEquals(r.getByte(columnIndex), expectedValue.byteValueExact(), "Failed for column " + columnIndex);
            } else if (bits == 8) {
                Assert.assertEquals(r.getShort(columnName), expectedValue.shortValueExact(), "Failed for column " + columnName);
                Assert.assertEquals(r.getShort(columnIndex), expectedValue.shortValueExact(), "Failed for column " + columnIndex);
            }
            if (bits == 16 && isSigned) {
                Assert.assertEquals(r.getShort(columnName), expectedValue.shortValueExact(), "Failed for column " + columnName);
                Assert.assertEquals(r.getShort(columnIndex), expectedValue.shortValueExact(), "Failed for column " + columnIndex);
            } else if (bits == 16) {
                Assert.assertEquals(r.getInteger(columnName), expectedValue.intValueExact(), "Failed for column " + columnName);
                Assert.assertEquals(r.getInteger(columnIndex), expectedValue.intValueExact(), "Failed for column " + columnIndex);
            }

            if (bits == 32 && isSigned) {
                Assert.assertEquals(r.getInteger(columnName), expectedValue.intValueExact(), "Failed for column " + columnName);
                Assert.assertEquals(r.getInteger(columnIndex), expectedValue.intValueExact(), "Failed for column " + columnIndex);
            } else if (bits == 32) {
                Assert.assertEquals(r.getLong(columnName), expectedValue.longValueExact(), "Failed for column " + columnName);
                Assert.assertEquals(r.getLong(columnIndex), expectedValue.longValueExact(), "Failed for column " + columnIndex);
            }

            if (bits == 64 && isSigned) {
                Assert.assertEquals(r.getLong(columnName), expectedValue.longValueExact(), "Failed for column " + columnName);
                Assert.assertEquals(r.getLong(columnIndex), expectedValue.longValueExact(), "Failed for column " + columnIndex);
            } else if (bits == 64) {
                Assert.assertEquals(r.getBigInteger(columnName), expectedValue, "Failed for column " + columnName);
                Assert.assertEquals(r.getBigInteger(columnIndex), expectedValue, "Failed for column " + columnIndex);
            }

            if (bits >= 128) {
                Assert.assertEquals(r.getBigInteger(columnName), expectedValue, "Failed for column " + columnName);
                Assert.assertEquals(r.getBigInteger(columnIndex), expectedValue, "Failed for column " + columnIndex);
            }
        };
    }

    @Test(groups = {"integration"})
    public void testIntegerDataTypes() {
        final List<String> columns = new ArrayList<>();
        List<Supplier<String>> valueGenerators = new ArrayList<>();
        List<Consumer<ClickHouseBinaryFormatReader>> verifiers = new ArrayList<>();

        for (int i = 3; i < 9; i++) {
            int bits = (int) Math.pow(2, i);
            columns.add("min_int" + bits + " Int" + bits);
            columns.add("min_uint" + bits + " UInt" + bits);
            columns.add("max_int" + bits + " Int" + bits);
            columns.add("max_uint" + bits + " UInt" + bits);

            final BigInteger minInt = BigInteger.valueOf(-1).multiply(BigInteger.valueOf(2).pow(bits - 1));
            final BigInteger maxInt = BigInteger.valueOf(2).pow(bits - 1).subtract(BigInteger.ONE);
            final BigInteger maxUInt = BigInteger.valueOf(2).pow(bits).subtract(BigInteger.ONE);

            valueGenerators.add(() -> String.valueOf(minInt));
            valueGenerators.add(() -> String.valueOf(0));
            valueGenerators.add(() -> String.valueOf(maxInt));
            valueGenerators.add(() -> String.valueOf(maxUInt));

            final int index = i - 3;
            verifiers.add(createNumberVerifier("min_int" + bits, index * 4 + 1, bits, true,
                    minInt));
            verifiers.add(createNumberVerifier("min_uint" + bits, index * 4 + 2, bits, false,
                    BigInteger.ZERO));
            verifiers.add(createNumberVerifier("max_int" + bits, index * 4 + 3, bits, true,
                    maxInt));
            verifiers.add(createNumberVerifier("max_uint" + bits, index * 4 + 4, bits, false,
                    maxUInt));
        }

//        valueGenerators.forEach(r -> System.out.println(r.get()));

        testDataTypes(columns, valueGenerators, verifiers);
    }

    @Test(groups = {"integration"})
    public void testFloatDataTypes() {
        final List<String> columns = Arrays.asList(
                "min_float32 Float32",
                "max_float32 Float32",
                "min_float64 Float64",
                "max_float64 Float64",
                "float_nan Float32",
                "float_pos_inf Float32",
                "float_neg_inf Float32",
                "pi_float32 Float32",
                "pi_float64 Float64"
        );

        final List<Supplier<String>> valueGenerators = Arrays.asList(
                () -> String.valueOf(Float.MIN_VALUE),
                () -> String.valueOf(Float.MAX_VALUE),
                () -> String.valueOf(Double.MIN_VALUE),
                () -> String.valueOf(Double.MAX_VALUE),
                () -> "NaN",
                () -> "Inf",
                () -> "-Inf",
                () -> String.valueOf((float) Math.PI),
                () -> String.valueOf(Math.PI)
        );

        final List<Consumer<ClickHouseBinaryFormatReader>> verifiers = new ArrayList<>();
        verifiers.add(r -> {
            Assert.assertEquals(r.getFloat("min_float32"), Float.MIN_VALUE);
            Assert.assertEquals(r.getFloat(1), Float.MIN_VALUE);

        });
        verifiers.add(r -> {
            Assert.assertEquals(r.getFloat("max_float32"), 3.4028233E38F); // TODO: investigate why it's not Float.MAX_VALUE returned from server
            Assert.assertEquals(r.getFloat(2), 3.4028233E38F);
        });
        verifiers.add(r -> {
            Assert.assertEquals(r.getDouble("min_float64"), 0.0D); // TODO: investigate why it's not Double.MIN_VALUE returned from server
            Assert.assertEquals(r.getDouble(3), 0.0D);
        });
        verifiers.add(r -> {
            Assert.assertEquals(r.getDouble("max_float64"), Double.MAX_VALUE);
            Assert.assertEquals(r.getDouble(4), Double.MAX_VALUE);
        });
        verifiers.add(r -> {
            Assert.assertTrue(Float.isNaN(r.getFloat("float_nan")));
            Assert.assertTrue(Float.isNaN(r.getFloat(5)));
        });
        verifiers.add(r -> {
            Assert.assertTrue(Float.isInfinite(r.getFloat("float_pos_inf")));
            Assert.assertTrue(Float.isInfinite(r.getFloat(6)));
        });
        verifiers.add(r -> {
            Assert.assertTrue(Float.isInfinite(r.getFloat("float_neg_inf")));
            Assert.assertTrue(Float.isInfinite(r.getFloat(7)));
        });
        verifiers.add(r -> {
            Assert.assertEquals(r.getFloat("pi_float32"), (float) Math.PI);
            Assert.assertEquals(r.getFloat(8), (float) Math.PI);
        });
        verifiers.add(r -> {
            Assert.assertEquals(r.getDouble("pi_float64"), Math.PI);
            Assert.assertEquals(r.getDouble(9), Math.PI);
        });

        testDataTypes(columns, valueGenerators, verifiers);
    }

    @Test
    public void testDecimalDataTypes() {
        final List<String> columns = new ArrayList<>();
        List<Supplier<String>> valueGenerators = new ArrayList<>();
        List<Consumer<ClickHouseBinaryFormatReader>> verifiers = new ArrayList<>();

        for (int i = 5; i < 9; i++) {
            int bits = (int) Math.pow(2, i);
            int scale = 4;
            columns.add("min_decimal" + bits + " Decimal" + (bits == 5 ? "" : bits) + "(" + scale + ")");
            columns.add("max_decimal" + bits + " Decimal" + (bits == 5 ? "" : bits) + "(" + scale + ")");

            BigDecimal minDecimal = BigDecimal.valueOf(-1).multiply(BigDecimal.valueOf(10).pow(9 - scale)).add(BigDecimal.ONE).setScale(scale);
            BigDecimal maxDecimal = BigDecimal.valueOf(1).multiply(BigDecimal.valueOf(10).pow(9 - scale)).subtract(BigDecimal.ONE).setScale(scale);

            valueGenerators.add(() -> String.valueOf(minDecimal));
            valueGenerators.add(() -> String.valueOf(maxDecimal));

            final int index = i - 5;

            verifiers.add(r -> {
                Assert.assertTrue(r.hasValue("min_decimal" + bits), "No value for column min_decimal" + bits + " found");
                Assert.assertEquals(r.getBigDecimal("min_decimal" + bits), minDecimal, "Failed for column min_decimal" + bits);
                Assert.assertEquals(r.getBigDecimal(index * 2 + 1), minDecimal, "Failed for column " + index * 2 + 1);
            });
            verifiers.add(r -> {
                Assert.assertTrue(r.hasValue("max_decimal" + bits), "No value for column max_decimal" + bits + " found");
                Assert.assertEquals(r.getBigDecimal("max_decimal" + bits), maxDecimal, "Failed for column max_decimal" + bits);
                Assert.assertEquals(r.getBigDecimal(index * 2 + 2), maxDecimal, "Failed for column " + index * 2 + 2);
            });

        }
        System.out.println("Columns: " + columns);
//        valueGenerators.forEach(r -> System.out.println(r.get()));
        testDataTypes(columns, valueGenerators, verifiers);
    }

    @Test(groups = {"integration"})
    public void testTuples() {
        final List<String> columns = Arrays.asList(
                "col1 Tuple(UInt32, String)",
                "col2 Tuple(UInt32, String, Float32)",
                "col3 Tuple(UInt32, Tuple(Float32, String))"
        );

        final List<Supplier<String>> valueGenerators = Arrays.asList(
                () -> "(1, 'value1')",
                () -> "(1, 'value2', 23.43)",
                () -> "(1, (23.43, 'value3'))"
        );

        final List<Consumer<ClickHouseBinaryFormatReader>> verifiers = new ArrayList<>();
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("col1"), "No value for column col1 found");
            Assert.assertEquals(r.getTuple("col1"), new Object[]{1L, "value1"});
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("col2"), "No value for column col2 found");
            Assert.assertEquals(r.getTuple("col2"), new Object[]{1L, "value2", 23.43f});
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("col3"), "No value for column col3 found");
            Assert.assertEquals(r.getTuple("col3"), new Object[]{1L, new Object[]{23.43f, "value3"}});
        });

        testDataTypes(columns, valueGenerators, verifiers);
    }

    @Test
    public void testEnums() {
        final List<String> columns = Arrays.asList(
                "min_enum16 Enum16('value1' = -32768, 'value2' = 2, 'value3' = 3)",
                "max_enum16 Enum16('value1' = -32768, 'value2' = 2, 'value3' = 32767)",
                "min_enum8 Enum8('value1' = -128, 'value2' = 2, 'value3' = 3)",
                "max_enum8 Enum8('value1' = 1, 'value2' = 2, 'value3' = 127)"
        );

        final UUID providedUUID = UUID.randomUUID();
        final List<Supplier<String>> valueGenerators = Arrays.asList(
                () -> "'value1'",
                () -> "'value3'",
                () -> "'value1'",
                () -> "'value3'"
        );

        final List<Consumer<ClickHouseBinaryFormatReader>> verifiers = new ArrayList<>();
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("min_enum16"), "No value for column min_enum16 found");
            Assert.assertEquals(r.getEnum16("min_enum16"), (short) -32768);
            Assert.assertEquals(r.getEnum16(1), (short) -32768);
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("max_enum16"), "No value for column max_enum16 found");
            Assert.assertEquals(r.getEnum16("max_enum16"), (short) 32767);
            Assert.assertEquals(r.getEnum16(2), (short) 32767);
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("min_enum8"), "No value for column min_enum8 found");
            Assert.assertEquals(r.getEnum8("min_enum8"), (byte) -128);
            Assert.assertEquals(r.getEnum8(3), (byte) -128);
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("max_enum8"), "No value for column max_enum8 found");
            Assert.assertEquals(r.getEnum8("max_enum8"), (byte) 127);
            Assert.assertEquals(r.getEnum8(4), (byte) 127);
        });

        testDataTypes(columns, valueGenerators, verifiers);
    }

    @Test
    public void testUUID() {
        final List<String> columns = Arrays.asList(
                "provided UUID",
                "db_generated UUID",
                "zero UUID"
        );

        final UUID providedUUID = UUID.randomUUID();
        final List<Supplier<String>> valueGenerators = Arrays.asList(
                () -> sq(providedUUID.toString()),
                () -> "generateUUIDv4()",
                () -> sq("00000000-0000-0000-0000-000000000000")
        );

        final List<Consumer<ClickHouseBinaryFormatReader>> verifiers = new ArrayList<>();
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("provided"), "No value for column provided found");
            Assert.assertEquals(r.getUUID("provided"), providedUUID);
            Assert.assertEquals(r.getUUID(1), providedUUID);
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("db_generated"), "No value for column db_generated found");
            Assert.assertNotNull(r.getUUID("db_generated"));
            Assert.assertNotNull(r.getUUID(2));
            Assert.assertEquals(r.getUUID("db_generated"), UUID.fromString(r.getString(2)));
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("zero"), "No value for column zero found");
            Assert.assertEquals(r.getUUID("zero"), UUID.fromString("00000000-0000-0000-0000-000000000000"));
            Assert.assertEquals(r.getUUID(3), UUID.fromString("00000000-0000-0000-0000-000000000000"));
        });

        testDataTypes(columns, valueGenerators, verifiers);
    }

    @Test(groups = {"integration"})
    public void testStringDataTypes() {
        final List<String> columns = Arrays.asList(
                "col1 String",
                "col2 FixedString(10)",
                "col3 String NULL",
                "col4 String NULL"

        );

        final List<Supplier<String>> valueGenerators = Arrays.asList(
                () -> sq("utf8 string с кириллицей そして他のホイッスル"),
                () -> sq("7 chars"),
                () -> "NULL",
                () -> sq("not null string")
        );

        final List<Consumer<ClickHouseBinaryFormatReader>> verifiers = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            final int index = i;
            String tmpVal = valueGenerators.get(index).get();
            if (tmpVal.startsWith("'") && tmpVal.endsWith("'")) {
                tmpVal = tmpVal.substring(1, tmpVal.length() - 1);
            }
            final String val = tmpVal;
            String columnName = columns.get(index).split(" ")[0];

            if (val.equals("NULL")) {
                verifiers.add(r -> {
                    Assert.assertFalse(r.hasValue(columnName), "No value for column " + columnName + " expected");
                    Assert.assertNull(r.getString(columnName));
                    Assert.assertNull(r.getString(index + 1));
                });
            } else {
                verifiers.add(r -> {
                    Assert.assertTrue(r.hasValue(columnName), "No value for column " + columnName + " found");
                    Assert.assertEquals(r.getString(columnName), val);
                    Assert.assertEquals(r.getString(index + 1), val);
                });
            }
        }
        testDataTypes(columns, valueGenerators, verifiers);
    }

    private static String sq(String str) {
        return "\'" + str + "\'";
    }

    public void testDataTypes(List<String> columns, List<Supplier<String>> valueGenerators, List<Consumer<ClickHouseBinaryFormatReader>> verifiers) {
        final String table = "data_types_test_table";

        try (ClickHouseClient client = ClickHouseClient.builder().config(new ClickHouseConfig())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build()) {
            // Drop table
            ClickHouseRequest<?> request = client.read(getServer(ClickHouseProtocol.HTTP))
                    .query("DROP TABLE IF EXISTS default." + table);
            try (ClickHouseResponse response = request.executeAndWait()) {}

            // Create table
            StringBuilder createStmtBuilder = new StringBuilder();
            createStmtBuilder.append("CREATE TABLE IF NOT EXISTS default.").append(table).append(" (");
            for (String column : columns) {
                createStmtBuilder.append(column).append(", ");
            }
            createStmtBuilder.setLength(createStmtBuilder.length() - 2);
            createStmtBuilder.append(") ENGINE = MergeTree ORDER BY tuple()");
            request = client.read(getServer(ClickHouseProtocol.HTTP))
                    .query(createStmtBuilder.toString());
            try (ClickHouseResponse response = request.executeAndWait()) {}


            // Insert data
            StringBuilder insertStmtBuilder = new StringBuilder();
            insertStmtBuilder.append("INSERT INTO default.").append(table).append(" VALUES ");
            insertStmtBuilder.append("(");
            for (Supplier<String> valueSupplier : valueGenerators) {
                insertStmtBuilder.append(valueSupplier.get()).append(", ");
            }
            insertStmtBuilder.setLength(insertStmtBuilder.length() - 2);
            insertStmtBuilder.append("), ");
            insertStmtBuilder.setLength(insertStmtBuilder.length() - 2);
            System.out.println("Insert statement: " + insertStmtBuilder);

            request = client.write(getServer(ClickHouseProtocol.HTTP))
                    .query(insertStmtBuilder.toString());
            try (ClickHouseResponse response = request.executeAndWait()) {
                Assert.assertEquals(response.getSummary().getWrittenRows(), 1);
            }
        } catch (Exception e) {
            Assert.fail("Failed at prepare stage", e);
        }

        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        StringBuilder selectStmtBuilder = new StringBuilder();
        selectStmtBuilder.append("SELECT ");
        for (String column : columns) {
            String columnName = column.split(" ")[0];
            selectStmtBuilder.append(columnName).append(", ");
        }
        selectStmtBuilder.setLength(selectStmtBuilder.length() - 2);
        selectStmtBuilder.append(" FROM ").append(table);
        Future<QueryResponse> response = client.query(selectStmtBuilder.toString(), settings);
        TableSchema schema = client.getTableSchema(table);

        try {
            QueryResponse queryResponse = response.get();
            ClickHouseBinaryFormatReader reader = createBinaryFormatReader(queryResponse, settings, schema);
            Assert.assertNotNull(reader.next());
            Assert.assertEquals(verifiers.size(), columns.size(), "Number of verifiers should match number of columns");
            int colIndex = 0;
            for (Consumer<ClickHouseBinaryFormatReader> verifier : verifiers) {
                colIndex++;
                try {
                    verifier.accept(reader);
                    System.out.println("Verified " + colIndex);
                } catch (Exception e) {
                    Assert.fail("Failed to verify " + columns.get(colIndex), e);
                }
            }

        } catch (Exception e) {
            Assert.fail("Failed at verification stage", e);
        }
    }

    @Test(groups = {"integration"})
    public void testQueryMetrics() throws Exception {
        prepareDataSet(DATASET_TABLE, DATASET_COLUMNS, DATASET_VALUE_GENERATORS, 10);

        String uuid = UUID.randomUUID().toString();
        QuerySettings settings = new QuerySettings()
                .setFormat(ClickHouseFormat.TabSeparated)
                .waitEndOfQuery(true)
                .setQueryId(uuid);

        QueryResponse response = client.query("SELECT * FROM " + DATASET_TABLE + " LIMIT 3", settings).get();

        // Stats should be available after the query is done
        OperationMetrics metrics = response.getMetrics();
        System.out.println("Server read rows: " + metrics.getMetric(ServerMetrics.NUM_ROWS_READ).getLong());
        System.out.println("Client stats: " + metrics.getMetric(ClientMetrics.OP_DURATION).getLong());

        Assert.assertEquals(metrics.getMetric(ServerMetrics.NUM_ROWS_READ).getLong(), 10); // 10 rows in the table
        Assert.assertEquals(metrics.getMetric(ServerMetrics.RESULT_ROWS).getLong(), 3);

        StringBuilder insertStmtBuilder = new StringBuilder();
        insertStmtBuilder.append("INSERT INTO default.").append(DATASET_TABLE).append(" VALUES ");
        final int rowsToInsert = 5;
        for (int i = 0; i < rowsToInsert; i++) {
            insertStmtBuilder.append("(");
            Map<String, Object> values = writeValuesRow(insertStmtBuilder, DATASET_COLUMNS, DATASET_VALUE_GENERATORS);
            insertStmtBuilder.setLength(insertStmtBuilder.length() - 2);
            insertStmtBuilder.append("), ");
        }
        response = client.query(insertStmtBuilder.toString(), settings).get();

        metrics = response.getMetrics();
        System.out.println("Server read rows: " + metrics.getMetric(ServerMetrics.NUM_ROWS_READ).getLong());
        System.out.println("Client stats: " + metrics.getMetric(ClientMetrics.OP_DURATION).getLong());

        Assert.assertEquals(metrics.getMetric(ServerMetrics.NUM_ROWS_READ).getLong(), rowsToInsert); // 10 rows in the table
        Assert.assertEquals(metrics.getMetric(ServerMetrics.RESULT_ROWS).getLong(), rowsToInsert);
        Assert.assertEquals(response.getReadRows(), rowsToInsert);
        Assert.assertTrue(metrics.getMetric(ClientMetrics.OP_DURATION).getLong() > 0);
        Assert.assertEquals(metrics.getQueryId(), uuid);
        Assert.assertEquals(response.getQueryId(), uuid);

    }

    private final static List<String> DATASET_COLUMNS = Arrays.asList(
            "col1 UInt32",
            "col2 Int32",
            "col3 String",
            "col4 Int64",
            "col5 String"
    );

    private final static List<Function<String, Object>> DATASET_VALUE_GENERATORS = Arrays.asList(
            c -> Long.valueOf(RANDOM.nextInt(Integer.MAX_VALUE)),
            c -> RANDOM.nextInt(Integer.MAX_VALUE),
            c -> "value_" + RANDOM.nextInt(Integer.MAX_VALUE),
            c -> Long.valueOf(RANDOM.nextInt(Integer.MAX_VALUE)),
            c -> "value_" + RANDOM.nextInt(Integer.MAX_VALUE)
    );

    private final static String DATASET_TABLE = "query_test_table";

    private Map<String, Object> prepareSimpleDataSet() {
        return prepareDataSet(DATASET_TABLE, DATASET_COLUMNS, DATASET_VALUE_GENERATORS, 1).get(0);
    }

    private List<Map<String, Object>> prepareDataSet(String table, List<String> columns, List<Function<String, Object>> valueGenerators,
                                                     int rows) {
        List<Map<String, Object>> data = new ArrayList<>(rows);
        try (
                ClickHouseClient client = ClickHouseClient.builder().config(new ClickHouseConfig())
                        .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                        .build()) {
            // Drop table
            ClickHouseRequest<?> request = client.read(getServer(ClickHouseProtocol.HTTP))
                    .query("DROP TABLE IF EXISTS default." + table);
            try (ClickHouseResponse response = request.executeAndWait()) {}


            // Create table
            StringBuilder createStmtBuilder = new StringBuilder();
            createStmtBuilder.append("CREATE TABLE IF NOT EXISTS default.").append(table).append(" (");
            for (String column : columns) {
                createStmtBuilder.append(column).append(", ");
            }
            createStmtBuilder.setLength(createStmtBuilder.length() - 2);
            createStmtBuilder.append(") ENGINE = MergeTree ORDER BY tuple()");
            request = client.read(getServer(ClickHouseProtocol.HTTP))
                    .query(createStmtBuilder.toString());
            try (ClickHouseResponse response = request.executeAndWait()) {}

            // Insert data
            StringBuilder insertStmtBuilder = new StringBuilder();
            insertStmtBuilder.append("INSERT INTO default.").append(table).append(" VALUES ");
            for (int i = 0; i < rows; i++) {
                insertStmtBuilder.append("(");
                Map<String, Object> values = writeValuesRow(insertStmtBuilder, columns, valueGenerators);
                insertStmtBuilder.setLength(insertStmtBuilder.length() - 2);
                insertStmtBuilder.append("), ");
                data.add(values);
            }
            System.out.println("Insert statement: " + insertStmtBuilder);
            request = client.write(getServer(ClickHouseProtocol.HTTP))
                    .query(insertStmtBuilder.toString());
            try (ClickHouseResponse response = request.executeAndWait()) {}
        } catch (Exception e) {
            Assert.fail("failed to prepare data set", e);
        }
        return data;
    }

    private Map<String, Object> writeValuesRow(StringBuilder insertStmtBuilder, List<String> columns, List<Function<String, Object>> valueGenerators) {
        Map<String, Object> values = new HashMap<>();
        Iterator<String> columnIterator = columns.iterator();
        for (Function<String, Object> valueGenerator : valueGenerators) {
            Object value = valueGenerator.apply(null);
            if (value instanceof String) {
                insertStmtBuilder.append('\'').append(value).append('\'').append(", ");
            } else if (value instanceof BaseStream<?, ?>) {
                insertStmtBuilder.append('[');
                BaseStream<?, ?> stream = ((BaseStream<?, ?>) value);
                for (Iterator<?> it = stream.iterator(); it.hasNext(); ) {
                    insertStmtBuilder.append(quoteValue(it.next())).append(", ");
                }
                insertStmtBuilder.setLength(insertStmtBuilder.length() - 2);
                insertStmtBuilder.append("], ");
            } else if (value instanceof Map) {
                insertStmtBuilder.append("{");
                Map<String, Object> map = (Map<String, Object>) value;
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    insertStmtBuilder.append(quoteValue(entry.getKey())).append(" : ")
                            .append(quoteValue(entry.getValue())).append(", ");
                }
                insertStmtBuilder.setLength(insertStmtBuilder.length() - 2);
                insertStmtBuilder.append("}, ");
            } else {
                insertStmtBuilder.append(value).append(", ");
            }
            values.put(columnIterator.next().split(" ")[0], value);

        }
        return values;
    }

    private String quoteValue(Object value) {
        if (value instanceof String) {
            return '\'' + value.toString() + '\'';
        }
        return value.toString();
    }

    void writeArrayValues(StringBuilder sb, Iterator<?> values) {
        sb.append('[');
        while (values.hasNext()) {
            Object value = values.next();
            if (value instanceof List<?>) {
                writeArrayValues(sb, ((List<?>) value).iterator());
            } else if (value instanceof String) {
                sb.append('\'').append(value).append('\'');
            } else {
                sb.append(value);
            }
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(']');
    }

    @Test(groups = {"integration"})
    public void testQueryParams() throws Exception {
        final String table = "query_params_test_table";

        client.query("DROP TABLE IF EXISTS default." + table, null).get();
        client.query("CREATE TABLE default." + table + " (col1 UInt32, col2 String) ENGINE = MergeTree ORDER BY tuple()").get();

        ByteArrayOutputStream insertData = new ByteArrayOutputStream();
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(insertData))) {
            writer.write("1\t'one'\n");
            writer.write("2\t'two'\n");
            writer.write("3\t'three'\n");
        }
        InsertSettings insertSettings = new InsertSettings();
        client.insert(table, new ByteArrayInputStream(insertData.toByteArray()), ClickHouseFormat.TabSeparated, insertSettings).get();

        QuerySettings querySettings = new QuerySettings().setFormat(ClickHouseFormat.TabSeparatedWithNamesAndTypes);
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("param1", 2);
        QueryResponse queryResponse =
                client.query("SELECT * FROM " + table + " WHERE col1 >= {param1:UInt32}", queryParams, querySettings).get();

        try (BufferedReader responseBody = new BufferedReader(new InputStreamReader(queryResponse.getInputStream()))) {
            responseBody.lines().forEach(System.out::println);
        }
    }

    @Test(groups = {"integration"})
    public void testGetTableSchema() throws Exception {

        final String table = "table_schema_test";
        client.execute("DROP TABLE IF EXISTS default." + table).get(10, TimeUnit.SECONDS);
        client.execute("CREATE TABLE default." + table +
                " (col1 UInt32, col2 String) ENGINE = MergeTree ORDER BY tuple()").get(10, TimeUnit.SECONDS);

        TableSchema schema = client.getTableSchema(table);
        Assert.assertNotNull(schema);
        Assert.assertEquals(schema.getColumns().size(), 2);
        Assert.assertEquals(schema.getColumns().get(0).getColumnName(), "col1");
        Assert.assertEquals(schema.getColumns().get(0).getDataType(), ClickHouseDataType.UInt32);
        Assert.assertEquals(schema.getColumns().get(1).getColumnName(), "col2");
        Assert.assertEquals(schema.getColumns().get(1).getDataType(), ClickHouseDataType.String);
    }

    @Test(groups = {"integration"})
    public void testGetTableSchemaError() {
        try {
            client.getTableSchema("unknown_table");
            Assert.fail("no exception");
        } catch (ServerException e) {
            Assert.assertEquals(e.getCode(), ServerException.TABLE_NOT_FOUND);
        } catch (ClientException e) {
            e.printStackTrace();
            if (e.getCause().getCause() instanceof ServerException) {
                ServerException se = (ServerException) e.getCause().getCause();
                Assert.assertEquals(se.getCode(), ServerException.TABLE_NOT_FOUND);
            } else {
                Assert.assertEquals(((ClickHouseException) e.getCause().getCause().getCause()).getErrorCode(),
                        ServerException.TABLE_NOT_FOUND);
            }

        }
    }

    @Test(groups = {"integration"})
    public void testServerTimeZoneFromHeader() {

        final String requestTimeZone = "America/Los_Angeles";
        try (QueryResponse response =
                     client.query("SELECT now() as t, toDateTime(now(), 'UTC') as utc_time " +
                             "SETTINGS session_timezone = '" + requestTimeZone + "'").get(1, TimeUnit.SECONDS)) {

            ClickHouseBinaryFormatReader reader =
                    new RowBinaryWithNamesAndTypesFormatReader(response.getInputStream(), response.getSettings());

            reader.next();

            LocalDateTime serverTime = reader.getLocalDateTime(1);
            System.out.println("Server time: " + serverTime);
            LocalDateTime serverUtcTime = reader.getLocalDateTime(2);
            System.out.println("Server UTC time: " + serverUtcTime);

            ZonedDateTime serverTimeZ = serverTime.atZone(ZoneId.of(requestTimeZone));
            ZonedDateTime serverUtcTimeZ = serverUtcTime.atZone(ZoneId.of("UTC"));

            Assert.assertEquals(serverTimeZ.withZoneSameInstant(ZoneId.of("UTC")), serverUtcTimeZ);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Failed to get server time zone from header", e);
        }
    }


    @Test(groups = {"integration"})
    public void testClientUseOwnTimeZone() {

        final String overrideTz = "America/Los_Angeles";
        try (Client client = newClient().useTimeZone(overrideTz).useServerTimeZone(false).build()) {
            final String requestTimeZone = "Europe/Berlin";
            try (QueryResponse response =
                         client.query("SELECT now() as t, toDateTime(now(), 'UTC') as utc_time, " +
                                 "toDateTime(now(), 'Europe/Lisbon')" +
                                 "SETTINGS session_timezone = '" + requestTimeZone + "'").get(1, TimeUnit.SECONDS)) {

                ClickHouseBinaryFormatReader reader =
                        new RowBinaryWithNamesAndTypesFormatReader(response.getInputStream(), response.getSettings());

                reader.next();

                LocalDateTime serverTime = reader.getLocalDateTime(1); // in "America/Los_Angeles"
                System.out.println("Server time: " + serverTime);
                LocalDateTime serverUtcTime = reader.getLocalDateTime(2);
                System.out.println("Server UTC time: " + serverUtcTime);
                ZonedDateTime serverLisbonTime = reader.getZonedDateTime(3);  // in "Europe/Lisbon"
                System.out.println("Server Lisbon time: " + serverLisbonTime);

                ZonedDateTime serverTimeZ = serverTime.atZone(ZoneId.of("America/Los_Angeles"));
                ZonedDateTime serverUtcTimeZ = serverUtcTime.atZone(ZoneId.of("UTC"));

                Assert.assertEquals(serverTimeZ.withZoneSameInstant(ZoneId.of("UTC")), serverUtcTimeZ);
                Assert.assertEquals(serverLisbonTime.withZoneSameInstant(ZoneId.of("UTC")), serverUtcTimeZ);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Failed to get server time zone from header", e);
        }
    }

    private Client.Builder newClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), false)
                .setUsername("default")
                .setPassword("")
                .compressClientRequest(false)
                .compressServerResponse(false)
                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "true").equals("true"));
    }
}
