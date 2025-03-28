package com.clickhouse.client.query;


import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.DataTypeUtils;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.command.CommandResponse;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.NullValueException;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseVersion;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.util.Strings;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class QueryTests extends BaseIntegrationTest {

    private final static Random RANDOM = new Random();

    private Client client;

    private boolean useServerCompression = false;

    private boolean useHttpCompression = false;

    private boolean usePreallocatedBuffers = false;

    QueryTests(){
    }

    public QueryTests(boolean useServerCompression, boolean useHttpCompression) {
        this(useServerCompression, useHttpCompression, false);
    }

    public QueryTests(boolean useServerCompression, boolean useHttpCompression, boolean usePreallocatedBuffers) {
        this.useServerCompression = useServerCompression;
        this.useHttpCompression = useHttpCompression;
        this.usePreallocatedBuffers = usePreallocatedBuffers;
    }

    @BeforeMethod(groups = {"integration"})
    public void setUp() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = newClient().build();

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
        List<Map<String, Object>> dataset = prepareDataSet(DATASET_TABLE, DATASET_COLUMNS, DATASET_VALUE_GENERATORS, 10);

        Records records = client.queryRecords("SELECT * FROM " + DATASET_TABLE).get(3, TimeUnit.SECONDS);
        Assert.assertEquals(records.getResultRows(), 10, "Unexpected number of rows");

        Iterator<Map<String, Object>> dataIterator = dataset.iterator();
        for (GenericRecord record : records) {
            Map<String,Object> dsRecords = dataIterator.next();
            Assert.assertEquals(record.getLong("col1"), dsRecords.get("col1"));
            Assert.assertEquals(record.getInteger("col2"), dsRecords.get("col2"));
            Assert.assertEquals(record.getString("col3"), dsRecords.get("col3"));
            Assert.assertEquals(record.getLong("col4"), dsRecords.get("col4"));
            Assert.assertEquals(record.getString("col5"), dsRecords.get("col5"));
            Assert.assertEquals(record.getBooleanArray("col6"), ((List)dsRecords.get("col6")).toArray());
            Assert.assertEquals(record.getIntArray("col7"), ((List)dsRecords.get("col7")).toArray());
        }
    }

    @Test(groups = {"integration"})
    public void testBigUnsignedInt() throws Exception {
        final BigInteger expected128 = BigInteger.valueOf(2).pow(128).subtract(BigInteger.ONE).subtract(BigInteger.ONE);
        final BigInteger expected256 = BigInteger.valueOf(2).pow(256).subtract(BigInteger.ONE).subtract(BigInteger.ONE);

        String sqlQuery = "SELECT toUInt128('" + expected128 + "') as i128, toUInt256('" + expected256 + "') as i256";
        Records records = client.queryRecords(sqlQuery).get(3, TimeUnit.SECONDS);

        GenericRecord firstRecord = records.iterator().next();

        Assert.assertEquals(firstRecord.getBigInteger("i128"), expected128);
        Assert.assertEquals(firstRecord.getBigInteger("i256"), expected256);
    }

    @Test(groups = {"integration"})
    public void testEndianReadingNumbers() throws Exception {

        byte[][] numbers = new byte[][] {
            new byte[] {0x00, 0x02, 0x00, 0x01},
            new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
            new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10},
        };


        for (byte[] number : numbers) {
            String typeName = "UInt32";
            if (number.length == 8) {
                typeName = "UInt64";
            } else if (number.length == 16) {
                typeName = "UInt128";
            }
            BigInteger expected = new BigInteger(number);
            String sqlQuery = "SELECT to" + typeName + "('" + expected + "') as value1";
            System.out.println(sqlQuery);
            Records records = client.queryRecords(sqlQuery).get(3, TimeUnit.SECONDS);
            GenericRecord firstRecord = records.iterator().next();

            if (number.length == 4) {
                System.out.println(firstRecord.getLong("value1"));
                Assert.assertEquals(firstRecord.getLong("value1"), expected.longValue());
            } else {
                System.out.println(firstRecord.getBigInteger("value1"));
                Assert.assertEquals(firstRecord.getBigInteger("value1"), expected);
            }
        }
    }

    @Test(groups = {"integration"})
    public void testReadRecordsWithStreamAPI() throws Exception {
        final int tables = 10;

        Set<String> expectedTableNames = new HashSet<>();
        for (int i = 0; i < tables; i++) {
            final String tableName = "a_" + i;
            expectedTableNames.add(tableName);
            client.execute("DROP TABLE IF EXISTS " + tableName);
            client.execute("CREATE TABLE " + tableName +" (x UInt32) ENGINE = MergeTree ORDER BY ()");
        }

        Records records = client.queryRecords("SHOW TABLES").get(3, TimeUnit.SECONDS);

        HashSet<String> tableNames = new HashSet<>();
        records.forEach(r -> {
            tableNames.add(r.getString(1));
        });
        Assert.assertTrue(tableNames.containsAll(expectedTableNames));

        Assert.expectThrows(IllegalStateException.class, () -> {
            records.forEach(r -> {
                System.out.println(r);
            });
        });
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

    @Test(description = "Verifies correct handling of empty data set while column information is present", groups = {"integration"})
    public void testQueryRecordsOnEmptyDataset() throws Exception {
        Records records = client.queryRecords("SELECT 1 LIMIT 0").get(3, TimeUnit.SECONDS);

        Iterator<GenericRecord> iter = records.iterator();
        Assert.assertFalse(iter.hasNext());
    }

    @Test(description = "Verifies correct handling when no column information expected", groups = {"integration"})
    public void testQueryRecordsWithEmptyResult() throws Exception {
        // This test uses a query that returns no data and no column information
        try (Records records = client.queryRecords("CREATE DATABASE IF NOT EXISTS test_db").get(3, TimeUnit.SECONDS)) {
            Assert.assertTrue(records.isEmpty());
            for (GenericRecord record : records) {
               Assert.fail("unexpected record: " + record);
            }
        }
    }

    @Test(groups = {"integration"})
    public void testQueryAll() throws Exception {
        List<Map<String, Object>> dataset = prepareDataSet(DATASET_TABLE, DATASET_COLUMNS, DATASET_VALUE_GENERATORS, 10);
        List<GenericRecord> records = client.queryAll("SELECT * FROM " + DATASET_TABLE + " LIMIT " + dataset.size());
        Assert.assertFalse(records.isEmpty());

        for (String colDefinition : DATASET_COLUMNS) {
            // result values
            String colName = colDefinition.split(" ")[0];
            List<Object> colValues = records.stream().map(r -> {
                Object v = r.getObject(colName);
                if (v instanceof BinaryStreamReader.ArrayValue) {
                    v = ((BinaryStreamReader.ArrayValue)v).asList();
                }

                return v;
            }

            ).collect(Collectors.toList());
            Assert.assertEquals(colValues.size(), dataset.size());

            // dataset values
            List<Object> dataValue = dataset.stream().map(d -> d.get(colName)).collect(Collectors.toList());
            Assert.assertEquals(colValues, dataValue, "Failed for column " + colName);
        }
    }

    @Test(groups = {"integration"})
    public void testQueryAllSimple() throws Exception {
        testQueryAllSimple(10);
    }
    public void testQueryAllSimple(int numberOfRecords) throws Exception {
        GenericRecord record = client.queryAll("SELECT number FROM system.numbers LIMIT " + numberOfRecords).stream().findFirst().get();
        Assert.assertNotNull(record);
    }

    @Test(groups = {"integration"})
    public void testQueryAllNoResult() throws Exception {
        List<GenericRecord> records = client.queryAll("SELECT 1 LIMIT 0");
        Assert.assertEquals(records.size(), 0);
        Assert.assertTrue(records.isEmpty());
    }

    @Test
    public void testQueryAllTableNames() {
        final int tables = 10;
        Set<String> expectedTableNames = new HashSet<>();
        for (int i = 0; i < tables; i++) {
            final String tableName = "a_" + i;
            expectedTableNames.add(tableName);
            client.execute("DROP TABLE IF EXISTS " + tableName);
            client.execute("CREATE TABLE " + tableName +" (x UInt32) ENGINE = MergeTree ORDER BY ()");
        }

        List<GenericRecord> records = client.queryAll("SHOW TABLES");
        Assert.assertTrue(records.size() >= tables);

        Set<String> tableNames = records.stream().map(r -> r.getString(1)).collect(Collectors.toSet());
        Assert.assertTrue(tableNames.containsAll(expectedTableNames));
    }

    @Test(groups = {"integration"})
    public void testQueryJSONEachRow() throws ExecutionException, InterruptedException {
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
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(queryResponse, tableSchema);

        Iterator<Map<String, Object>> dataIterator = data.iterator();
        int rowsCount = 0;
        while (dataIterator.hasNext()) {
            Map<String, Object> expectedRecord = dataIterator.next();
            Map<String, Object> actualRecord = reader.next();
            for (Map.Entry<String, Object> entry : actualRecord.entrySet()) {
                Object value = entry.getValue();
                if (entry.getValue() instanceof BinaryStreamReader.ArrayValue) {
                    value = ((BinaryStreamReader.ArrayValue)value).asList();
                }

                Assert.assertEquals(value, expectedRecord.get(entry.getKey()), "Value of " + entry.getKey() + " doesn't match: "
                    + expectedRecord.get(entry.getKey()) + " expected, actual: " + value);

            }
            rowsCount++;
        }

        Assert.assertEquals(rowsCount, rows);
    }

    @Test
    public void testReadingArrayInNative() throws Exception {

        QuerySettings querySettings = new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        try (QueryResponse response = client.query("SELECT [1, 2, 3] as arr1, [[1, 2, 3], [4, 5, 6]] as arr2", querySettings).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);

            Map<String, Object> record = reader.next();
            Assert.assertEquals(((BinaryStreamReader.ArrayValue)record.get("arr1")).asList(), Arrays.asList((short)1, (short)2, (short)3));
            Assert.assertEquals(((BinaryStreamReader.ArrayValue)record.get("arr2")).asList().get(0), Arrays.asList((short)1, (short)2, (short)3));
            Assert.assertEquals(((BinaryStreamReader.ArrayValue)record.get("arr2")).asList().get(1), Arrays.asList((short)4, (short)5, (short)6));
        }
    }

    @Test(groups = {"integration"})
    public void testBinaryStreamReader() throws Exception {
        final String table = "dynamic_schema_test_table";
        List<Map<String, Object>> data = prepareDataSet(table, DATASET_COLUMNS,
                DATASET_VALUE_GENERATORS, 10);
        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        Future<QueryResponse> response = client.query("SELECT col1, col3, hostname() as host FROM " + table, settings);
        QueryResponse queryResponse = response.get();


        TableSchema schema = new TableSchema(
                Arrays.asList(
                        ClickHouseColumn.of("col1", "UInt32"),
                        ClickHouseColumn.of("col3", "String"),
                        ClickHouseColumn.of("host", "String")
                ));
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(queryResponse, schema);
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
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(queryResponse);

        Map<String, Object> record;
        for (int i = 0; i < rows; i++) {
            record = reader.next();
            Assert.assertNotNull(record);
        }
    }

    private final static List<String> ARRAY_COLUMNS = Arrays.asList(
            "col1 Array(UInt32)",
            "col2 Array(Array(Int32))",
            "col3 Array(UInt64)",
            "col4 Array(Bool)",
            "col5 Array(String)"
    );

    private final static List<Function<String, Object>> ARRAY_VALUE_GENERATORS = Arrays.asList(
            c ->
                    RANDOM.ints(10, 0, 100)
                            .asLongStream().collect(ArrayList::new, ArrayList::add, ArrayList::addAll),
            c -> {
                List<List<Integer>> values = new ArrayList<>();

                for (int i = 0; i < 10; i++) {
                    values.add(Arrays.asList(i, 2 * i , 3 * i));
                }
                return values;
            },
            c ->
                RANDOM.longs(10, 0, Long.MAX_VALUE)
                        .mapToObj(BigInteger::valueOf).collect(Collectors.toList()),
            c -> RANDOM.ints(10, 0, 1)
                    .mapToObj(i -> i == 0 ).collect(Collectors.toList()),
            c -> {
                UUID uuid = UUID.randomUUID();
                byte[] bts = ByteBuffer.allocate(16)
                        .putLong(uuid.getMostSignificantBits())
                        .putLong(uuid.getLeastSignificantBits())
                        .array();
                String sep = "\\x";
                String hex = sep + BaseEncoding.base16().withSeparator(sep, 2).encode(bts);
                return Arrays.asList(hex);
            }
    );

    @Test(groups = {"integration"})
    public void testBinaryReaderOnQueryWithNoResult() throws Exception {
        try (QueryResponse response = client.query("SELECT 1 LIMIT 0").get(3, TimeUnit.SECONDS)) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            Assert.assertFalse(reader.hasNext());
            Assert.assertNull(reader.next());
        }
    }

    @Test(groups = {"integration"})
    public void testArrayValues() throws Exception {
        final String table = "array_values_test_table";
        final int rows = 1;
        List<Map<String, Object>> data = prepareDataSet(table, ARRAY_COLUMNS, ARRAY_VALUE_GENERATORS, rows);

        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        Future<QueryResponse> response = client.query("SELECT * FROM " + table, settings);
        TableSchema schema = client.getTableSchema(table);

        QueryResponse queryResponse = response.get();
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(queryResponse, schema);

        Map<String, Object> record = reader.next();
        Assert.assertNotNull(record);
        Map<String, Object> datasetRecord = data.get(0);
        long[] col1Values = reader.getLongArray("col1");
        Assert.assertEquals(Arrays.stream(col1Values).collect(ArrayList<Long>::new, ArrayList::add,
                ArrayList::addAll), datasetRecord.get("col1"));
        Assert.assertEquals(reader.getList("col1"), datasetRecord.get("col1"));
        List<List<Long>> col2Values = reader.getList("col2");
        Assert.assertEquals(col2Values, data.get(0).get("col2"));
        List<BigInteger> col3Values = reader.getList("col3");
        Assert.assertEquals(col3Values, data.get(0).get("col3"));
        List<Boolean> col4Values = reader.getList("col4");
        Assert.assertEquals(col4Values, data.get(0).get("col4"));
        boolean[] col4Array = reader.getBooleanArray("col4");
        Assert.assertEquals(col4Array, ((List)data.get(0).get("col4")).toArray());
        Assert.assertEquals(reader.getList("col5"), ((List)data.get(0).get("col5")));
    }

    @Test
    public void testArraysAsList() {
        GenericRecord record =
                client.queryAll("SELECT [] as empty_array").get(0);

        List<Object> items = record.getList("empty_array");
        Assert.assertTrue(items.isEmpty());
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
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(queryResponse, schema);

        Map<String, Object> record = reader.next();
        Assert.assertNotNull(record);
//        System.out.println("col1: " + Arrays.toString(col1Values));
        System.out.println("Record: " + record);
    }


    @Test(groups = {"integration"})
    public void testQueryExceptionHandling() throws Exception {

        try {
            client.queryRecords("SELECT * FROM unknown_table").get(3, TimeUnit.SECONDS);
            Assert.fail("exception is expected");
        } catch (ServerException e) {
            Assert.assertEquals(e.getCode(), 60);
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof ServerException);
        } catch (ClientException e) {
            // expected
        }
    }

    @Test
    public void testQueryRecordsEmptyResult() throws Exception {
        try (Records records = client.queryRecords("SELECT 1 LIMIT 0").get(3, TimeUnit.SECONDS)) {
            Assert.assertTrue(records.isEmpty());
            for (GenericRecord record : records) {
               Assert.fail("unexpected record: " + record);
            }
        }
    }

    @Test(description = "Verifies that queryRecords reads all values from the response", groups = {"integration"})
    public void testQueryRecordsReadsAllValues() throws Exception {
        try (Records records = client.queryRecords("SELECT toInt32(number) FROM system.numbers LIMIT 3").get(3, TimeUnit.SECONDS)) {
            Assert.assertFalse(records.isEmpty());
            Assert.assertEquals(records.getResultRows(), 3);

            int expectedNumber = 0;
            for (GenericRecord record : records) {
                Assert.assertEquals(record.getInteger(1), expectedNumber);
                expectedNumber++;
            }

            Assert.assertEquals(expectedNumber, 3);
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
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(queryResponse, schema);

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
                Assert.assertTrue(reader.hasValue(i+1), "No value for column " + i);

            } else {
                Assert.assertFalse(record.containsKey(columnName));
                Assert.assertNull(record.get(columnName));
                Assert.assertFalse(reader.hasValue(columnName));
                Assert.assertFalse(reader.hasValue(i+1));

                if (columnName.equals("col1") || columnName.equals("col2")) {
                    Assert.expectThrows(NullValueException.class, () -> reader.getLong(columnName));
                }
            }

            i++;
        }
    }

    @Test
    public void testIPAddresses() throws Exception {

        final List<String> columns = Arrays.asList(
                "srcV4 IPv4",
                "targetV4 IPv4",
                "srcV6 IPv6",
                "targetV6 IPv6"

        );

        Random random = new Random();
        byte[] ipv4 = new byte[4];
        random.nextBytes(ipv4);
        InetAddress ipv4src = Inet4Address.getByAddress(ipv4);
        random.nextBytes(ipv4);
        InetAddress ipv4target = Inet4Address.getByAddress(ipv4);
        byte[] ipv6 = new byte[16];
        random.nextBytes(ipv6);
        InetAddress ipv6src = Inet6Address.getByAddress(ipv6);
        random.nextBytes(ipv6);
        InetAddress ipv6target = Inet6Address.getByAddress(ipv6);


        final List<Supplier<String>> valueGenerators = Arrays.asList(
                () -> sq(ipv4src.getHostAddress()),
                () -> sq(ipv4target.getHostAddress()),
                () -> sq(ipv6src.getHostAddress()),
                () -> sq(ipv6target.getHostAddress())
        );

        final List<Consumer<ClickHouseBinaryFormatReader>> verifiers = new ArrayList<>();
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("srcV4"), "No value for column srcV4 found");
            Assert.assertEquals(r.getInet4Address("srcV4"), ipv4src);
            Assert.assertEquals(r.getInet4Address(1), ipv4src);
        });

        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("targetV4"), "No value for column targetV4 found");
            Assert.assertEquals(r.getInet4Address("targetV4"), ipv4target);
            Assert.assertEquals(r.getInet4Address(2), ipv4target);
        });

        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("srcV6"), "No value for column src6 found");
            Assert.assertEquals(r.getInet6Address("srcV6"), ipv6src);
            Assert.assertEquals(r.getInet6Address(3), ipv6src);
        });

        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("targetV6"), "No value for column targetV6 found");
            Assert.assertEquals(r.getInet6Address("targetV6"), ipv6target);
            Assert.assertEquals(r.getInet6Address(4), ipv6target);
        });

        testDataTypes(columns, valueGenerators, verifiers);
    }

    @Test
    public void testDateTimeDataTypes() {
        final List<String> columns = Arrays.asList(
                "min_date Date",
                "max_date Date",
                "min_dateTime DateTime",
                "max_dateTime DateTime",
                "min_dateTime64 DateTime64",
                "max_dateTime64 DateTime64",
                "min_dateTime64_6 DateTime64(6)",
                "max_dateTime64_6 DateTime64(6)",
                "min_dateTime64_9 DateTime64(9)",
                "max_dateTime64_9 DateTime64(9)"
        );

        final LocalDate minDate = LocalDate.parse("1970-01-01");
        final LocalDate maxDate = LocalDate.parse("2149-06-06");
        final LocalDateTime minDateTime = LocalDateTime.parse("1970-01-01T01:02:03");
        final LocalDateTime maxDateTime = LocalDateTime.parse("2106-02-07T06:28:15");
        final LocalDateTime minDateTime64 = LocalDateTime.parse("1970-01-01T01:02:03.123");
        final LocalDateTime maxDateTime64 = LocalDateTime.parse("2106-02-07T06:28:15.123");
        final LocalDateTime minDateTime64_6 = LocalDateTime.parse("1970-01-01T01:02:03.123456");
        final LocalDateTime maxDateTime64_6 = LocalDateTime.parse("2106-02-07T06:28:15.123456");
        final LocalDateTime minDateTime64_9 = LocalDateTime.parse("1970-01-01T01:02:03.123456789");
        final LocalDateTime maxDateTime64_9 = LocalDateTime.parse("2106-02-07T06:28:15.123456789");
        final List<Supplier<String>> valueGenerators = Arrays.asList(
                () -> sq(minDate.toString()),
                () -> sq(maxDate.toString()),
                () -> sq(minDateTime.format(DataTypeUtils.DATETIME_FORMATTER)),
                () -> sq(maxDateTime.format(DataTypeUtils.DATETIME_FORMATTER)),
                () -> sq(minDateTime64.format(DataTypeUtils.DATETIME_WITH_NANOS_FORMATTER)),
                () -> sq(maxDateTime64.format(DataTypeUtils.DATETIME_WITH_NANOS_FORMATTER)),
                () -> sq(minDateTime64_6.format(DataTypeUtils.DATETIME_WITH_NANOS_FORMATTER)),
                () -> sq(maxDateTime64_6.format(DataTypeUtils.DATETIME_WITH_NANOS_FORMATTER)),
                () -> sq(minDateTime64_9.format(DataTypeUtils.DATETIME_WITH_NANOS_FORMATTER)),
                () -> sq(maxDateTime64_9.format(DataTypeUtils.DATETIME_WITH_NANOS_FORMATTER))
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
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("min_dateTime64"), "No value for column min_dateTime64 found");
            Assert.assertEquals(r.getLocalDateTime("min_dateTime64"), minDateTime64);
            Assert.assertEquals(r.getLocalDateTime(5), minDateTime64);
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("max_dateTime64"), "No value for column max_dateTime64 found");
            Assert.assertEquals(r.getLocalDateTime("max_dateTime64"), maxDateTime64);
            Assert.assertEquals(r.getLocalDateTime(6), maxDateTime64);
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("min_dateTime64_6"), "No value for column min_dateTime64_6 found");
            Assert.assertEquals(r.getLocalDateTime("min_dateTime64_6"), minDateTime64_6);
            Assert.assertEquals(r.getLocalDateTime(7), minDateTime64_6);
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("max_dateTime64_6"), "No value for column max_dateTime64_6 found");
            Assert.assertEquals(r.getLocalDateTime("max_dateTime64_6"), maxDateTime64_6);
            Assert.assertEquals(r.getLocalDateTime(8), maxDateTime64_6);
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("min_dateTime64_9"), "No value for column min_dateTime64_9 found");
            Assert.assertEquals(r.getLocalDateTime("min_dateTime64_9"), minDateTime64_9);
            Assert.assertEquals(r.getLocalDateTime(9), minDateTime64_9);
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("max_dateTime64_9"), "No value for column max_dateTime64_9 found");
            Assert.assertEquals(r.getLocalDateTime("max_dateTime64_9"), maxDateTime64_9);
            Assert.assertEquals(r.getLocalDateTime(10), maxDateTime64_9);
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
            final BigInteger nearMaxInt = BigInteger.valueOf(2).pow(bits - 1).subtract(BigInteger.ONE).subtract(BigInteger.ONE);//LE vs BigEndian test
            final BigInteger nearMaxUInt = BigInteger.valueOf(2).pow(bits).subtract(BigInteger.ONE).subtract(BigInteger.ONE);//LE vs BE

            valueGenerators.add(() -> String.valueOf(minInt));
            valueGenerators.add(() -> String.valueOf(0));
            valueGenerators.add(() -> String.valueOf(nearMaxInt));
            valueGenerators.add(() -> String.valueOf(nearMaxUInt));

            final int index = i - 3;
            verifiers.add(createNumberVerifier("min_int" + bits, index * 4 + 1, bits, true,
                    minInt));
            verifiers.add(createNumberVerifier("min_uint" + bits, index * 4 + 2, bits, false,
                    BigInteger.ZERO));
            verifiers.add(createNumberVerifier("max_int" + bits, index * 4 + 3, bits, true,
                    nearMaxInt));
            verifiers.add(createNumberVerifier("max_uint" + bits, index * 4 + 4, bits, false,
                    nearMaxUInt));
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
    public void testArrayTuples() {
        final List<String> columns = Arrays.asList(
            "col1 Array(Tuple(UInt32, String))",
            "col2 Array(Tuple(UInt32, String, Float32))",
            "col3 Array(Tuple(UInt32, Tuple(Float32, String)))"
        );

        final List<Supplier<String>> valueGenerators = Arrays.asList(
            () -> "[(1, 'value1'), (2, 'value2')]",
            () -> "[(1, 'value2', 23.43), (2, 'value3', 43.21)]",
            () -> "[(1, (23.43, 'value3')), (2, (43.21, 'value4'))]"
        );

        final List<Consumer<ClickHouseBinaryFormatReader>> verifiers = new ArrayList<>();
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("col1"), "No value for column col1 found");
            Assert.assertEquals(r.getList("col1").get(0), new Object[]{1L, "value1"});
            Assert.assertEquals(r.getList("col1").get(1), new Object[]{2L, "value2"});
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("col2"), "No value for column col2 found");
            Assert.assertEquals(r.getList("col2").get(0),  new Object[]{1L, "value2", 23.43f});
            Assert.assertEquals(r.getList("col2").get(1), new Object[]{2L, "value3", 43.21f});
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("col3"), "No value for column col2 found");
            Assert.assertEquals(r.getList("col3").get(0), new Object[]{1L, new Object[]{23.43f, "value3"}});
            Assert.assertEquals(r.getList("col3").get(1), new Object[]{2L, new Object[]{43.21f, "value4"}});
        });

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
            Assert.assertEquals(r.getString(1), "value1");

        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("max_enum16"), "No value for column max_enum16 found");
            Assert.assertEquals(r.getEnum16("max_enum16"), (short) 32767);
            Assert.assertEquals(r.getEnum16(2), (short) 32767);
            Assert.assertEquals(r.getString(2), "value3");
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("min_enum8"), "No value for column min_enum8 found");
            Assert.assertEquals(r.getEnum8("min_enum8"), (byte) -128);
            Assert.assertEquals(r.getEnum8(3), (byte) -128);
            Assert.assertEquals(r.getString(3), "value1");
        });
        verifiers.add(r -> {
            Assert.assertTrue(r.hasValue("max_enum8"), "No value for column max_enum8 found");
            Assert.assertEquals(r.getEnum8("max_enum8"), (byte) 127);
            Assert.assertEquals(r.getEnum8(4), (byte) 127);
            Assert.assertEquals(r.getString(4), "value3");
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
                () -> sq("7 chars\0\0\0"),
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


    @Test
    public void testNumberToStringConvertions() throws Exception {

        GenericRecord record =
                client.queryAll("SELECT '100' as small_number, '100500' as number").get(0);

        Assert.assertEquals(record.getString("number"), "100500");
        Assert.assertEquals(record.getString(2), "100500");
        Assert.assertEquals(record.getString("small_number"), "100");
        Assert.assertEquals(record.getByte("small_number"), 100);
        Assert.assertEquals(record.getShort("small_number"), 100);
        Assert.assertEquals(record.getShort(1), 100);
        Assert.assertThrows(() -> record.getShort("number"));
        Assert.assertEquals(record.getInteger("number"), 100500);
        Assert.assertEquals(record.getInteger(2), 100500);
        Assert.assertEquals(record.getLong("number"), 100500L);
        Assert.assertEquals(record.getFloat("number"), 100500.0F);
        Assert.assertEquals(record.getBigInteger("number"), BigInteger.valueOf(100500L));

    }

    private static String sq(String str) {
        return "\'" + str + "\'";
    }

    public void testDataTypes(List<String> columns, List<Supplier<String>> valueGenerators, List<Consumer<ClickHouseBinaryFormatReader>> verifiers) {
        final String table = "data_types_test_table";

        try {
            // Drop table
            client.execute("DROP TABLE IF EXISTS " + table).get(10, TimeUnit.SECONDS);

            // Create table
            StringBuilder createStmtBuilder = new StringBuilder();
            createStmtBuilder.append("CREATE TABLE IF NOT EXISTS ").append(table).append(" (");
            for (String column : columns) {
                createStmtBuilder.append(column).append(", ");
            }
            createStmtBuilder.setLength(createStmtBuilder.length() - 2);
            createStmtBuilder.append(") ENGINE = MergeTree ORDER BY tuple()");
            client.execute(createStmtBuilder.toString()).get(10, TimeUnit.SECONDS);


            // Insert data
            StringBuilder insertStmtBuilder = new StringBuilder();
            insertStmtBuilder.append("INSERT INTO ").append(table).append(" VALUES ");
            insertStmtBuilder.append("(");
            for (Supplier<String> valueSupplier : valueGenerators) {
                insertStmtBuilder.append(valueSupplier.get()).append(", ");
            }
            insertStmtBuilder.setLength(insertStmtBuilder.length() - 2);
            insertStmtBuilder.append("), ");
            insertStmtBuilder.setLength(insertStmtBuilder.length() - 2);

            client.execute(insertStmtBuilder.toString()).get(10, TimeUnit.SECONDS);
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
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(queryResponse, schema);
            Assert.assertNotNull(reader.next());
            Assert.assertEquals(verifiers.size(), columns.size(), "Number of verifiers should match number of columns");
            int colIndex = 0;
            for (Consumer<ClickHouseBinaryFormatReader> verifier : verifiers) {
                colIndex++;
                try {
                    verifier.accept(reader);
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

        try (QueryResponse response = client.query("SELECT * FROM " + DATASET_TABLE + " LIMIT 3", settings).get()) {
            // Stats should be available after the query is done
            OperationMetrics metrics = response.getMetrics();

            Assert.assertEquals(metrics.getMetric(ServerMetrics.NUM_ROWS_READ).getLong(), 10); // 10 rows in the table
            Assert.assertEquals(metrics.getMetric(ServerMetrics.RESULT_ROWS).getLong(), 3);
        }

        StringBuilder insertStmtBuilder = new StringBuilder();
        insertStmtBuilder.append("INSERT INTO ").append(DATASET_TABLE).append(" VALUES ");
        final int rowsToInsert = 5;
        for (int i = 0; i < rowsToInsert; i++) {
            insertStmtBuilder.append("(");
            Map<String, Object> values = writeValuesRow(insertStmtBuilder, DATASET_COLUMNS, DATASET_VALUE_GENERATORS);
            insertStmtBuilder.setLength(insertStmtBuilder.length() - 2);
            insertStmtBuilder.append("), ");
        }
        try (QueryResponse response = client.query(insertStmtBuilder.toString(), settings).get()) {

            OperationMetrics metrics = response.getMetrics();

            Assert.assertEquals(metrics.getMetric(ServerMetrics.NUM_ROWS_READ).getLong(), rowsToInsert); // 10 rows in the table
            Assert.assertEquals(metrics.getMetric(ServerMetrics.RESULT_ROWS).getLong(), rowsToInsert);
            Assert.assertEquals(response.getReadRows(), rowsToInsert);
            Assert.assertTrue(metrics.getMetric(ClientMetrics.OP_DURATION).getLong() > 0);
            Assert.assertEquals(metrics.getQueryId(), uuid);
            Assert.assertEquals(response.getQueryId(), uuid);
        }

        try (QueryResponse response = client.query("SELECT number FROM system.numbers LIMIT 30", settings).get()) {
            // Stats should be available after the query is done
            OperationMetrics metrics = response.getMetrics();

            Assert.assertEquals(metrics.getMetric(ServerMetrics.NUM_ROWS_READ).getLong(), 30);
            Assert.assertTrue(metrics.getMetric(ServerMetrics.RESULT_ROWS).getLong() > 0);
        }
    }

    private final static List<String> DATASET_COLUMNS = Arrays.asList(
            "col1 UInt32",
            "col2 Int32",
            "col3 String",
            "col4 Int64",
            "col5 String",
            "col6 Array(Bool)",
            "col7 Array(Int32)"
    );

    private final static List<Function<String, Object>> DATASET_VALUE_GENERATORS = Arrays.asList(
            c -> Long.valueOf(RANDOM.nextInt(Integer.MAX_VALUE)),
            c -> RANDOM.nextInt(Integer.MAX_VALUE),
            c -> "value_" + RANDOM.nextInt(Integer.MAX_VALUE),
            c -> Long.valueOf(RANDOM.nextInt(Integer.MAX_VALUE)),
            c -> "value_" + RANDOM.nextInt(Integer.MAX_VALUE),
            c -> RANDOM.ints(10, 0, 1).mapToObj(i -> i == 0).collect(Collectors.toList()),
            c -> RANDOM.ints(10, 0, Integer.MAX_VALUE).boxed().collect(Collectors.toList())
    );

    private final static String DATASET_TABLE = "query_test_table";

    private Map<String, Object> prepareSimpleDataSet() {
        return prepareDataSet(DATASET_TABLE, DATASET_COLUMNS, DATASET_VALUE_GENERATORS, 1).get(0);
    }

    private List<Map<String, Object>> prepareDataSet(String table, List<String> columns, List<Function<String, Object>> valueGenerators,
                                                     int rows) {
        List<Map<String, Object>> data = new ArrayList<>(rows);

        try {
            // Drop table
            client.execute("DROP TABLE IF EXISTS " + table).get(10, TimeUnit.SECONDS);

            // Create table
            CommandSettings settings = new CommandSettings();
            if (isVersionMatch("[24.8,)")) {
                settings.serverSetting("allow_experimental_variant_type", "1")
                        .serverSetting("allow_experimental_dynamic_type", "1")
                        .serverSetting("allow_experimental_json_type", "1");
            }
            StringBuilder createStmtBuilder = new StringBuilder();
            createStmtBuilder.append("CREATE TABLE IF NOT EXISTS ").append(table).append(" (");
            for (String column : columns) {
                createStmtBuilder.append(column).append(", ");
            }
            createStmtBuilder.setLength(createStmtBuilder.length() - 2);
            createStmtBuilder.append(") ENGINE = MergeTree ORDER BY tuple()");
            client.execute(createStmtBuilder.toString(), settings).get(10, TimeUnit.SECONDS);

            // Insert data
            StringBuilder insertStmtBuilder = new StringBuilder();
            insertStmtBuilder.append("INSERT INTO ").append(table).append(" VALUES ");
            for (int i = 0; i < rows; i++) {
                insertStmtBuilder.append("(");
                Map<String, Object> values = writeValuesRow(insertStmtBuilder, columns, valueGenerators);
                insertStmtBuilder.setLength(insertStmtBuilder.length() - 2);
                insertStmtBuilder.append("), ");
                data.add(values);
            }
            insertStmtBuilder.setLength(insertStmtBuilder.length() - 2);
            client.execute(insertStmtBuilder.toString()).get(10, TimeUnit.SECONDS);
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
            } else if (value instanceof List) {
                insertStmtBuilder.append("[");
                for (Object item : (List)value) {
                    insertStmtBuilder.append(quoteValue(item)).append(", ");
                }
                insertStmtBuilder.setLength(insertStmtBuilder.length() - 2);
                insertStmtBuilder.append("], ");
            } else {
                insertStmtBuilder.append(value).append(", ");
            }
            values.put(columnIterator.next().split(" ")[0], value);

        }
        return values;
    }

    private String quoteValue(Object value) {
        if (value instanceof String) {
            String strVal = (String)value;

            return '\'' + strVal.replaceAll("\\\\", "\\\\\\\\") + '\'';
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

        client.execute("DROP TABLE IF EXISTS " + table).get();
        client.execute("CREATE TABLE " + table + " (col1 UInt32, col2 String) ENGINE = MergeTree ORDER BY tuple()").get();

        ByteArrayOutputStream insertData = new ByteArrayOutputStream();
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(insertData))) {
            writer.write("1\t'one'\n");
            writer.write("2\t'two'\n");
            writer.write("3\t'three'\n");
        }
        InsertSettings insertSettings = new InsertSettings();
        client.insert(table, new ByteArrayInputStream(insertData.toByteArray()), ClickHouseFormat.TabSeparated, insertSettings).get();

        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("param1", 2);
        QueryResponse queryResponse =
                client.query("SELECT * FROM " + table + " WHERE col1 >= {param1:UInt32}", queryParams).get();

        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(queryResponse);
        int count = 0;
        while (reader.hasNext()) {
            reader.next();
            count++;
            Assert.assertTrue(reader.getInteger("col1") >=2 );
        }
        Assert.assertEquals(count, 2);

        try (Records records = client.queryRecords("SELECT * FROM " + table + " WHERE col1 >= {param1:UInt32}", queryParams).get()) {
            count = 0;
            for (GenericRecord record : records) {
                Assert.assertTrue((Integer) record.getInteger("col1") >= 2);
                count++;
            }
            Assert.assertEquals(count, 2);
        }

        List<GenericRecord> allRecords = client.queryAll("SELECT * FROM " + table + " WHERE col1 >= {param1:UInt32}", queryParams);
        for (GenericRecord record : allRecords) {
            Assert.assertTrue((Integer) record.getInteger("col1") >= 2);
        }
        Assert.assertEquals(allRecords.size(), 2);
    }

    @Test(groups = {"integration"})
    public void testExecuteQueryParam() throws ExecutionException, InterruptedException, TimeoutException {

        final String table = "execute_query_test";
        Map<String, Object> query_param = new HashMap<>();
        query_param.put("table_name",table);
        query_param.put("engine","MergeTree");
        client.execute("DROP TABLE IF EXISTS " + table).get(10, TimeUnit.SECONDS);
        client.execute("CREATE TABLE {table_name:Identifier} ( id UInt32, name String, created_at DateTime) ENGINE = MergeTree ORDER BY tuple()", query_param)
                .get(10, TimeUnit.SECONDS);

        TableSchema schema = client.getTableSchema(table);
        Assert.assertNotNull(schema);
    }

    @Test(groups = {"integration"})
    public void testExecuteQueryParamCommandSettings() throws ExecutionException, InterruptedException, TimeoutException {

        final String table = "execute_query_test";
        String q1Id = UUID.randomUUID().toString();
        Map<String, Object> query_param = new HashMap<>();
        query_param.put("table_name",table);
        query_param.put("engine","MergeTree");
        client.execute("DROP TABLE IF EXISTS " + table).get(10, TimeUnit.SECONDS);
        client.execute("CREATE TABLE {table_name:Identifier} ( id UInt32, name String, created_at DateTime) ENGINE = MergeTree ORDER BY tuple()",
                        query_param, (CommandSettings) new CommandSettings().setQueryId(q1Id))
                .get(10, TimeUnit.SECONDS);

        TableSchema schema = client.getTableSchema(table);
        Assert.assertNotNull(schema);
    }

    @Test(groups = {"integration"})
    public void testGetTableSchema() throws Exception {

        final String table = "table_schema_test";
        client.execute("DROP TABLE IF EXISTS " + table).get(10, TimeUnit.SECONDS);
        client.execute("CREATE TABLE " + table +
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
    public void testGetTableSchemaFromQuery() {
        TableSchema schema = client.getTableSchemaFromQuery("SELECT toUInt32(1) as col1, 'value' as col2");
        Assert.assertNotNull(schema);
        Assert.assertEquals(schema.getColumns().size(), 2);
        Assert.assertEquals(schema.getColumns().get(0).getColumnName(), "col1");
        Assert.assertEquals(schema.getColumns().get(0).getDataType(), ClickHouseDataType.UInt32);
        Assert.assertEquals(schema.getColumns().get(1).getColumnName(), "col2");
        Assert.assertEquals(schema.getColumns().get(1).getDataType(), ClickHouseDataType.String);
    }

    @Test(groups = {"integration"})
    public void testGetTableSchemaFromQueryWithParams() {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("param1", 2);
        TableSchema schema = client.getTableSchemaFromQuery("SELECT toUInt32(1) as col1, {param1:String} as col2",
                queryParams);
        Assert.assertNotNull(schema);
        Assert.assertEquals(schema.getColumns().size(), 2);
        Assert.assertEquals(schema.getColumns().get(0).getColumnName(), "col1");
        Assert.assertEquals(schema.getColumns().get(0).getDataType(), ClickHouseDataType.UInt32);
        Assert.assertEquals(schema.getColumns().get(1).getColumnName(), "col2");
        Assert.assertEquals(schema.getColumns().get(1).getDataType(), ClickHouseDataType.String);
    }

    @Test(groups = {"integration"})
    public void testServerTimeZoneFromHeader() {

        final String requestTimeZone = "America/Los_Angeles";
        try (QueryResponse response =
                     client.query("SELECT now() as t, toDateTime(now(), 'UTC') as utc_time " +
                             "SETTINGS session_timezone = '" + requestTimeZone + "'").get(1, TimeUnit.SECONDS)) {

            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);

            reader.next();

            LocalDateTime serverTime = reader.getLocalDateTime(1);
            LocalDateTime serverUtcTime = reader.getLocalDateTime(2);

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

                ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);

                reader.next();

                LocalDateTime serverTime = reader.getLocalDateTime(1); // in "America/Los_Angeles"
                LocalDateTime serverUtcTime = reader.getLocalDateTime(2);
                ZonedDateTime serverLisbonTime = reader.getZonedDateTime(3);  // in "Europe/Lisbon"

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

    @Test
    public void testAsyncQuery() {
        try (Client client = newClient().useAsyncRequests(true).build()){
             simpleRequest(client);
        } catch (Exception e) {
            Assert.fail("Failed to get server time zone from header", e);
        }
    }

    protected void simpleRequest(Client client) throws Exception {
        try (QueryResponse response =
                     client.query("SELECT number FROM system.numbers LIMIT 1000_000").get(1, TimeUnit.SECONDS)) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);

            int count = 0;
            while (reader.hasNext()) {
                reader.next();
                count++;
            }

            Assert.assertEquals(count, 1000_000);
        }
    }

    @Test
    public void testConcurrentQueries() throws Exception{
        final Client client = newClient().build();
        final int concurrency = 10;
        CountDownLatch latch = new CountDownLatch(concurrency);
        Runnable task = () -> {
            try {
                simpleRequest(client);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail("Failed", e);
            } finally {
                latch.countDown();
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        IntStream.range(0,concurrency).forEach(i -> executor.submit(task));
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        latch.await();
        Assert.assertEquals(latch.getCount(), 0);
    }

    @Test(groups = {"integration"})
    public void testQueryReadToPOJO() {
        int limit = 10;
        final String sql = "SELECT toInt32(rand32()) as id, toInt32(number * 10) as age, concat('name_', toString(number + 1)) as name " +
                " FROM system.numbers LIMIT " + limit;
        TableSchema schema = client.getTableSchemaFromQuery(sql);
        client.register(SimplePOJO.class, schema);

        List<SimplePOJO> pojos = client.queryAll(sql, SimplePOJO.class, schema);
        Assert.assertEquals(pojos.size(), limit);
    }

    @Test(groups = {"integration"})
    public void testQueryReadToPOJOWithoutGetters() {
        int limit = 10;
        final String sql = "SELECT toInt32(1) as p1, toInt32(1) as p2 ";
        TableSchema schema = client.getTableSchemaFromQuery(sql);
        client.register(NoGettersPOJO.class, schema);

        try {
            client.queryAll(sql, SimplePOJO.class, schema);
            Assert.fail("No exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("No deserializers found for the query and class"));
        }
    }

    @Test(groups = {"integration"})
    public void testQueryAllWithPOJO() throws Exception {

        final String tableName = "test_query_all_with_pojo";
        final String createTableSQL = QuerySamplePOJO.generateTableCreateSQL(tableName);
        client.execute("DROP TABLE IF EXISTS test_query_all_with_pojo").get();
        client.execute(createTableSQL).get();

        QuerySamplePOJO pojo = new QuerySamplePOJO();
        TableSchema schema = client.getTableSchema(tableName);
        client.register(QuerySamplePOJO.class, schema);

        client.insert(tableName, Collections.singletonList(pojo)).get();

        // correct decimal according to the table schema
        pojo.setDecimal32(cropDecimal(pojo.getDecimal32(), 2));
        pojo.setDecimal64(cropDecimal(pojo.getDecimal64(), 3));
        pojo.setDecimal128(cropDecimal(pojo.getDecimal128(),4));
        pojo.setDecimal256(cropDecimal(pojo.getDecimal256(),5));

        // adjust datetime
        pojo.setDateTime(pojo.getDateTime().minusNanos(pojo.getDateTime().getNano()));
        pojo.setDateTime64(pojo.getDateTime64().withNano((int) Math.ceil((pojo.getDateTime64().getNano() / 1000_000) * 1000_000)));

        List<QuerySamplePOJO> pojos = client.queryAll("SELECT * FROM " + tableName + " LIMIT 1", QuerySamplePOJO.class,
                schema);
        Assert.assertEquals(pojos.get(0), pojo, "Expected " + pojo + " but got " + pojos.get(0));
    }

    public static BigDecimal cropDecimal(BigDecimal value, int scale) {
        BigInteger bi = value.unscaledValue().divide(BigInteger.TEN.pow(value.scale() - scale));
        return new BigDecimal(bi, scale);
    }

    @DataProvider(name = "sessionRoles")
    private static Object[][] sessionRoles() {
        return new Object[][]{
                {new String[]{"ROL1", "ROL2"}},
                {new String[]{"ROL1", "ROL2"}},
                {new String[]{"ROL1", "ROL2"}},
                {new String[]{"ROL1", "ROL2,☺"}},
                {new String[]{"ROL1", "ROL2"}},
        };
    }

    @Test(groups = {"integration"}, dataProvider = "sessionRoles", dataProviderClass = QueryTests.class)
    public void testOperationCustomRoles(String[] roles) throws Exception {
        if (isVersionMatch("(,24.3]")) {
            return;
        }

        String password = "^1A" + RandomStringUtils.random(12, true, true) + "3B$";
        final String rolesList = "\"" + Strings.join("\",\"", roles) + "\"";
        try (CommandResponse resp = client.execute("DROP ROLE IF EXISTS " + rolesList).get()) {
        }
        try (CommandResponse resp = client.execute("CREATE ROLE " + rolesList).get()) {
        }
        try (CommandResponse resp = client.execute("DROP USER IF EXISTS some_user").get()) {
        }
        try (CommandResponse resp = client.execute("CREATE USER some_user IDENTIFIED BY '" + password + "'" ).get()) {
        }
        try (CommandResponse resp = client.execute("GRANT " + rolesList + " TO some_user").get()) {
        }


        try (Client userClient = newClient().setUsername("some_user").setPassword(password).build()) {
            QuerySettings settings = new QuerySettings().setDBRoles(Arrays.asList(roles));
            List<GenericRecord> resp = userClient.queryAll("SELECT currentRoles()", settings);
            Set<String> roleSet = new HashSet<>(Arrays.asList(roles));
            Set<String> currentRoles = new  HashSet<String> (resp.get(0).getList(1));
            Assert.assertEquals(currentRoles, roleSet, "Roles " + roleSet + " not found in " + currentRoles);
        }
    }

    @DataProvider(name = "clientSessionRoles")
    private static Object[][] clientSessionRoles() {
        return new Object[][]{
                {new String[]{"ROL1", "ROL2"}},
                {new String[]{"ROL1", "ROL2,☺"}},
        };
    }
    @Test(groups = {"integration"}, dataProvider = "clientSessionRoles", dataProviderClass = QueryTests.class)
    public void testClientCustomRoles(String[] roles) throws Exception {
        if (isVersionMatch("(,24.3]")) {
            return;
        }

        String password = "^1A" + RandomStringUtils.random(12, true, true) + "3B$";
        final String rolesList = "\"" + Strings.join("\",\"", roles) + "\"";
        try (CommandResponse resp = client.execute("DROP ROLE IF EXISTS " + rolesList).get()) {
        }
        try (CommandResponse resp = client.execute("CREATE ROLE " + rolesList).get()) {
        }
        try (CommandResponse resp = client.execute("DROP USER IF EXISTS some_user").get()) {
        }
        try (CommandResponse resp = client.execute("CREATE USER some_user IDENTIFIED WITH sha256_password BY '" + password + "'" ).get()) {
        }
        try (CommandResponse resp = client.execute("GRANT " + rolesList + " TO some_user").get()) {
        }

        try (Client userClient = newClient().setUsername("some_user").setPassword(password).build()) {
            userClient.setDBRoles(Arrays.asList(roles));
            List<GenericRecord> resp = userClient.queryAll("SELECT currentRoles()");
            Set<String> roleSet = new HashSet<>(Arrays.asList(roles));
            Set<String> currentRoles = new  HashSet<String> (resp.get(0).getList(1));
            Assert.assertEquals(currentRoles, roleSet, "Roles " + roleSet + " not found in " + currentRoles);
        }
    }


    @Test(groups = {"integration"})
    public void testLogComment() throws Exception {

        String logComment = "Test log comment";
        QuerySettings settings = new QuerySettings()
                .setQueryId(UUID.randomUUID().toString())
                .logComment(logComment);
        try (QueryResponse response = client.query("SELECT 1", settings).get()) {
            Assert.assertNotNull(response.getQueryId());
            Assert.assertTrue(response.getQueryId().startsWith(settings.getQueryId()));
        }

        try (CommandResponse resp = client.execute("SYSTEM FLUSH LOGS").get()) {
        }

        List<GenericRecord> logRecords = client.queryAll("SELECT query_id, log_comment FROM system.query_log WHERE query_id = '" + settings.getQueryId() + "'");
        Assert.assertEquals(logRecords.get(0).getString("query_id"), settings.getQueryId());
        Assert.assertEquals(logRecords.get(0).getString("log_comment"), logComment);
    }
    @Test(groups = { "integration" }, enabled = true)
    public void testReadingBitmap() throws Exception {
        final String tableName = "bitmaps_test_table";
        final String createSQL = AggregateFuncDTO.generateTableCreateSQL(tableName);
        final AggregateFuncDTO pojo = new AggregateFuncDTO();

        try {
            client.execute("DROP TABLE IF EXISTS " + tableName).get();
            client.execute(createSQL).get();
        } catch (Exception e) {
            throw e;
        }

        client.register(AggregateFuncDTO.class, client.getTableSchema(tableName));

        try (InsertResponse response = client.insert(tableName, Collections.singletonList(pojo)).get(30, TimeUnit.SECONDS)) {
            Assert.assertEquals(response.getWrittenRows(), 1);
        }

        try (QueryResponse queryResponse =
                     client.query("SELECT * FROM " + tableName + " LIMIT 1").get(30, TimeUnit.SECONDS)) {

            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(queryResponse);
            Assert.assertNotNull(reader.next());
            Assert.assertFalse(reader.hasNext());

            Assert.assertEquals(reader.getClickHouseBitmap("groupBitmapUint32"), pojo.getGroupBitmapUint32());
            Assert.assertEquals(reader.getClickHouseBitmap("groupBitmapUint64"), pojo.getGroupBitmapUint64());
        }
    }

    @Test(groups = {"integration"})
    public void testReadingJSONValues() throws Exception {
        if (isCloud()) {
            return; // TODO: add support on cloud
        }
        if (isVersionMatch("(,24.8]")) {
            return;
        }
        CommandSettings commandSettings = new CommandSettings();
        commandSettings.serverSetting("allow_experimental_json_type", "1");
        client.execute("DROP TABLE IF EXISTS test_json_values", commandSettings).get(1, TimeUnit.SECONDS);
        client.execute("CREATE TABLE test_json_values (json JSON) ENGINE = MergeTree ORDER BY ()", commandSettings).get(1, TimeUnit.SECONDS);
        client.execute("INSERT INTO test_json_values VALUES ('{\"a\" : {\"b\" : 42}, \"c\" : [1, 2, 3]}')", commandSettings).get(1, TimeUnit.SECONDS);


        QuerySettings settings = new QuerySettings().setFormat(ClickHouseFormat.CSV);
        try (QueryResponse resp = client.query("SELECT json FROM test_json_values", settings).get(1, TimeUnit.SECONDS)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(resp.getInputStream()));
            Assert.assertEquals(StringEscapeUtils.unescapeCsv(reader.lines().findFirst().get()), "{\"a\":{\"b\":\"42\"},\"c\":[\"1\",\"2\",\"3\"]}");
        }

        settings = new QuerySettings()
                .serverSetting(ServerSettings.OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING, "1");
        try (QueryResponse resp = client.query("SELECT json FROM test_json_values", settings).get(1, TimeUnit.SECONDS)) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(resp);
            Assert.assertNotNull(reader.next());
            Assert.assertEquals(reader.getString(1), "{\"a\":{\"b\":\"42\"},\"c\":[\"1\",\"2\",\"3\"]}");
        }
    }

    @Test
    public void testGetColumnsByIndex() throws Exception {

        try (QueryResponse response = client.query("SELECT toInt8(1) as number, 'test' as string").get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            reader.next();
            Assert.assertEquals(reader.getInteger(1), 1);
            Assert.assertEquals(reader.getString(2), "test");
        }
    }

    protected Client.Builder newClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .compressClientRequest(false)
                .compressServerResponse(useServerCompression)
                .useHttpCompression(useHttpCompression)
                .allowBinaryReaderToReuseBuffers(usePreallocatedBuffers)
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .serverSetting(ServerSettings.WAIT_ASYNC_INSERT, "1")
                .serverSetting(ServerSettings.ASYNC_INSERT, "0")
                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "true").equals("true"));
    }

    @Test(groups = {"integration"})
    public void testReadingSimpleAggregateFunction() throws Exception {
        final String tableName = "simple_aggregate_function_test_table";
        client.execute("DROP TABLE IF EXISTS " + tableName).get();
        client.execute("CREATE TABLE `" + tableName + "` " +
                "(idx UInt8, lowest_value SimpleAggregateFunction(min, UInt8), count SimpleAggregateFunction(sum, Int64), mp SimpleAggregateFunction(maxMap, Map(UInt8, UInt8))) " +
                "ENGINE MergeTree ORDER BY ();").get();


            try (InsertResponse response = client.insert(tableName, new ByteArrayInputStream("1\t2\t3\t{1:2}".getBytes(StandardCharsets.UTF_8)), ClickHouseFormat.TSV).get(30, TimeUnit.SECONDS)) {
            Assert.assertEquals(response.getWrittenRows(), 1);
        }

        try (QueryResponse queryResponse = client.query("SELECT * FROM " + tableName + " LIMIT 1").get(30, TimeUnit.SECONDS)) {

            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(queryResponse);
            Assert.assertNotNull(reader.next());
            Assert.assertEquals(reader.getByte("idx"), Byte.valueOf("1"));
            Assert.assertEquals((Short) reader.readValue("lowest_value"), Short.parseShort("2"));
            Assert.assertEquals((Long) reader.readValue("count"), Long.parseLong("3"));
            Assert.assertEquals(String.valueOf((LinkedHashMap) reader.readValue("mp")), "{1=2}");
            Assert.assertFalse(reader.hasNext());
        }
    }

    @Test(groups = {"integration"})
    public void testReadingSimpleAggregateFunction2() throws Exception {
        final String tableName = "simple_aggregate_function_test_table";
        client.execute("DROP TABLE IF EXISTS " + tableName).get();
        client.execute("CREATE TABLE `" + tableName + "` " +
                "(idx UInt8, lowest_value SimpleAggregateFunction(min, UInt8), count SimpleAggregateFunction(sum, Int64), date SimpleAggregateFunction(anyLast, DateTime32)) " +
                "ENGINE Memory;").get();


        try (InsertResponse response = client.insert(tableName, new ByteArrayInputStream("1\t2\t3\t2024-12-22T12:00:00".getBytes(StandardCharsets.UTF_8)), ClickHouseFormat.TSV).get(30, TimeUnit.SECONDS)) {
            Assert.assertEquals(response.getWrittenRows(), 1);
        }

        try (QueryResponse queryResponse = client.query("SELECT * FROM " + tableName + " LIMIT 1").get(30, TimeUnit.SECONDS)) {

            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(queryResponse);
            Assert.assertNotNull(reader.next());
            Assert.assertEquals(reader.getByte("idx"), Byte.valueOf("1"));
            Assert.assertEquals((Short) reader.getShort("lowest_value"), Short.parseShort("2"));
            Assert.assertEquals((Long) reader.getLong("count"), Long.parseLong("3"));
            Assert.assertEquals(reader.getLocalDateTime("date"), LocalDateTime.of(2024,12,22,12,00,00));
            Assert.assertFalse(reader.hasNext());
        }
    }

    @Test(groups = {"integration"})
    public void testReadingEnumsAsStrings() throws Exception {
        final String tableName = "enums_as_strings_test_table";
        client.execute("DROP TABLE IF EXISTS " + tableName).get();
        client.execute("CREATE TABLE `" + tableName + "` " +
                "(idx UInt8, enum1 Enum8('a' = 1, 'b' = 2, 'c' = 3), enum2 Enum16('atch' = 1, 'batch' = 2, 'catch' = 3)) " +
                "ENGINE MergeTree ORDER BY ()").get();

        try (InsertResponse response = client.insert(tableName, new ByteArrayInputStream("1\ta\t2".getBytes(StandardCharsets.UTF_8)), ClickHouseFormat.TSV).get(30, TimeUnit.SECONDS)) {
            Assert.assertEquals(response.getWrittenRows(), 1);
        }

        try (QueryResponse queryResponse = client.query("SELECT * FROM " + tableName + " LIMIT 1").get(30, TimeUnit.SECONDS)) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(queryResponse);
            Assert.assertNotNull(reader.next());
            Assert.assertEquals(reader.getByte("idx"), Byte.valueOf("1"));
            Assert.assertEquals(reader.getString("enum1"), "a");
            Assert.assertEquals(reader.getShort("enum1"), 1);
            Assert.assertEquals(reader.getInteger("enum1"), 1);
            Assert.assertEquals(reader.getString("enum2"), "batch");
            Assert.assertEquals(reader.getShort("enum2"), 2);
            Assert.assertEquals(reader.getInteger("enum2"), 2);
            Assert.assertFalse(reader.hasNext());
        }
    }

    @Test(groups = {"integration"})
    public void testServerTimezone() throws Exception {
        final String sql = "SELECT now() as t, toDateTime(now(), 'UTC') as utc_time, toDateTime(now(), 'America/New_York') as est_time";
        try (QueryResponse response = client.query(sql).get(1, TimeUnit.SECONDS)) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            Assert.assertNotNull(reader.next());
            ZonedDateTime serverTime = reader.getZonedDateTime(1);
            ZonedDateTime serverUtcTime = reader.getZonedDateTime(2);
            ZonedDateTime serverEstTime = reader.getZonedDateTime(3);
            Assert.assertEquals(serverTime.withZoneSameInstant(ZoneId.of("UTC")), serverUtcTime);
            Assert.assertEquals(serverTime, serverUtcTime);
            Assert.assertEquals(serverUtcTime.withZoneSameInstant(ZoneId.of("America/New_York")), serverEstTime);
        }
    }

    @Test(groups = {"integration"})
    public void testLowCardinalityValues() throws Exception {
        final String table = "test_low_cardinality_values";
        final String tableCreate = "CREATE TABLE " + table + "(rowID Int32, keyword LowCardinality(String)) Engine = MergeTree ORDER BY ()";

        client.execute("DROP TABLE IF EXISTS " + table);
        client.execute(tableCreate);

        client.execute("INSERT INTO " + table + " VALUES (0, 'db'), (1, 'fast'), (2, 'not a java')");
        String[] values = new String[] {"db", "fast", "not a java"};
        Collection<GenericRecord> records = client.queryAll("SELECT * FROM " + table);
        for (GenericRecord record : records) {
            int rowId = record.getInteger("rowID");
            Assert.assertEquals(record.getString("keyword"), values[rowId]);
        }
    }

    @Test(groups = {"integration"})
    public void testGettingRowsBeforeLimit() throws Exception {
        int expectedTotalRowsToRead = 100;
        if (isVersionMatch("(,23.8]")) {
            // issue in prev. release.
            expectedTotalRowsToRead = 0;
        }

        try (QueryResponse response = client.query("SELECT number FROM system.numbers LIMIT 100").get()) {
            Assert.assertTrue(response.getResultRows() < 1000);

            Assert.assertEquals(response.getTotalRowsToRead(), expectedTotalRowsToRead);
        }
    }

    @Test(groups = {"integration"})
    public void testGetDynamicValue() throws Exception  {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        String table = "test_get_dynamic_values";

        final AtomicInteger rowId = new AtomicInteger(-1);
        final Random rnd = new Random();

        List<Map<String,Object>> dataset = prepareDataSet(table, Arrays.asList("rowId Int32", "v Dynamic"),
                Arrays.asList(s -> rowId.incrementAndGet(), s-> {
                    int decision = rnd.nextInt(3);
                    if (decision == 0) {
                        return RandomStringUtils.randomAlphanumeric(3, 10);
                    } else if (decision == 1) {
                        return rnd.nextInt();
                    } else {
                        return rnd.nextDouble();
                    }
                }), 1000);

        try (QueryResponse response = client.query("SELECT * FROM " + table).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            while (reader.next() != null) {
                int rowIndex  = reader.getInteger("rowId");
                Assert.assertEquals(reader.getString("v"), dataset.get(rowIndex).get("v").toString());
            }
        }
    }

    @Test(groups = {"integration"})
    public void testBinaryEncodedJSON() throws Exception  {
        if (isVersionMatch("(,24.8]")) {
            return;
        }

        String table = "test_get_json_values";

        final AtomicInteger rowId = new AtomicInteger(-1);
        final Random rnd = new Random();

        List<Map<String,Object>> dataset = prepareDataSet(table, Arrays.asList("rowId Int32", "v1 JSON"),
                Arrays.asList(s -> rowId.incrementAndGet(),
                s-> {
                    String a = "{'a': '" + RandomStringUtils.randomAlphabetic(20) + "', 'b': { 'c': 'test1', 'd': " + rnd
                            .nextInt(1000) + "}}";
                    return a.replaceAll("'", "\"");
                }), 1);

        ObjectMapper jackson = new ObjectMapper();
        try (QueryResponse response = client.query("SELECT * FROM " + table).get()) {
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);
            while (reader.next() != null) {
                int rowIndex  = reader.getInteger("rowId");
                JsonNode expected = jackson.readValue(dataset.get(rowIndex).get("v1").toString(), JsonNode.class);
                Map<String, Object> v1 = reader.readValue("v1");
                for (Map.Entry<String, Object> e : v1.entrySet()) {
                    String pointer = "/" + e.getKey().replaceAll("\\.", "/");
                    Assert.assertEquals(e.getValue().toString(), expected.at(pointer).asText());
                }
            }
        }
    }

    public boolean isVersionMatch(String versionExpression) {
        List<GenericRecord> serverVersion = client.queryAll("SELECT version()");
        return ClickHouseVersion.of(serverVersion.get(0).getString(1)).check(versionExpression);
    }

    @Test(groups = {"integration"})
    public void testEmptyResponse() throws Exception {
        try (QueryResponse response = client.query("SELECT number FROM system.numbers LIMIT 0", new QuerySettings().setFormat(ClickHouseFormat.RowBinary)).get()) {
            System.out.println(response.getResultRows());
        }
    }
}
