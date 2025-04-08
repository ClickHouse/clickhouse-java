package com.clickhouse.client.insert;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.DataTypeUtils;
import com.clickhouse.client.api.command.CommandResponse;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryFormatSerializer;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseVersion;
import com.clickhouse.data.format.BinaryStreamUtils;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.SnappyCompressorOutputStream;
import org.apache.commons.lang3.StringEscapeUtils;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testcontainers.shaded.org.checkerframework.checker.units.qual.A;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class InsertTests extends BaseIntegrationTest {
    private Client client;
    private InsertSettings settings;

    private boolean useClientCompression = false;

    private boolean useHttpCompression = false;

    private static final int EXECUTE_CMD_TIMEOUT = 10; // seconds

    InsertTests() {
    }

    public InsertTests(boolean useClientCompression, boolean useHttpCompression) {
        this.useClientCompression = useClientCompression;
        this.useHttpCompression = useHttpCompression;
    }

    @BeforeMethod(groups = { "integration" })
    public void setUp() throws IOException {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        int bufferSize = (7 * 65500);
        client = newClient()
                .setSocketSndbuf(bufferSize)
                .setSocketRcvbuf(bufferSize)
                .setClientNetworkBufferSize(bufferSize)
                .build();

        settings = new InsertSettings()
                .setDeduplicationToken(RandomStringUtils.randomAlphabetic(36))
                .setQueryId(String.valueOf(UUID.randomUUID()));
    }

    protected Client.Builder newClient() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        return new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .compressClientRequest(useClientCompression)
                .useHttpCompression(useHttpCompression)
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .serverSetting(ServerSettings.ASYNC_INSERT, "0")
                .serverSetting(ServerSettings.WAIT_END_OF_QUERY, "1");
    }

    @AfterMethod(groups = { "integration" })
    public void tearDown() {
        client.close();
    }

    @Test(groups = { "integration" }, enabled = true)
    public void insertSimplePOJOs() throws Exception {
        String tableName = "simple_pojo_table";
        String createSQL = SamplePOJO.generateTableCreateSQL(tableName);
        String uuid = UUID.randomUUID().toString();

        initTable(tableName, createSQL);

        client.register(SamplePOJO.class, client.getTableSchema(tableName));
        List<Object> simplePOJOs = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            simplePOJOs.add(new SamplePOJO());
        }
        settings.setQueryId(uuid);
        InsertResponse response = client.insert(tableName, simplePOJOs, settings).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);

        OperationMetrics metrics = response.getMetrics();
        assertEquals(simplePOJOs.size(), metrics.getMetric(ServerMetrics.NUM_ROWS_WRITTEN).getLong());
        assertEquals(simplePOJOs.size(), response.getWrittenRows());
        assertTrue(metrics.getMetric(ClientMetrics.OP_DURATION).getLong() > 0);
        assertTrue(metrics.getMetric(ClientMetrics.OP_SERIALIZATION).getLong() > 0);
        assertEquals(metrics.getQueryId(), uuid);
        assertEquals(response.getQueryId(), uuid);
    }

    @Test(groups = { "integration" }, enabled = true)
    public void insertPOJOWithJSON() throws Exception {
        if (isCloud()) {
            return; // not working because of Code: 452. DB::Exception: Setting allow_experimental_json_type should not be changed. (SETTING_CONSTRAINT_VIOLATION)
            // but without this setting it doesn't let to create a table
        }
        List<GenericRecord> serverVersion = client.queryAll("SELECT version()");
        if (ClickHouseVersion.of(serverVersion.get(0).getString(1)).check("(,24.8]")) {
            System.out.println("Test is skipped: feature is supported since 24.8");
            return;
        }

        final String tableName = "pojo_with_json_table";
        final String createSQL = PojoWithJSON.createTable(tableName);
        final String originalJsonStr = "{\"a\":{\"b\":\"42\"},\"c\":[\"1\",\"2\",\"3\"]}";


        CommandSettings commandSettings = new CommandSettings();
        commandSettings.serverSetting("allow_experimental_json_type", "1");
        client.execute("DROP TABLE IF EXISTS " + tableName, commandSettings).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);
        client.execute(createSQL, commandSettings).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);

        client.register(PojoWithJSON.class, client.getTableSchema(tableName));
        PojoWithJSON pojo = new PojoWithJSON();
        pojo.setEventPayload(originalJsonStr);
        List<Object> data = Arrays.asList(pojo);

        InsertSettings insertSettings = new InsertSettings()
                .serverSetting(ServerSettings.INPUT_FORMAT_BINARY_READ_JSON_AS_STRING, "1");
        InsertResponse response = client.insert(tableName, data, insertSettings).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);
        assertEquals(response.getWrittenRows(), 1);

        QuerySettings settings = new QuerySettings()
                .setFormat(ClickHouseFormat.CSV);
        try (QueryResponse resp = client.query("SELECT * FROM " + tableName, settings).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(resp.getInputStream()));
            String jsonStr = StringEscapeUtils.unescapeCsv(reader.lines().findFirst().get());
            Assert.assertEquals(jsonStr, originalJsonStr);
        }
    }

    @Test(groups = { "integration" }, enabled = true)
    public void insertPOJOAndReadBack() throws Exception {
        final String tableName = "single_pojo_table";
        final String createSQL = SamplePOJO.generateTableCreateSQL(tableName);
        final SamplePOJO pojo = new SamplePOJO();

        initTable(tableName, createSQL);

        client.register(SamplePOJO.class, client.getTableSchema(tableName));

        System.out.println("Inserting POJO: " + pojo);
        try (InsertResponse response = client.insert(tableName, Collections.singletonList(pojo), settings).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS)) {
            Assert.assertEquals(response.getWrittenRows(), 1);
        }

        try (QueryResponse queryResponse =
                client.query("SELECT * FROM " + tableName + " LIMIT 1").get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS)) {

            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(queryResponse);
            Assert.assertNotNull(reader.next());

            Assert.assertEquals(reader.getByte("byteValue"), pojo.getByteValue());
            Assert.assertEquals(reader.getByte("int8"), pojo.getInt8());
            Assert.assertEquals(reader.getShort("uint8"), pojo.getUint8());
            Assert.assertEquals(reader.getShort("int16"), pojo.getInt16());
            Assert.assertEquals(reader.getInteger("int32"), pojo.getInt32());
            Assert.assertEquals(reader.getLong("int64"), pojo.getInt64());
            Assert.assertEquals(reader.getFloat("float32"), pojo.getFloat32());
            Assert.assertEquals(reader.getDouble("float64"), pojo.getFloat64());
            Assert.assertEquals(reader.getString("string"), pojo.getString());
            Assert.assertEquals(reader.getString("fixedString"), pojo.getFixedString());
            Assert.assertTrue(reader.getZonedDateTime("zonedDateTime").isEqual(pojo.getZonedDateTime().withNano(0)));
            Assert.assertTrue(reader.getZonedDateTime("zonedDateTime64").isEqual(pojo.getZonedDateTime64()));
            Assert.assertTrue(reader.getOffsetDateTime("offsetDateTime").isEqual(pojo.getOffsetDateTime().withNano(0)));
            Assert.assertTrue(reader.getOffsetDateTime("offsetDateTime64").isEqual(pojo.getOffsetDateTime64()));
            Assert.assertEquals(reader.getInstant("instant"), pojo.getInstant().with(ChronoField.MICRO_OF_SECOND, 0));
            Assert.assertEquals(reader.getInstant("instant64"), pojo.getInstant64());
        }
    }

    @Test
    public void testInsertingPOJOWithNullValueForNonNullableColumn() throws Exception {
        final String tableName = "single_pojo_table";
        final String createSQL = SamplePOJO.generateTableCreateSQL(tableName);
        final SamplePOJO pojo = new SamplePOJO();

        pojo.setBoxedByte(null);

        initTable(tableName, createSQL);

        client.register(SamplePOJO.class, client.getTableSchema(tableName));

        try (InsertResponse response = client.insert(tableName, Collections.singletonList(pojo), settings).get(30, TimeUnit.SECONDS)) {
            fail("Should have thrown an exception");
        } catch (ClientException e) {
            e.printStackTrace();
            assertTrue(e.getCause() instanceof  IllegalArgumentException);
        }
    }

    @Test(groups = { "integration" }, enabled = true)
    public void insertRawData() throws Exception {
        final String tableName = "raw_data_table";
        final String createSQL = "CREATE TABLE " + tableName +
                " (Id UInt32, event_ts Timestamp, name String, p1 Int64, p2 String) ENGINE = MergeTree() ORDER BY ()";

        initTable(tableName, createSQL);

        settings.setInputStreamCopyBufferSize(8198 * 2);
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(data);
        for (int i = 0; i < 1000; i++) {
            writer.printf("%d\t%s\t%s\t%d\t%s\n", i, "2021-01-01 00:00:00", "name" + i, i, "p2");
        }
        writer.flush();
        InsertResponse response = client.insert(tableName, new ByteArrayInputStream(data.toByteArray()),
                ClickHouseFormat.TSV, settings).get(30, TimeUnit.SECONDS);
        OperationMetrics metrics = response.getMetrics();
        assertEquals((int)response.getWrittenRows(), 1000 );

        List<GenericRecord> records = client.queryAll("SELECT * FROM " + tableName);
        assertEquals(records.size(), 1000);
    }

    @Test(groups = { "integration" }, dataProvider = "insertRawDataSimpleDataProvider", dataProviderClass = InsertTests.class)
    public void insertRawDataSimple(String tableName) throws Exception {
//        final String tableName = "raw_data_table";
        final String createSql = String.format("CREATE TABLE IF NOT EXISTS %s " +
                " (Id UInt32, event_ts Timestamp, name String, p1 Int64, p2 String) ENGINE = MergeTree() ORDER BY ()", tableName);

        initTable(tableName, createSql);

        InsertSettings settings = new InsertSettings()
                .setDeduplicationToken(RandomStringUtils.randomAlphabetic(36))
                .setQueryId(String.valueOf(UUID.randomUUID()))
                .setInputStreamCopyBufferSize(8198 * 2);
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(data);
        int numberOfRecords = 1000;
        for (int i = 0; i < numberOfRecords; i++) {
            writer.printf("%d\t%s\t%s\t%d\t%s\n", i, "2021-01-01 00:00:00", "name" + i, i, "p2");
        }
        writer.flush();
        InsertResponse response = client.insert(tableName, new ByteArrayInputStream(data.toByteArray()),
                ClickHouseFormat.TSV, settings).get(30, TimeUnit.SECONDS);
        OperationMetrics metrics = response.getMetrics();
        assertEquals((int)response.getWrittenRows(), numberOfRecords );
    }

    @Test(groups = { "integration" })
    public void insertRawDataFewerColumns() throws Exception {
        final String tableName = "raw_data_select_columns_table";
        final String createSQL = "CREATE TABLE " + tableName +
                " (Id UInt32, event_ts Timestamp, name String, p1 Int64, p2 String, p3 String, p4 Int8) ENGINE = MergeTree() ORDER BY ()";
        List<String> columnNames = Arrays.asList("Id", "event_ts", "name", "p1", "p2");

        initTable(tableName, createSQL);

        settings.setInputStreamCopyBufferSize(8198 * 2);
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(data);
        for (int i = 0; i < 1000; i++) {
            writer.printf("%d\t%s\t%s\t%d\t%s\n", i, "2021-01-01 00:00:00", "name" + i, i, "p2");
        }
        writer.flush();
        InsertResponse response = client.insert(tableName, columnNames, new ByteArrayInputStream(data.toByteArray()),
                ClickHouseFormat.TSV, settings).get(30, TimeUnit.SECONDS);
        OperationMetrics metrics = response.getMetrics();
        assertEquals((int)response.getWrittenRows(), 1000 );

        List<GenericRecord> records = client.queryAll("SELECT * FROM " + tableName);
        assertEquals(records.size(), 1000);
    }

    @DataProvider(name = "insertRawDataSimpleDataProvider")
    public static Object[][] insertRawDataSimpleDataProvider() {
        return new Object[][] {
            {"raw_data_table"},
            {"`raw_data_table`"},
            {"`" + ClickHouseServerForTest.getDatabase() + ".raw_data_table`"},
        };
    }

    @Test(groups = { "integration" })
    public void testInsertMetricsOperationId() throws Exception {
        final String tableName = "insert_metrics_test";
        final String createSQL = "CREATE TABLE " + tableName +
                                 " (Id UInt32, event_ts Timestamp, name String, p1 Int64, p2 String) ENGINE = MergeTree() ORDER BY ()";

        initTable(tableName, createSQL);

        ByteArrayOutputStream data = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(data);
        int numberOfRecords = 3;
        for (int i = 0; i < numberOfRecords; i++) {
            writer.printf("%d\t%s\t%s\t%d\t%s\n", i, "2021-01-01 00:00:00", "name" + i, i, "p2");
        }
        writer.flush();

        InsertSettings settings = new InsertSettings()
            .setQueryId(String.valueOf(UUID.randomUUID()))
            .setOperationId(UUID.randomUUID().toString());
        InsertResponse response = client.insert(tableName, new ByteArrayInputStream(data.toByteArray()),
            ClickHouseFormat.TSV, settings).get(30, TimeUnit.SECONDS);
        OperationMetrics metrics = response.getMetrics();
        assertEquals((int)response.getWrittenRows(), numberOfRecords );
        assertEquals(metrics.getQueryId(), settings.getQueryId());
        assertTrue(metrics.getMetric(ClientMetrics.OP_DURATION).getLong() > 0);
    }

    @Test(groups = { "integration" })
    public void testInsertSettingsAddDatabase() throws Exception {
        final String tableName = "insert_settings_database_test";
        final String new_database = "new_database";
        final String createDatabaseSQL = "CREATE DATABASE " + new_database;
        final String createTableSQL = "CREATE TABLE " + new_database + "." + tableName +
                                 " (Id UInt32, event_ts Timestamp, name String, p1 Int64, p2 String) ENGINE = MergeTree() ORDER BY ()";
        final String dropDatabaseSQL = "DROP DATABASE IF EXISTS " + new_database;

        client.execute(dropDatabaseSQL).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);
        client.execute(createDatabaseSQL).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);
        client.execute(createTableSQL).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);

        InsertSettings insertSettings = settings.setInputStreamCopyBufferSize(8198 * 2)
            .setDeduplicationToken(RandomStringUtils.randomAlphabetic(36))
            .setQueryId(String.valueOf(UUID.randomUUID()))
            .setDatabase(new_database);

        ByteArrayOutputStream data = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(data);
        for (int i = 0; i < 1000; i++) {
            writer.printf("%d\t%s\t%s\t%d\t%s\n", i, "2021-01-01 00:00:00", "name" + i, i, "p2");
        }
        writer.flush();
        InsertResponse response = client.insert(tableName, new ByteArrayInputStream(data.toByteArray()),
            ClickHouseFormat.TSV, insertSettings).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);
        assertEquals((int)response.getWrittenRows(), 1000 );

        List<GenericRecord> records = client.queryAll("SELECT * FROM " + new_database + "." + tableName);
        assertEquals(records.size(), 1000);
    }

    @Test(groups = {"integration"}, dataProviderClass = InsertTests.class, dataProvider = "logCommentDataProvider")
    public void testLogComment(String logComment) throws Exception {

        InsertSettings settings = new InsertSettings()
                .setQueryId(UUID.randomUUID().toString())
                .logComment(logComment);

        final String tableName = "single_pojo_table";
        final String createSQL = SamplePOJO.generateTableCreateSQL(tableName);
        final SamplePOJO pojo = new SamplePOJO();

        initTable(tableName, createSQL);

        client.register(SamplePOJO.class, client.getTableSchema(tableName));

        try (InsertResponse response = client.insert(tableName, Collections.singletonList(pojo), settings).get(30, TimeUnit.SECONDS)) {
            Assert.assertEquals(response.getWrittenRows(), 1);
        }

        try (CommandResponse resp = client.execute("SYSTEM FLUSH LOGS").get()) {
        }

        List<GenericRecord> logRecords = client.queryAll("SELECT query_id, log_comment FROM system.query_log WHERE query_id = '" + settings.getQueryId() + "'");
        Assert.assertEquals(logRecords.get(0).getString("query_id"), settings.getQueryId());
        Assert.assertEquals(logRecords.get(0).getString("log_comment"), logComment == null ? "" : logComment);
    }

    @Test(groups = { "integration" })
    public void testInsertSettingsDeduplicationToken() throws Exception {
        final String tableName = "insert_settings_database_test";
        final String createTableSQL = "CREATE TABLE " + tableName + " ( A Int64 ) ENGINE = MergeTree ORDER BY A SETTINGS " +
                "non_replicated_deduplication_window = 100";
        final String deduplicationToken = RandomStringUtils.randomAlphabetic(36);

        initTable(tableName, createTableSQL);

        InsertSettings insertSettings = settings.setInputStreamCopyBufferSize(8198 * 2)
                .setDeduplicationToken(deduplicationToken);

        for (int i = 0; i < 3; ++i) {
            ByteArrayOutputStream data = new ByteArrayOutputStream();
            PrintWriter writer = new PrintWriter(data);
            writer.printf("%d\n", i);
            writer.flush();
            InsertResponse response = client.insert(tableName, new ByteArrayInputStream(data.toByteArray()), ClickHouseFormat.TSV, insertSettings)
                    .get(30, TimeUnit.SECONDS);
            response.close();
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM " + tableName);
        assertEquals(records.size(), 1);
    }

    @DataProvider( name = "logCommentDataProvider")
    public static Object[] logCommentDataProvider() {
        return new Object[][] {
                { "Test log comment" },
                { "Another log comment?" },
                { "Log comment with special characters: !@#$%^&*()" },
                { "Log comment with unicode: 你好" },
                { "", },
                { "               "},
                { null }
        };
    }

    @Test(enabled = false)
    public void testWriter() throws Exception {
        String tableName = "very_long_table_name_with_uuid_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (name String, " +
                "  v1 Float32, " +
                "  v2 Float32, " +
                "  attrs Nullable(String), " +
                "  corrected_time DateTime('UTC') DEFAULT now()," +
                "  special_attr Nullable(Int8) DEFAULT -1)" +
                "  Engine = MergeTree ORDER by ()";

        initTable(tableName, tableCreate);

        ZonedDateTime correctedTime = Instant.now().atZone(ZoneId.of("UTC"));
        Object[][] rows = new Object[][] {
                {"foo1", 0.3f, 0.6f, "a=1,b=2,c=5", correctedTime, 10},
                {"foo2", 0.6f, 0.1f, "a=1,b=2,c=5", correctedTime, null},
                {"foo3", 0.7f, 0.4f, "a=1,b=2,c=5", null, null},
                {"foo4", 0.8f, 0.5f, null, null, null},
        };

        TableSchema schema = client.getTableSchema(tableName);

        try (InsertResponse response = client.insert(tableName, out -> {
            RowBinaryFormatSerializer formatWriter = new RowBinaryFormatSerializer(out);
            for (Object[] row : rows) {
                formatWriter.writeString((String) row[0]);
                formatWriter.writeFloat32((float)row[1]);
                formatWriter.writeFloat32((float)row[2]);
                if (row[3] == null) {
                    formatWriter.writeNull();
                } else {
                    formatWriter.writeString((String) row[3]);
                }
                if (row[4] == null) {
                    formatWriter.writeDefault();
                } else {
                    formatWriter.writeDateTime((ZonedDateTime) row[4], null);
                }
                if (row[5] == null) {
                    formatWriter.writeDefault();
                } else {
                    formatWriter.writeInt8(((Integer) row[5]).byteValue());
                }
            }
        }, ClickHouseFormat.RowBinaryWithDefaults, new InsertSettings()).get()) {
            System.out.println("Rows written: " + response.getWrittenRows());
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM \"" + tableName  + "\"" );

        for (GenericRecord record : records) {
            System.out.println("> " + record.getString(1) + ", " + record.getFloat(2) + ", " + record.getFloat(3));
        }
    }

    @Test
    public void testAdvancedWriter() throws Exception {
        String tableName = "very_long_table_name_with_uuid_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (name String, " +
                "  v1 Float32, " +
                "  v2 Float32, " +
                "  attrs Nullable(String), " +
                "  corrected_time DateTime('UTC') DEFAULT now()," +
                "  special_attr Nullable(Int8) DEFAULT -1)" +
                "  Engine = MergeTree ORDER by ()";

        initTable(tableName, tableCreate);

        ZonedDateTime correctedTime = Instant.now().atZone(ZoneId.of("UTC"));
        Object[][] rows = new Object[][] {
                {"foo1", 0.3f, 0.6f, "a=1,b=2,c=5", correctedTime, 10},
                {"foo2", 0.6f, 0.1f, "a=1,b=2,c=5", correctedTime, null},
                {"foo3", 0.7f, 0.4f, "a=1,b=2,c=5", null, null},
                {"foo4", 0.8f, 0.5f, null, null, null},
        };

        TableSchema schema = client.getTableSchema(tableName);

        ClickHouseFormat format = ClickHouseFormat.RowBinaryWithDefaults;
        try (InsertResponse response = client.insert(tableName, out -> {
            RowBinaryFormatWriter w = new RowBinaryFormatWriter(out, schema, format);
            for (Object[] row : rows) {
                for (int i = 0; i < row.length; i++) {
                    w.setValue(i + 1, row[i]);
                }
                w.commitRow();
            }
        }, format, new InsertSettings()).get()) {
            System.out.println("Rows written: " + response.getWrittenRows());
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM \"" + tableName  + "\"" );

        for (GenericRecord record : records) {
            System.out.println("> " + record.getString(1) + ", " + record.getFloat(2) + ", " + record.getFloat(3));
        }
    }

    @Test
    public void testCollectionInsert() throws Exception {
        String tableName = "very_long_table_name_with_uuid_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (name String, " +
                "  v1 Float32, " +
                "  v2 Float32, " +
                "  attrs Nullable(String), " +
                "  corrected_time DateTime('UTC') DEFAULT now()," +
                "  special_attr Nullable(Int8) DEFAULT -1)" +
                "  Engine = MergeTree ORDER by ()";

        initTable(tableName, tableCreate);

        String correctedTime = Instant.now().atZone(ZoneId.of("UTC")).format(DataTypeUtils.DATETIME_FORMATTER);
        String[] rows = new String[] {
                "{ \"name\": \"foo1\", \"v1\": 0.3, \"v2\": 0.6, \"attrs\": \"a=1,b=2,c=5\", \"corrected_time\": \"" + correctedTime + "\", \"special_attr\": 10}",
                "{ \"name\": \"foo1\", \"v1\": 0.3, \"v2\": 0.6, \"attrs\": \"a=1,b=2,c=5\", \"corrected_time\": \"" + correctedTime + "\"}",
                "{ \"name\": \"foo1\", \"v1\": 0.3, \"v2\": 0.6, \"attrs\": \"a=1,b=2,c=5\" }",
                "{ \"name\": \"foo1\", \"v1\": 0.3, \"v2\": 0.6 }",
        };

        try (InsertResponse response = client.insert(tableName, out -> {
            for (String row : rows) {
                out.write(row.getBytes());
            }
        }, ClickHouseFormat.JSONEachRow, new InsertSettings()).get()) {
            System.out.println("Rows written: " + response.getWrittenRows());
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM \"" + tableName  + "\"" );

        for (GenericRecord record : records) {
            System.out.println("> " + record.getString(1) + ", " + record.getFloat(2) + ", " + record.getFloat(3));
        }
    }


    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
    }

    @Test(groups = {"integration"}, dataProvider = "testAppCompressionDataProvider", dataProviderClass = InsertTests.class)
    public void testAppCompression(String algo) throws Exception {
        String tableName = "very_long_table_name_with_uuid_" + UUID.randomUUID().toString().replace('-', '_');
        String tableCreate = "CREATE TABLE \"" + tableName + "\" " +
                " (name String, " +
                "  v1 Float32, " +
                "  v2 Float32, " +
                "  attrs Nullable(String), " +
                "  corrected_time DateTime('UTC') DEFAULT now()," +
                "  special_attr Nullable(Int8) DEFAULT -1)" +
                "  Engine = MergeTree ORDER by ()";

        initTable(tableName, tableCreate);

        String correctedTime = Instant.now().atZone(ZoneId.of("UTC")).format(DataTypeUtils.DATETIME_FORMATTER);
        String[] data = new String[] {
                "{ \"name\": \"foo1\", \"v1\": 0.3, \"v2\": 0.6, \"attrs\": \"a=1,b=2,c=5\", \"corrected_time\": \"" + correctedTime + "\", \"special_attr\": 10}",
                "{ \"name\": \"foo1\", \"v1\": 0.3, \"v2\": 0.6, \"attrs\": \"a=1,b=2,c=5\", \"corrected_time\": \"" + correctedTime + "\"}",
                "{ \"name\": \"foo1\", \"v1\": 0.3, \"v2\": 0.6, \"attrs\": \"a=1,b=2,c=5\" }",
                "{ \"name\": \"foo1\", \"v1\": 0.3, \"v2\": 0.6 }",
        };

        byte[][] compressedData = new byte[data.length][];
        for (int i = 0 ; i < data.length; i++) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            if (algo.equalsIgnoreCase("gzip")) {
                GZIPOutputStream gz = new GZIPOutputStream(baos);
                gz.write(data[i].getBytes(StandardCharsets.UTF_8));
                gz.finish();
            } else if (algo.equalsIgnoreCase("lz4")) {
                FramedLZ4CompressorOutputStream lz4 = new FramedLZ4CompressorOutputStream(baos);
                lz4.write(data[i].getBytes(StandardCharsets.UTF_8));
                lz4.finish();
            } else if (algo.equalsIgnoreCase("snappy")) {
                byte bytes[] = data[i].getBytes(StandardCharsets.UTF_8);

                SnappyCompressorOutputStream snappy = new SnappyCompressorOutputStream(baos,bytes.length, 32);
                snappy.write(bytes);
                snappy.finish();
            }
            compressedData[i] = baos.toByteArray();
        }

        InsertSettings insertSettings = new InsertSettings()
                .appCompressedData(true, algo);
        try (InsertResponse response = client.insert(tableName, out -> {
            for (byte[] row : compressedData) {
                out.write(row);
            }
        }, ClickHouseFormat.JSONEachRow, insertSettings).get()) {
            System.out.println("Rows written: " + response.getWrittenRows());
        }

        List<GenericRecord> records = client.queryAll("SELECT * FROM \"" + tableName  + "\"" );

        for (GenericRecord record : records) {
            System.out.println("> " + record.getString(1) + ", " + record.getFloat(2) + ", " + record.getFloat(3));
        }
    }

    @DataProvider(name = "testAppCompressionDataProvider")
    public static Object[][] testAppCompressionDataProvider() {
        return new Object[][] {
                {"gzip"},
                {"lz4"},
//                {"snappy"}, // TODO: investigate proper snappy compression
        };
    }

    @Test(groups = { "integration" }, enabled = true)
    public void testPOJOWithDynamicType() throws Exception {
        if (isVersionMatch("(,24.8]")) {
            return;
        }
        final String tableName = "pojo_dynamic_type_test";
        final String createSQL = PojoWithDynamic.getTableDef(tableName);

        initTable(tableName, createSQL, (CommandSettings)
                new CommandSettings().serverSetting("allow_experimental_dynamic_type", "1"));

        TableSchema tableSchema = client.getTableSchema(tableName);
        client.register(PojoWithDynamic.class, tableSchema);

        List<PojoWithDynamic> data = new ArrayList<>();
        data.add(new PojoWithDynamic(1, "test_string", null));
        data.add(new PojoWithDynamic(2, 10000L, Arrays.asList(1, 2, 3)));
        data.add(new PojoWithDynamic(3, 10000L, LocalDateTime.now()));
        data.add(new PojoWithDynamic(4, 10000L, ZonedDateTime.now(ZoneId.of("America/Chicago"))));
        data.add(new PojoWithDynamic(5, 10000L, LocalDate.now()));
        try (InsertResponse response = client.insert(tableName, data, settings)
                .get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS)) {
            Assert.assertEquals(response.getWrittenRows(), data.size());
        }

        List<PojoWithDynamic> items =
                client.queryAll("SELECT * FROM " + tableName, PojoWithDynamic.class, tableSchema);

        int i = 0;
        for (PojoWithDynamic item : items) {
            if (item.rowId == 3) {
                assertEquals(((ZonedDateTime) item.getNullableAny()).toLocalDateTime(), data.get(i++).getNullableAny());
            } else if (item.rowId == 5) {
                assertEquals(((ZonedDateTime) item.getNullableAny()).toLocalDate(), data.get(i++).getNullableAny());
            } else {
                assertEquals(item, data.get(i++));
            }
        }
    }

    protected void initTable(String tableName, String createTableSQL) throws Exception {
        initTable(tableName, createTableSQL, new CommandSettings());
    }

    protected void initTable(String tableName, String createTableSQL, CommandSettings settings) throws Exception {
        client.execute("DROP TABLE IF EXISTS " + tableName, settings).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);
        client.execute(createTableSQL, settings).get(EXECUTE_CMD_TIMEOUT, TimeUnit.SECONDS);
    }

    private boolean isVersionMatch(String versionExpression) {
        List<GenericRecord> serverVersion = client.queryAll("SELECT version()");
        return ClickHouseVersion.of(serverVersion.get(0).getString(1)).check(versionExpression);
    }
}
