package com.clickhouse.client.insert;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.ClientSettings;
import com.clickhouse.client.api.command.CommandResponse;
import com.clickhouse.client.api.command.CommandSettings;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseVersion;
import org.apache.commons.lang3.StringEscapeUtils;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class InsertTests extends BaseIntegrationTest {
    private Client client;
    private InsertSettings settings;

    private boolean useClientCompression = false;

    private boolean useHttpCompression = false;

    InsertTests() {
    }

    public InsertTests(boolean useClientCompression, boolean useHttpCompression) {
        this.useClientCompression = useClientCompression;
        this.useHttpCompression = useHttpCompression;
    }

    @BeforeMethod(groups = { "integration" })
    public void setUp() throws IOException {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), false)
                .setUsername("default")
                .setPassword("")
                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "true").equals("true"))
                .compressClientRequest(useClientCompression)
                .useHttpCompression(useHttpCompression)
                .build();
        settings = new InsertSettings()
                .setDeduplicationToken(RandomStringUtils.randomAlphabetic(36))
                .setQueryId(String.valueOf(UUID.randomUUID()));
    }

    @AfterMethod(groups = { "integration" })
    public void tearDown() {
        client.close();
    }

    private void createTable(String tableQuery) throws ClickHouseException {
        try (ClickHouseClient client = ClickHouseClient.builder().config(new ClickHouseConfig())
                        .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                        .build()) {
            client.read(getServer(ClickHouseProtocol.HTTP)).query(tableQuery).executeAndWait().close();
        }
    }

    private void dropTable(String tableName) throws ClickHouseException {
        try (ClickHouseClient client = ClickHouseClient.builder().config(new ClickHouseConfig())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build()) {
            String tableQuery = "DROP TABLE IF EXISTS " + tableName;
            client.read(getServer(ClickHouseProtocol.HTTP)).query(tableQuery).executeAndWait().close();
        }
    }

    @Test(groups = { "integration" }, enabled = true)
    public void insertSimplePOJOs() throws Exception {
        String tableName = "simple_pojo_table";
        String createSQL = SamplePOJO.generateTableCreateSQL(tableName);
        String uuid = UUID.randomUUID().toString();
        System.out.println(createSQL);
        dropTable(tableName);
        createTable(createSQL);
        client.register(SamplePOJO.class, client.getTableSchema(tableName, "default"));
        List<Object> simplePOJOs = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            simplePOJOs.add(new SamplePOJO());
        }
        settings.setQueryId(uuid);
        InsertResponse response = client.insert(tableName, simplePOJOs, settings).get(30, TimeUnit.SECONDS);

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
        client.execute("DROP TABLE IF EXISTS " + tableName, commandSettings).get(1, TimeUnit.SECONDS);
        client.execute(createSQL, commandSettings).get(1, TimeUnit.SECONDS);

        client.register(PojoWithJSON.class, client.getTableSchema(tableName, "default"));
        PojoWithJSON pojo = new PojoWithJSON();
        pojo.setEventPayload(originalJsonStr);
        List<Object> data = Arrays.asList(pojo);

        InsertSettings insertSettings = new InsertSettings()
                .serverSetting(ClientSettings.INPUT_FORMAT_BINARY_READ_JSON_AS_STRING, "1");
        InsertResponse response = client.insert(tableName, data, insertSettings).get(30, TimeUnit.SECONDS);
        assertEquals(response.getWrittenRows(), 1);

        QuerySettings settings = new QuerySettings()
                .setFormat(ClickHouseFormat.CSV);
        try (QueryResponse resp = client.query("SELECT * FROM " + tableName, settings).get(1, TimeUnit.SECONDS)) {
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

        dropTable(tableName);
        createTable(createSQL);
        client.register(SamplePOJO.class, client.getTableSchema(tableName, "default"));

        System.out.println("Inserting POJO: " + pojo);
        try (InsertResponse response = client.insert(tableName, Collections.singletonList(pojo), settings).get(30, TimeUnit.SECONDS)) {
            Assert.assertEquals(response.getWrittenRows(), 1);
        }

        try (QueryResponse queryResponse =
                client.query("SELECT * FROM " + tableName + " LIMIT 1").get(30, TimeUnit.SECONDS)) {

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
        }
    }

    @Test
    public void testInsertingPOJOWithNullValueForNonNullableColumn() throws Exception {
        final String tableName = "single_pojo_table";
        final String createSQL = SamplePOJO.generateTableCreateSQL(tableName);
        final SamplePOJO pojo = new SamplePOJO();

        pojo.setBoxedByte(null);

        dropTable(tableName);
        createTable(createSQL);
        client.register(SamplePOJO.class, client.getTableSchema(tableName, "default"));



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
        dropTable(tableName);
        createTable(createSQL);

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


    @Test(groups = { "integration" }, enabled = true)
    public void insertRawDataSimple() throws Exception {
        insertRawDataSimple(1000);
    }
    public void insertRawDataSimple(int numberOfRecords) throws Exception {
        final String tableName = "raw_data_table";
        createTable(String.format("CREATE TABLE IF NOT EXISTS %s (Id UInt32, event_ts Timestamp, name String, p1 Int64, p2 String) ENGINE = MergeTree() ORDER BY ()", tableName));

        InsertSettings settings = new InsertSettings()
                .setDeduplicationToken(RandomStringUtils.randomAlphabetic(36))
                .setQueryId(String.valueOf(UUID.randomUUID()))
                .setInputStreamCopyBufferSize(8198 * 2);
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(data);
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
    public void testInsertMetricsOperationId() throws Exception {
        final String tableName = "insert_metrics_test";
        final String createSQL = "CREATE TABLE " + tableName +
                                 " (Id UInt32, event_ts Timestamp, name String, p1 Int64, p2 String) ENGINE = MergeTree() ORDER BY ()";
        dropTable(tableName);
        createTable(createSQL);

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

        try (ClickHouseClient client = ClickHouseClient.builder().config(new ClickHouseConfig())
            .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
            .build()) {
            client.read(getServer(ClickHouseProtocol.HTTP)).query(dropDatabaseSQL).executeAndWait().close();
            client.read(getServer(ClickHouseProtocol.HTTP)).query(createDatabaseSQL).executeAndWait().close();
            client.read(getServer(ClickHouseProtocol.HTTP)).query(createTableSQL).executeAndWait().close();
        }


        InsertSettings insertSettings = settings.setInputStreamCopyBufferSize(8198 * 2)
            .setDeduplicationToken(RandomStringUtils.randomAlphabetic(36))
            .setQueryId(String.valueOf(UUID.randomUUID()));
        insertSettings.setDatabase(new_database);

        ByteArrayOutputStream data = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(data);
        for (int i = 0; i < 1000; i++) {
            writer.printf("%d\t%s\t%s\t%d\t%s\n", i, "2021-01-01 00:00:00", "name" + i, i, "p2");
        }
        writer.flush();
        InsertResponse response = client.insert(tableName, new ByteArrayInputStream(data.toByteArray()),
            ClickHouseFormat.TSV, insertSettings).get(30, TimeUnit.SECONDS);
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

        dropTable(tableName);
        createTable(createSQL);
        client.register(SamplePOJO.class, client.getTableSchema(tableName, "default"));

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

        dropTable(tableName);
        createTable(createTableSQL);

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
}
