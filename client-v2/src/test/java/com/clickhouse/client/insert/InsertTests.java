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
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Fault;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
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
}
