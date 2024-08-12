package com.clickhouse.client.insert;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.query.GenericRecord;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "false").equals("true"))
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

        settings.setInputStreamCopyBufferSize(8198 * 2);
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

    @Test(groups = { "integration" }, enabled = true)
    public void testNoHttpResponseFailure() {
        WireMockServer faultyServer = new WireMockServer( WireMockConfiguration
                .options().port(9090).notifier(new ConsoleNotifier(false)));
        faultyServer.start();

        byte[] requestBody = ("INSERT INTO table01 FORMAT " +
                ClickHouseFormat.TSV.name() + " \n1\t2\t3\n").getBytes();

        // First request gets no response
        faultyServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                        .withRequestBody(WireMock.binaryEqualTo(requestBody))
                .inScenario("Retry")
                .whenScenarioStateIs(STARTED)
                .willSetStateTo("Failed")
                .willReturn(WireMock.aResponse().withFault(Fault.EMPTY_RESPONSE)).build());

        // Second request gets a response (retry)
        faultyServer.addStubMapping(WireMock.post(WireMock.anyUrl())
                        .withRequestBody(WireMock.binaryEqualTo(requestBody))
                .inScenario("Retry")
                .whenScenarioStateIs("Failed")
                .willSetStateTo("Done")
                .willReturn(WireMock.aResponse()
                        .withHeader("X-ClickHouse-Summary",
                                "{ \"read_bytes\": \"10\", \"read_rows\": \"1\"}")).build());

        Client mockServerClient = new Client.Builder()
                .addEndpoint(Protocol.HTTP, "localhost", faultyServer.port(), false)
                .setUsername("default")
                .setPassword("")
                .useNewImplementation(true)
//                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "false").equals("true"))
                .compressClientRequest(false)
                .setOption(ClickHouseClientOption.RETRY.getKey(), "2")
                .build();
        try {
            InsertResponse insertResponse = mockServerClient.insert("table01",
                    new ByteArrayInputStream("1\t2\t3\n".getBytes()), ClickHouseFormat.TSV, settings).get(30, TimeUnit.SECONDS);
            insertResponse.close();
        } catch (Exception e) {
            Assert.fail("Unexpected exception", e);
        } finally {
            faultyServer.stop();
        }
    }
}
