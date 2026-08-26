package com.clickhouse.client.observability.otel;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.metrics.OperationType;
import com.clickhouse.client.api.observability.SpanAttribute;
import com.clickhouse.client.api.observability.otel.OpenTelemetrySpanRecorder;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class OpenTelemetrySpanRecorderTest extends BaseIntegrationTest {

    private static final String TABLE = "otel_span_recorder_test_table";

    private InMemorySpanExporter exporter;
    private OpenTelemetrySdk openTelemetry;
    private Client client;
    private String database;

    @BeforeMethod(groups = {"integration"})
    void setUp() throws Exception {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        database = ClickHouseServerForTest.getDatabase();
        exporter = InMemorySpanExporter.create();
        openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                        .build())
                .build();
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(database)
                .setSpanRecorder(new OpenTelemetrySpanRecorder(openTelemetry))
                .build();
        client.execute("DROP TABLE IF EXISTS " + TABLE).get();
        client.execute("CREATE TABLE " + TABLE + " (id Int32, name String) ENGINE = MergeTree ORDER BY id").get();
        client.execute("INSERT INTO " + TABLE + " VALUES (1, 'a'), (2, 'b'), (3, 'c')").get();
        exporter.reset();
    }

    @AfterMethod(groups = {"integration"})
    void tearDown() throws Exception {
        if (client != null) {
            client.execute("DROP TABLE IF EXISTS " + TABLE).get();
            client.close();
        }
        if (openTelemetry != null) {
            openTelemetry.close();
        }
    }

    @Test(groups = {"integration"})
    public void testQueryExportsOperationSpanWithRequestChild() throws Exception {
        QuerySettings settings = new QuerySettings().waitEndOfQuery(true);
        try (QueryResponse response = client.query("SELECT id FROM " + TABLE + " ORDER BY id", settings).get()) {
            Assert.assertNotNull(response);
            Assert.assertEquals(response.getMetrics().getOperationType(), OperationType.QUERY);
        }

        SpanData operation = spanByName("query " + database);
        Assert.assertEquals(operation.getKind(), SpanKind.CLIENT);
        Assert.assertEquals(stringAttribute(operation, SpanAttribute.DB_SYSTEM_NAME), "clickhouse");
        Assert.assertEquals(stringAttribute(operation, SpanAttribute.DB_NAMESPACE), database);
        Assert.assertEquals(stringAttribute(operation, SpanAttribute.DB_QUERY_TEXT),
                "SELECT id FROM " + TABLE + " ORDER BY id");
        Assert.assertEquals(longAttribute(operation, SpanAttribute.DB_RESPONSE_RETURNED_ROWS), Long.valueOf(3L));
        Assert.assertEquals(longAttribute(operation, SpanAttribute.CLICKHOUSE_RESPONSE_READ_ROWS), Long.valueOf(3L));
        Assert.assertNotNull(longAttribute(operation, SpanAttribute.CLICKHOUSE_RESPONSE_READ_BYTES));
        // the query track does not report what an insert would
        Assert.assertNull(longAttribute(operation, SpanAttribute.CLICKHOUSE_RESPONSE_WRITTEN_ROWS));
        Assert.assertNull(longAttribute(operation, SpanAttribute.CLICKHOUSE_RESPONSE_WRITTEN_BYTES));
        Assert.assertNotNull(stringAttribute(operation, SpanAttribute.CLICKHOUSE_QUERY_ID));
        Assert.assertEquals(operation.getStatus().getStatusCode(), StatusCode.UNSET);

        SpanData request = spanByName("POST");
        Assert.assertEquals(request.getTraceId(), operation.getTraceId());
        Assert.assertEquals(request.getParentSpanId(), operation.getSpanId());
        Assert.assertEquals(longAttribute(request, SpanAttribute.HTTP_RESPONSE_STATUS_CODE), Long.valueOf(200L));
        Assert.assertEquals(stringAttribute(request, SpanAttribute.SERVER_ADDRESS),
                getServer(ClickHouseProtocol.HTTP).getHost());
    }

    @Test(groups = {"integration"})
    public void testFailingQueryExportsErrorStatusAndServerCode() {
        try {
            client.query("SELECT * FROM table_that_does_not_exist_at_all").get();
            Assert.fail("querying a missing table must fail");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof ServerException, "Unexpected cause: " + e.getCause());
        } catch (ServerException e) {
            // synchronous operations report the server failure directly
        } catch (Exception e) {
            Assert.fail("Unexpected exception: " + e);
        }

        SpanData operation = spanByName("query " + database);
        Assert.assertEquals(operation.getStatus().getStatusCode(), StatusCode.ERROR);
        Assert.assertEquals(stringAttribute(operation, SpanAttribute.ERROR_TYPE), ServerException.class.getName());
        Assert.assertEquals(longAttribute(operation, SpanAttribute.DB_RESPONSE_STATUS_CODE), Long.valueOf(60L));

        SpanData request = spanByName("POST");
        Assert.assertEquals(request.getStatus().getStatusCode(), StatusCode.ERROR);
        Assert.assertEquals(stringAttribute(request, SpanAttribute.ERROR_TYPE), ServerException.class.getName());
        Assert.assertEquals(longAttribute(request, SpanAttribute.HTTP_RESPONSE_STATUS_CODE), Long.valueOf(404L));
    }

    @Test(groups = {"integration"})
    public void testInsertExportsSpanWithBatchSize() throws Exception {
        client.register(SpanRecorderPojo.class, client.getTableSchema(TABLE));
        exporter.reset();

        SpanRecorderPojo pojo = new SpanRecorderPojo();
        pojo.setId(4);
        pojo.setName("d");
        try (InsertResponse response = client.insert(TABLE, java.util.Collections.singletonList(pojo)).get()) {
            Assert.assertEquals(response.getMetrics().getOperationType(), OperationType.INSERT);
        }

        SpanData operation = spanByName("insert " + database + "." + TABLE);
        Assert.assertEquals(stringAttribute(operation, SpanAttribute.DB_OPERATION_NAME), "insert");
        Assert.assertEquals(stringAttribute(operation, SpanAttribute.DB_COLLECTION_NAME), TABLE);
        Assert.assertEquals(longAttribute(operation, SpanAttribute.DB_OPERATION_BATCH_SIZE), Long.valueOf(1L));
        Assert.assertEquals(longAttribute(operation, SpanAttribute.CLICKHOUSE_RESPONSE_WRITTEN_ROWS), Long.valueOf(1L));
        Assert.assertNotNull(longAttribute(operation, SpanAttribute.CLICKHOUSE_RESPONSE_WRITTEN_BYTES));
        // the insert track does not report what a query would
        Assert.assertNull(longAttribute(operation, SpanAttribute.DB_RESPONSE_RETURNED_ROWS));
        Assert.assertNull(longAttribute(operation, SpanAttribute.CLICKHOUSE_RESPONSE_READ_ROWS));
        Assert.assertEquals(operation.getStatus().getStatusCode(), StatusCode.UNSET);
        Assert.assertEquals(spanByName("POST").getParentSpanId(), operation.getSpanId());
    }

    @Test(groups = {"integration"})
    public void testStreamInsertExportsSpanWithWrittenRows() throws Exception {
        // the second insert entry point: it does not know the batch size, but it reports the same
        // insert track as a POJO insert
        byte[] rows = "4,d\n5,e\n".getBytes(StandardCharsets.UTF_8);
        try (InsertResponse response = client.insert(TABLE, new ByteArrayInputStream(rows),
                ClickHouseFormat.CSV).get()) {
            Assert.assertEquals(response.getMetrics().getOperationType(), OperationType.INSERT);
        }

        SpanData operation = spanByName("insert " + database + "." + TABLE);
        Assert.assertEquals(longAttribute(operation, SpanAttribute.CLICKHOUSE_RESPONSE_WRITTEN_ROWS), Long.valueOf(2L));
        Assert.assertNotNull(longAttribute(operation, SpanAttribute.CLICKHOUSE_RESPONSE_WRITTEN_BYTES));
        Assert.assertNull(longAttribute(operation, SpanAttribute.DB_RESPONSE_RETURNED_ROWS));
        Assert.assertNull(longAttribute(operation, SpanAttribute.DB_OPERATION_BATCH_SIZE),
                "a stream insert does not know the batch size");
        Assert.assertEquals(operation.getStatus().getStatusCode(), StatusCode.UNSET);
    }

    private SpanData spanByName(String name) {
        List<SpanData> spans = exporter.getFinishedSpanItems();
        for (SpanData span : spans) {
            if (name.equals(span.getName())) {
                return span;
            }
        }
        Assert.fail("No span named '" + name + "' in " + spans);
        return null;
    }

    private static String stringAttribute(SpanData span, SpanAttribute attribute) {
        return span.getAttributes().get(AttributeKey.stringKey(attribute.getKey()));
    }

    private static Long longAttribute(SpanData span, SpanAttribute attribute) {
        return span.getAttributes().get(AttributeKey.longKey(attribute.getKey()));
    }

    public static class SpanRecorderPojo {

        private int id;

        private String name;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
