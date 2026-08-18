package com.clickhouse.client.observability;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.observability.CapturingSpanRecorder;
import com.clickhouse.client.api.observability.CapturingSpanRecorder.CapturedSpan;
import com.clickhouse.client.api.observability.SpanAttribute;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class SpanRecorderTest extends BaseIntegrationTest {

    private static final String TABLE = "span_recorder_test_table";

    private CapturingSpanRecorder recorder;
    private Client client;
    private String database;

    @BeforeMethod(groups = {"integration"})
    void setUp() throws Exception {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        database = ClickHouseServerForTest.getDatabase();
        recorder = new CapturingSpanRecorder();
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(database)
                .setSpanRecorder(recorder)
                .build();
        client.execute("DROP TABLE IF EXISTS " + TABLE).get();
        client.execute("CREATE TABLE " + TABLE + " (id Int32, name String) ENGINE = MergeTree ORDER BY id").get();
        client.execute("INSERT INTO " + TABLE + " VALUES (1, 'a'), (2, 'b'), (3, 'c')").get();
        recorder.clear();
    }

    @AfterMethod(groups = {"integration"})
    void tearDown() throws Exception {
        if (client != null) {
            client.execute("DROP TABLE IF EXISTS " + TABLE).get();
            client.close();
        }
    }

    @Test(groups = {"integration"})
    public void testQuerySpanReportsReturnedRowsAndServerQueryId() throws Exception {
        QuerySettings settings = new QuerySettings().waitEndOfQuery(true);
        try (QueryResponse response = client.query("SELECT id FROM " + TABLE + " ORDER BY id", settings).get()) {
            Assert.assertNotNull(response);
        }

        CapturedSpan operationSpan = recorder.operationSpan();
        Assert.assertEquals(operationSpan.getName(), "query " + database);
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_SYSTEM_NAME), "clickhouse");
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_NAMESPACE), database);
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_RESPONSE_RETURNED_ROWS), 3L);
        Assert.assertNotNull(operationSpan.getAttribute(SpanAttribute.CLICKHOUSE_QUERY_ID),
                "the server assigns a query id when the client did not");
        Assert.assertNull(operationSpan.getErrorType());
        Assert.assertEquals(operationSpan.getEndCount(), 1);

        List<CapturedSpan> requestSpans = recorder.requestSpans(operationSpan);
        Assert.assertEquals(requestSpans.size(), 1);
        Assert.assertEquals(requestSpans.get(0).getAttribute(SpanAttribute.HTTP_RESPONSE_STATUS_CODE), 200);
    }

    @Test(groups = {"integration"})
    public void testServerErrorRecordsErrorTypeAndStatusCode() {
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

        CapturedSpan operationSpan = recorder.operationSpan();
        Assert.assertEquals(operationSpan.getErrorType(), ServerException.class.getName());
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_RESPONSE_STATUS_CODE),
                ServerException.TABLE_NOT_FOUND);
        Assert.assertEquals(operationSpan.getEndCount(), 1);

        List<CapturedSpan> requestSpans = recorder.requestSpans(operationSpan);
        Assert.assertEquals(requestSpans.size(), 1);
        CapturedSpan requestSpan = requestSpans.get(0);
        Assert.assertEquals(requestSpan.getErrorType(), ServerException.class.getName());
        Assert.assertEquals(requestSpan.getAttribute(SpanAttribute.DB_RESPONSE_STATUS_CODE),
                ServerException.TABLE_NOT_FOUND);
        Object httpStatus = requestSpan.getAttribute(SpanAttribute.HTTP_RESPONSE_STATUS_CODE);
        Assert.assertNotNull(httpStatus, "the HTTP status of the error response must be recorded");
        Assert.assertTrue((Integer) httpStatus >= 400, "Unexpected HTTP status: " + httpStatus);
        Assert.assertEquals(requestSpan.getEndCount(), 1);
    }

    @Test(groups = {"integration"})
    public void testPingSpan() {
        Assert.assertTrue(client.ping());

        // a ping runs a query, so it is reported as one - the client does not name the operations it
        // implements on top of a query
        CapturedSpan operationSpan = recorder.operationSpan();
        Assert.assertEquals(operationSpan.getName(), "query " + database);
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_QUERY_TEXT), "SELECT 1 FORMAT TabSeparated");
        Assert.assertNull(operationSpan.getAttribute(SpanAttribute.DB_OPERATION_NAME));
        Assert.assertNull(operationSpan.getAttribute(SpanAttribute.DB_COLLECTION_NAME));
        Assert.assertEquals(operationSpan.getEndCount(), 1);
        Assert.assertEquals(recorder.requestSpans(operationSpan).size(), 1);
    }

    @Test(groups = {"integration"})
    public void testCommandSpanReportsStatement() throws Exception {
        client.execute("TRUNCATE TABLE " + TABLE).get();

        CapturedSpan operationSpan = recorder.operationSpan();
        Assert.assertEquals(operationSpan.getName(), "query " + database);
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_QUERY_TEXT), "TRUNCATE TABLE " + TABLE);
        Assert.assertNull(operationSpan.getAttribute(SpanAttribute.DB_OPERATION_NAME));
        Assert.assertNull(operationSpan.getErrorType());
        Assert.assertEquals(operationSpan.getEndCount(), 1);
    }

    @Test(groups = {"integration"})
    public void testTableSchemaSpanIsReportedAsQuery() {
        TableSchema schema = client.getTableSchema(TABLE);
        Assert.assertEquals(schema.getColumns().size(), 2);

        CapturedSpan operationSpan = recorder.operationSpan();
        Assert.assertEquals(operationSpan.getName(), "query " + database);
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_QUERY_TEXT),
                "DESCRIBE TABLE " + TABLE + " FORMAT TSKV");
        Assert.assertNull(operationSpan.getAttribute(SpanAttribute.DB_OPERATION_NAME));
        Assert.assertNull(operationSpan.getAttribute(SpanAttribute.DB_COLLECTION_NAME));
        Assert.assertEquals(operationSpan.getEndCount(), 1);
    }

    @Test(groups = {"integration"})
    public void testTableSchemaFromQuerySpanIsReportedAsQuery() {
        TableSchema schema = client.getTableSchemaFromQuery("SELECT id FROM " + TABLE);
        Assert.assertEquals(schema.getColumns().size(), 1);

        CapturedSpan operationSpan = recorder.operationSpan();
        Assert.assertEquals(operationSpan.getName(), "query " + database);
        Assert.assertEquals(operationSpan.getAttribute(SpanAttribute.DB_QUERY_TEXT),
                "DESC (SELECT id FROM " + TABLE + ") FORMAT TSKV");
        Assert.assertNull(operationSpan.getAttribute(SpanAttribute.DB_OPERATION_NAME));
        Assert.assertEquals(operationSpan.getEndCount(), 1);
    }
}
