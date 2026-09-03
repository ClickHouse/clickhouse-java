package com.clickhouse.client.observability;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.observability.CapturingMetricsRecorder;
import com.clickhouse.client.api.observability.CapturingMetricsRecorder.RecordedMetric;
import com.clickhouse.client.api.observability.MetricAttribute;
import com.clickhouse.client.api.observability.MetricName;
import com.clickhouse.client.api.query.QueryResponse;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class MetricsRecorderTest extends BaseIntegrationTest {

    private static final String TABLE = "metrics_recorder_test_table";

    private CapturingMetricsRecorder recorder;
    private Client client;
    private String database;

    @BeforeMethod(groups = {"integration"})
    void setUp() throws Exception {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        database = ClickHouseServerForTest.getDatabase();
        recorder = new CapturingMetricsRecorder();
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(database)
                .setMetricsRecorder(recorder)
                .build();
        client.execute("DROP TABLE IF EXISTS " + TABLE).get();
        client.execute("CREATE TABLE " + TABLE + " (value String) ENGINE = MergeTree ORDER BY value").get();
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
    public void testSuccessfulQueryReportsDurationAndOperationAttributes() throws Exception {
        try (QueryResponse response = client.query("SELECT value FROM " + TABLE).get()) {
            Assert.assertNotNull(response);
        }

        RecordedMetric duration = recorder.getOnlyMetric(MetricName.OPERATION_DURATION);
        Assert.assertTrue(duration.getValue() > 0, "Unexpected duration: " + duration.getValue());
        Assert.assertEquals(duration.getAttribute(MetricAttribute.DB_SYSTEM_NAME), "clickhouse");
        Assert.assertEquals(duration.getAttribute(MetricAttribute.DB_NAMESPACE), database);
        Assert.assertEquals(duration.getAttribute(MetricAttribute.DB_OPERATION_NAME), "query");
        Assert.assertNull(duration.getAttribute(MetricAttribute.ERROR_TYPE));
        Assert.assertEquals(recorder.getOnlyMetric(MetricName.OPERATION_COUNT).getValue(), 1d);
    }

    @Test(groups = {"integration"})
    public void testSuccessfulInsertReportsTargetTable() throws Exception {
        client.register(ValuePojo.class, client.getTableSchema(TABLE));
        recorder.clear();

        client.insert(TABLE, Collections.singletonList(new ValuePojo("a"))).get().close();

        RecordedMetric duration = recorder.getOnlyMetric(MetricName.OPERATION_DURATION);
        Assert.assertTrue(duration.getValue() > 0, "Unexpected duration: " + duration.getValue());
        Assert.assertEquals(duration.getAttribute(MetricAttribute.DB_OPERATION_NAME), "insert");
        Assert.assertEquals(duration.getAttribute(MetricAttribute.DB_COLLECTION_NAME), TABLE);
        Assert.assertNull(duration.getAttribute(MetricAttribute.ERROR_TYPE));
        Assert.assertTrue(recorder.getOnlyMetric(MetricName.OPERATION_SERIALIZATION_DURATION).getValue() > 0);
    }

    @Test(groups = {"integration"})
    public void testServerErrorReportsErrorTypeAndStatusCode() {
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

        RecordedMetric count = recorder.getOnlyMetric(MetricName.OPERATION_COUNT);
        Assert.assertEquals(count.getValue(), 1d, "a failed operation is counted too");
        Assert.assertEquals(count.getAttribute(MetricAttribute.ERROR_TYPE), ServerException.class.getName());
        Assert.assertEquals(count.getAttribute(MetricAttribute.DB_RESPONSE_STATUS_CODE),
                ServerException.TABLE_NOT_FOUND);
        Assert.assertTrue(recorder.getOnlyMetric(MetricName.OPERATION_DURATION).getValue() > 0);
    }

    @Test(groups = {"integration"})
    public void testTableSchemaLookupIsReportedAsQuery() {
        TableSchema schema = client.getTableSchema(TABLE);
        Assert.assertEquals(schema.getColumns().size(), 1);

        RecordedMetric count = recorder.getOnlyMetric(MetricName.OPERATION_COUNT);
        Assert.assertEquals(count.getAttribute(MetricAttribute.DB_OPERATION_NAME), "query");
        Assert.assertNull(count.getAttribute(MetricAttribute.DB_COLLECTION_NAME),
                "an operation the client implements on top of a query is reported as one");
    }

    public static class ValuePojo {
        private String value;

        public ValuePojo() {
        }

        public ValuePojo(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
