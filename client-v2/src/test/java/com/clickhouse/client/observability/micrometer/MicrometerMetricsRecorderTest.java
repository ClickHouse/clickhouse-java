package com.clickhouse.client.observability.micrometer;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.observability.MetricAttribute;
import com.clickhouse.client.api.observability.MetricName;
import com.clickhouse.client.api.observability.micrometer.MicrometerMetricsRecorder;
import com.clickhouse.client.api.query.QueryResponse;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MicrometerMetricsRecorderTest extends BaseIntegrationTest {

    private static final String TABLE = "micrometer_metrics_recorder_test_table";

    private SimpleMeterRegistry registry;
    private Client client;
    private String database;

    @BeforeMethod(groups = {"integration"})
    void setUp() throws Exception {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        database = ClickHouseServerForTest.getDatabase();
        registry = new SimpleMeterRegistry();
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isCloud())
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(database)
                .setMetricsRecorder(new MicrometerMetricsRecorder(registry))
                .build();
        client.execute("DROP TABLE IF EXISTS " + TABLE).get();
        client.execute("CREATE TABLE " + TABLE + " (value String) ENGINE = MergeTree ORDER BY value").get();
        registry.clear();
    }

    @AfterMethod(groups = {"integration"})
    void tearDown() throws Exception {
        if (client != null) {
            client.execute("DROP TABLE IF EXISTS " + TABLE).get();
            client.close();
        }
        if (registry != null) {
            registry.close();
        }
    }

    @Test(groups = {"integration"})
    public void testSuccessfulQueryIsTimedAndCounted() throws Exception {
        try (QueryResponse response = client.query("SELECT value FROM " + TABLE).get()) {
            Assert.assertNotNull(response);
        }

        Timer duration = registry.find(MetricName.OPERATION_DURATION.getKey())
                .tag(MetricAttribute.DB_OPERATION_NAME.getKey(), "query")
                .tag(MetricAttribute.DB_NAMESPACE.getKey(), database)
                .timer();
        Assert.assertNotNull(duration, "Unexpected meters: " + registry.getMeters());
        Assert.assertEquals(duration.count(), 1L);
        Assert.assertTrue(duration.totalTime(TimeUnit.NANOSECONDS) > 0);

        Counter count = registry.find(MetricName.OPERATION_COUNT.getKey()).counter();
        Assert.assertNotNull(count);
        Assert.assertEquals(count.count(), 1d);
        Assert.assertEquals(duration.getId().getTag(MetricAttribute.ERROR_TYPE.getKey()),
                MicrometerMetricsRecorder.ABSENT_ATTRIBUTE_VALUE, "the operation succeeded");
    }

    @Test(groups = {"integration"})
    public void testSuccessfulInsertIsTimedWithItsSerializationStep() throws Exception {
        client.register(ValuePojo.class, client.getTableSchema(TABLE));
        registry.clear();

        client.insert(TABLE, Collections.singletonList(new ValuePojo("a"))).get().close();

        Timer duration = registry.find(MetricName.OPERATION_DURATION.getKey())
                .tag(MetricAttribute.DB_OPERATION_NAME.getKey(), "insert")
                .tag(MetricAttribute.DB_COLLECTION_NAME.getKey(), TABLE)
                .timer();
        Assert.assertNotNull(duration, "Unexpected meters: " + registry.getMeters());
        Assert.assertEquals(duration.count(), 1L);

        Timer serialization = registry.find(MetricName.OPERATION_SERIALIZATION_DURATION.getKey()).timer();
        Assert.assertNotNull(serialization);
        Assert.assertEquals(serialization.count(), 1L);
        Assert.assertTrue(serialization.totalTime(TimeUnit.NANOSECONDS) > 0);
        Assert.assertEquals(serialization.getId().getTag(MetricAttribute.DB_COLLECTION_NAME.getKey()), TABLE);
    }

    @Test(groups = {"integration"})
    public void testFailedQueryIsTimedWithTheServerErrorCode() {
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

        Timer duration = registry.find(MetricName.OPERATION_DURATION.getKey())
                .tag(MetricAttribute.ERROR_TYPE.getKey(), ServerException.class.getName())
                .tag(MetricAttribute.DB_RESPONSE_STATUS_CODE.getKey(), String.valueOf(ServerException.TABLE_NOT_FOUND))
                .timer();
        Assert.assertNotNull(duration, "Unexpected meters: " + registry.getMeters());
        Assert.assertEquals(duration.count(), 1L);
        Assert.assertTrue(duration.totalTime(TimeUnit.NANOSECONDS) > 0);

        Counter count = registry.find(MetricName.OPERATION_COUNT.getKey())
                .tag(MetricAttribute.ERROR_TYPE.getKey(), ServerException.class.getName()).counter();
        Assert.assertNotNull(count, "a failed operation is counted too");
        Assert.assertEquals(count.count(), 1d);
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
