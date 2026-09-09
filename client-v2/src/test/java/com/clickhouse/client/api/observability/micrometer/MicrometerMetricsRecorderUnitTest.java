package com.clickhouse.client.api.observability.micrometer;

import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.ClientStatisticsHolder;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.OperationType;
import com.clickhouse.client.api.observability.MetricAttribute;
import com.clickhouse.client.api.observability.MetricName;
import com.clickhouse.client.api.query.QuerySettings;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MicrometerMetricsRecorderUnitTest {

    private static final String DATABASE = "metrics_db";

    private SimpleMeterRegistry registry;
    private MicrometerMetricsRecorder recorder;

    @BeforeMethod
    void setUp() {
        registry = new SimpleMeterRegistry();
        recorder = new MicrometerMetricsRecorder(registry);
    }

    @AfterMethod
    void tearDown() {
        registry.close();
    }

    @Test
    public void testQuerySuccessRecordsDurationCountAndStandardTags() {
        recorder.recordQuerySuccess(querySettings(), measuredMetrics(OperationType.QUERY, ClientMetrics.OP_DURATION));

        Timer duration = onlyTimer(MetricName.OPERATION_DURATION);
        Assert.assertEquals(duration.count(), 1L);
        Assert.assertTrue(duration.totalTime(TimeUnit.NANOSECONDS) > 0,
                "Unexpected duration: " + duration.totalTime(TimeUnit.NANOSECONDS));
        Assert.assertEquals(tag(duration, MetricAttribute.DB_SYSTEM_NAME), "clickhouse");
        Assert.assertEquals(tag(duration, MetricAttribute.DB_NAMESPACE), DATABASE);
        Assert.assertEquals(tag(duration, MetricAttribute.DB_OPERATION_NAME), "query");
        Assert.assertEquals(tag(duration, MetricAttribute.ERROR_TYPE),
                MicrometerMetricsRecorder.ABSENT_ATTRIBUTE_VALUE, "the operation succeeded");
        Assert.assertEquals(tag(duration, MetricAttribute.DB_COLLECTION_NAME),
                MicrometerMetricsRecorder.ABSENT_ATTRIBUTE_VALUE, "a query has no table");

        Assert.assertEquals(onlyCounter(MetricName.OPERATION_COUNT).count(), 1d);
        Assert.assertTrue(timers(MetricName.OPERATION_SERIALIZATION_DURATION).isEmpty(),
                "the client measures the serialization step of an insert only");
        Assert.assertTrue(counters(MetricName.OPERATION_RETRIES).isEmpty(), "no attempt failed");
    }

    @Test
    public void testInsertSuccessRecordsSerializationDurationAndTargetTable() {
        recorder.recordInsertSuccess(insertSettings(), "events",
                measuredMetrics(OperationType.INSERT, ClientMetrics.OP_DURATION, ClientMetrics.OP_SERIALIZATION));

        Timer duration = onlyTimer(MetricName.OPERATION_DURATION);
        Assert.assertEquals(duration.count(), 1L);
        Assert.assertEquals(tag(duration, MetricAttribute.DB_OPERATION_NAME), "insert");
        Assert.assertEquals(tag(duration, MetricAttribute.DB_COLLECTION_NAME), "events");

        Timer serialization = onlyTimer(MetricName.OPERATION_SERIALIZATION_DURATION);
        Assert.assertEquals(serialization.count(), 1L);
        Assert.assertTrue(serialization.totalTime(TimeUnit.NANOSECONDS) > 0);
        Assert.assertEquals(tag(serialization, MetricAttribute.DB_COLLECTION_NAME), "events");

        Assert.assertEquals(onlyCounter(MetricName.OPERATION_COUNT).count(), 1d);
    }

    @Test
    public void testDurationTheClientDidNotMeasureIsNotRecorded() {
        recorder.recordQuerySuccess(querySettings(), null);

        Assert.assertTrue(timers(MetricName.OPERATION_DURATION).isEmpty(),
                "a duration the client did not measure must not be recorded as a value");
        Assert.assertTrue(timers(MetricName.OPERATION_SERIALIZATION_DURATION).isEmpty());
        Assert.assertEquals(onlyCounter(MetricName.OPERATION_COUNT).count(), 1d,
                "the operation still completed and is counted");
    }

    @Test
    public void testFailedOperationRecordsMeasuredDurationAndErrorType() {
        IllegalStateException failure = new IllegalStateException("boom");

        recorder.recordQueryFailure(querySettings(), Duration.ofMillis(1500), failure);

        Timer duration = onlyTimer(MetricName.OPERATION_DURATION);
        Assert.assertEquals(duration.count(), 1L);
        Assert.assertEquals(duration.totalTime(TimeUnit.SECONDS), 1.5d, 1e-6,
                "the seconds of the SPI must reach the registry unscaled");
        Assert.assertEquals(tag(duration, MetricAttribute.ERROR_TYPE), IllegalStateException.class.getName());
        Assert.assertEquals(tag(duration, MetricAttribute.DB_RESPONSE_STATUS_CODE),
                MicrometerMetricsRecorder.ABSENT_ATTRIBUTE_VALUE,
                "a client-side failure carries no server error code");

        Counter count = onlyCounter(MetricName.OPERATION_COUNT);
        Assert.assertEquals(count.count(), 1d, "a failed operation is counted too");
        Assert.assertEquals(tag(count, MetricAttribute.ERROR_TYPE), IllegalStateException.class.getName());
    }

    @Test
    public void testServerFailureCarriesTheServerErrorCode() {
        ServerException serverException = new ServerException(60, "table not found", 404, "q-1");

        recorder.recordInsertFailure(insertSettings(), "events", Duration.ofMillis(250),
                new RuntimeException(serverException));

        Timer duration = onlyTimer(MetricName.OPERATION_DURATION);
        Assert.assertEquals(duration.totalTime(TimeUnit.SECONDS), 0.25d, 1e-6);
        Assert.assertEquals(tag(duration, MetricAttribute.ERROR_TYPE), ServerException.class.getName());
        Assert.assertEquals(tag(duration, MetricAttribute.DB_RESPONSE_STATUS_CODE), "60");
        Assert.assertEquals(tag(duration, MetricAttribute.DB_COLLECTION_NAME), "events");
    }

    @Test
    public void testSuccessAndFailureOfTheSameMetricAreSeparateTimeSeries() {
        recorder.recordQuerySuccess(querySettings(), measuredMetrics(OperationType.QUERY, ClientMetrics.OP_DURATION));
        recorder.recordQueryFailure(querySettings(), Duration.ofMillis(100), new IllegalStateException("boom"));

        List<Timer> durations = timers(MetricName.OPERATION_DURATION);
        Assert.assertEquals(durations.size(), 2, "Unexpected timers: " + durations);
        Assert.assertEquals(registry.find(MetricName.OPERATION_DURATION.getKey())
                .tag(MetricAttribute.ERROR_TYPE.getKey(), IllegalStateException.class.getName())
                .timer().count(), 1L);
        Assert.assertEquals(counters(MetricName.OPERATION_COUNT).size(), 2,
                "the number of operations by outcome is the count of separate series");
    }

    @Test
    public void testEveryMeterOfAMetricCarriesTheSameTagKeys() {
        recorder.recordQuerySuccess(querySettings(), measuredMetrics(OperationType.QUERY, ClientMetrics.OP_DURATION));
        recorder.recordQueryFailure(querySettings(), Duration.ofMillis(100),
                new RuntimeException(new ServerException(60, "table not found", 404, "q-1")));
        recorder.recordInsertSuccess(insertSettings(), "events",
                measuredMetrics(OperationType.INSERT, ClientMetrics.OP_DURATION));
        recorder.recordInsertRetry(insertSettings(), "events", new IllegalStateException("boom"));

        List<String> expectedKeys = new ArrayList<>();
        for (MetricAttribute attribute : MetricAttribute.values()) {
            expectedKeys.add(attribute.getKey());
        }
        Collections.sort(expectedKeys);
        for (Meter meter : registry.getMeters()) {
            List<String> keys = new ArrayList<>();
            for (Tag tag : meter.getId().getTags()) {
                keys.add(tag.getKey());
                Assert.assertNotNull(tag.getValue(), "Unexpected tag: " + tag);
            }
            Collections.sort(keys);
            Assert.assertEquals(keys, expectedKeys, "Unexpected tag keys of " + meter.getId());
        }
    }

    @Test
    public void testEveryOutcomeOfAMetricIsExportedWithTheSameLabels() {
        PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MicrometerMetricsRecorder prometheusRecorder = new MicrometerMetricsRecorder(prometheusRegistry);
        try {
            prometheusRecorder.recordQuerySuccess(querySettings(),
                    measuredMetrics(OperationType.QUERY, ClientMetrics.OP_DURATION));
            prometheusRecorder.recordInsertFailure(insertSettings(), "events", Duration.ofMillis(100),
                    new RuntimeException(new ServerException(60, "table not found", 404, "q-1")));

            List<String> series = new ArrayList<>();
            for (String line : prometheusRegistry.scrape().split("\n")) {
                if (line.startsWith("db_client_operation_duration_seconds_count")) {
                    series.add(line);
                }
            }
            Assert.assertEquals(series.size(), 2, "Unexpected series: " + series);
            for (String line : series) {
                for (MetricAttribute attribute : MetricAttribute.values()) {
                    Assert.assertTrue(line.contains(attribute.getKey().replace('.', '_') + "="),
                            "Series without " + attribute.getKey() + ": " + line);
                }
            }
        } finally {
            prometheusRegistry.close();
        }
    }

    @Test
    public void testFailureWithoutAMeasuredDurationIsStillCounted() {
        recorder.recordQueryFailure(querySettings(), null, new IllegalStateException("boom"));

        Assert.assertTrue(timers(MetricName.OPERATION_DURATION).isEmpty(),
                "a duration the client did not measure must not be recorded as a value");
        Counter count = onlyCounter(MetricName.OPERATION_COUNT);
        Assert.assertEquals(count.count(), 1d, "the operation still failed and is counted");
        Assert.assertEquals(tag(count, MetricAttribute.ERROR_TYPE), IllegalStateException.class.getName());
    }

    @Test
    public void testEachRetriedAttemptIsCounted() {
        recorder.recordQueryRetry(querySettings(), new IllegalStateException("first"));
        recorder.recordQueryRetry(querySettings(), new IllegalStateException("second"));

        Counter retries = onlyCounter(MetricName.OPERATION_RETRIES);
        Assert.assertEquals(retries.count(), 2d, "an operation that succeeds on its third attempt reports two retries");
        Assert.assertEquals(tag(retries, MetricAttribute.DB_OPERATION_NAME), "query");
        Assert.assertEquals(tag(retries, MetricAttribute.ERROR_TYPE), IllegalStateException.class.getName());
        Assert.assertTrue(counters(MetricName.OPERATION_COUNT).isEmpty(), "a retried attempt is not a completion");
    }

    @Test
    public void testInsertRetryCarriesTheTargetTable() {
        recorder.recordInsertRetry(insertSettings(), "events", new IllegalStateException("boom"));

        Counter retries = onlyCounter(MetricName.OPERATION_RETRIES);
        Assert.assertEquals(retries.count(), 1d);
        Assert.assertEquals(tag(retries, MetricAttribute.DB_OPERATION_NAME), "insert");
        Assert.assertEquals(tag(retries, MetricAttribute.DB_COLLECTION_NAME), "events");
    }

    @Test(dataProvider = "reportedMetrics")
    public void testMeterIsRegisteredWithTheStandardNameAndDescription(MetricName name) {
        recorder.recordInsertSuccess(insertSettings(), "events",
                measuredMetrics(OperationType.INSERT, ClientMetrics.OP_DURATION, ClientMetrics.OP_SERIALIZATION));
        recorder.recordInsertRetry(insertSettings(), "events", new IllegalStateException("boom"));

        Meter meter = registry.find(name.getKey()).meter();
        Assert.assertNotNull(meter, "no meter named " + name.getKey());
        Assert.assertEquals(meter.getId().getDescription(), name.getDescription());
        if (meter instanceof Timer) {
            // a timer keeps the base time unit of the registry, the SPI unit of a duration is the same one
            Assert.assertEquals(meter.getId().getBaseUnit(), "seconds");
        } else {
            Assert.assertNull(meter.getId().getBaseUnit(),
                    "a UCUM annotation names what is counted and would end up in the name of the meter");
        }
    }

    @DataProvider(name = "reportedMetrics")
    public static Object[][] reportedMetrics() {
        return new Object[][]{
                {MetricName.OPERATION_DURATION},
                {MetricName.OPERATION_SERIALIZATION_DURATION},
                {MetricName.OPERATION_COUNT},
                {MetricName.OPERATION_RETRIES},
        };
    }

    @Test
    public void testRecorderWithoutARegistryReportsToTheGlobalOne() {
        SimpleMeterRegistry globalMember = new SimpleMeterRegistry();
        Metrics.addRegistry(globalMember);
        try {
            MicrometerMetricsRecorder globalRecorder = new MicrometerMetricsRecorder();
            Assert.assertSame(globalRecorder.getRegistry(), Metrics.globalRegistry);

            globalRecorder.recordQueryFailure(querySettings(), Duration.ofMillis(100),
                    new IllegalStateException("boom"));

            Timer duration = globalMember.find(MetricName.OPERATION_DURATION.getKey()).timer();
            Assert.assertNotNull(duration, "the meters must reach the registries of the global one");
            Assert.assertEquals(duration.count(), 1L);
        } finally {
            Metrics.removeRegistry(globalMember);
            globalMember.close();
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "registry must not be null")
    public void testRecorderWithoutAValidRegistryIsRejected() {
        new MicrometerMetricsRecorder(null);
    }

    private OperationMetrics measuredMetrics(OperationType type, ClientMetrics... measured) {
        ClientStatisticsHolder holder = new ClientStatisticsHolder();
        for (ClientMetrics metric : measured) {
            holder.start(metric);
        }
        OperationMetrics metrics = new OperationMetrics(holder, type);
        metrics.operationComplete();
        return metrics;
    }

    private QuerySettings querySettings() {
        return new QuerySettings().setDatabase(DATABASE);
    }

    private InsertSettings insertSettings() {
        return new InsertSettings().setDatabase(DATABASE);
    }

    private Timer onlyTimer(MetricName name) {
        List<Timer> found = timers(name);
        Assert.assertEquals(found.size(), 1, "Expected exactly one " + name.getKey() + " but got " + found);
        return found.get(0);
    }

    private Counter onlyCounter(MetricName name) {
        List<Counter> found = counters(name);
        Assert.assertEquals(found.size(), 1, "Expected exactly one " + name.getKey() + " but got " + found);
        return found.get(0);
    }

    private List<Timer> timers(MetricName name) {
        return new ArrayList<>(registry.find(name.getKey()).timers());
    }

    private List<Counter> counters(MetricName name) {
        return new ArrayList<>(registry.find(name.getKey()).counters());
    }

    private static String tag(Meter meter, MetricAttribute attribute) {
        for (Tag tag : meter.getId().getTags()) {
            if (tag.getKey().equals(attribute.getKey())) {
                return tag.getValue();
            }
        }
        return null;
    }
}
