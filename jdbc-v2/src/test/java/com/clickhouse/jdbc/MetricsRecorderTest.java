package com.clickhouse.jdbc;

import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.observability.DefaultMetricsRecorder;
import com.clickhouse.client.api.observability.MetricAttribute;
import com.clickhouse.client.api.observability.MetricsSupport;
import com.clickhouse.client.api.query.QuerySettings;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MetricsRecorderTest extends JdbcIntegrationTest {

    @BeforeMethod(groups = {"integration"})
    void setUp() {
        CollectingMetricsRecorder.clear();
    }

    @Test(groups = {"integration"})
    public void testConnectionReportsMetricsToConfiguredRecorder() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(DriverProperties.METRICS_RECORDER.getKey(), CollectingMetricsRecorder.class.getName());

        try (Connection connection = getJdbcConnection(properties);
             Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT 1")) {
                Assert.assertTrue(rs.next());
            }
        }

        List<Map<String, Object>> operations = CollectingMetricsRecorder.getOperations();
        Assert.assertFalse(operations.isEmpty(), "the recorder named by the property must be used");
        for (Map<String, Object> attributes : operations) {
            Assert.assertEquals(attributes.get(MetricAttribute.DB_SYSTEM_NAME.getKey()), "clickhouse");
            Assert.assertEquals(attributes.get(MetricAttribute.DB_OPERATION_NAME.getKey()), "query");
        }
    }

    @Test(groups = {"integration"})
    public void testConnectionWithoutThePropertyRecordsNothing() throws Exception {
        try (Connection connection = getJdbcConnection();
             Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT 1")) {
                Assert.assertTrue(rs.next());
            }
        }

        Assert.assertTrue(CollectingMetricsRecorder.getOperations().isEmpty(),
                "a connection that did not ask for metrics must not report any");
    }

    @Test(groups = {"integration"})
    public void testUnknownRecorderClassIsRejected() {
        Properties properties = new Properties();
        properties.setProperty(DriverProperties.METRICS_RECORDER.getKey(), "com.acme.NoSuchRecorder");

        try {
            getJdbcConnection(properties).close();
            Assert.fail("a recorder class that is not on the classpath must be rejected");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains(DriverProperties.METRICS_RECORDER.getKey()),
                    "Unexpected message: " + e.getMessage());
        }
    }

    @Test(groups = {"integration"})
    public void testClassThatIsNotARecorderIsRejected() {
        Properties properties = new Properties();
        properties.setProperty(DriverProperties.METRICS_RECORDER.getKey(), String.class.getName());

        try {
            getJdbcConnection(properties).close();
            Assert.fail("a class that does not implement the recorder interface must be rejected");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains(String.class.getName()),
                    "Unexpected message: " + e.getMessage());
        }
    }

    /**
     * Recorder the driver instantiates through the connection property, so what it collects is static.
     */
    public static class CollectingMetricsRecorder extends DefaultMetricsRecorder {

        private static final List<Map<String, Object>> OPERATIONS = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void recordQuerySuccess(QuerySettings settings, OperationMetrics metrics) {
            OPERATIONS.add(getMetricsSupport().queryAttributes(settings, null));
        }

        @Override
        public void recordQueryFailure(QuerySettings settings, Duration duration, Throwable t) {
            OPERATIONS.add(getMetricsSupport().queryAttributes(settings, t));
        }

        @Override
        public void recordInsertSuccess(InsertSettings settings, String tableName, OperationMetrics metrics) {
            OPERATIONS.add(MetricsSupport.DEFAULT.insertAttributes(settings, tableName, null));
        }

        static List<Map<String, Object>> getOperations() {
            synchronized (OPERATIONS) {
                return new ArrayList<>(OPERATIONS);
            }
        }

        static void clear() {
            OPERATIONS.clear();
        }
    }
}
