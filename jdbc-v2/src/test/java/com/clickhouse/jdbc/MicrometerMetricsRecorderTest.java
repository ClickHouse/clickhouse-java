package com.clickhouse.jdbc;

import com.clickhouse.client.api.observability.MetricAttribute;
import com.clickhouse.client.api.observability.MetricName;
import com.clickhouse.client.api.observability.micrometer.MicrometerMetricsRecorder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class MicrometerMetricsRecorderTest extends JdbcIntegrationTest {

    private SimpleMeterRegistry registry;

    @BeforeMethod(groups = {"integration"})
    void setUp() {
        // the global registry keeps the meters of every recorder that reported to it, and hands them to
        // a registry added later, so each case starts from an empty one
        Metrics.globalRegistry.clear();
        registry = new SimpleMeterRegistry();
        Metrics.addRegistry(registry);
    }

    @AfterMethod(groups = {"integration"})
    void tearDown() {
        Metrics.removeRegistry(registry);
        Metrics.globalRegistry.clear();
        registry.close();
    }

    @Test(groups = {"integration"})
    public void testConnectionNamingTheRecorderReportsToTheGlobalRegistry() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(DriverProperties.METRICS_RECORDER.getKey(),
                MicrometerMetricsRecorder.class.getName());

        try (Connection connection = getJdbcConnection(properties);
             Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT 1")) {
                Assert.assertTrue(rs.next());
            }
        }

        Timer duration = registry.find(MetricName.OPERATION_DURATION.getKey())
                .tag(MetricAttribute.DB_OPERATION_NAME.getKey(), "query")
                .timer();
        Assert.assertNotNull(duration, "Unexpected meters: " + registry.getMeters());
        Assert.assertTrue(duration.count() > 0);
        Assert.assertTrue(duration.totalTime(TimeUnit.NANOSECONDS) > 0);
        Assert.assertEquals(duration.getId().getTag(MetricAttribute.DB_SYSTEM_NAME.getKey()), "clickhouse");

        Counter count = registry.find(MetricName.OPERATION_COUNT.getKey()).counter();
        Assert.assertNotNull(count);
        Assert.assertTrue(count.count() > 0);
    }

    @Test(groups = {"integration"})
    public void testConnectionsShareOneTimeSeries() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(DriverProperties.METRICS_RECORDER.getKey(),
                MicrometerMetricsRecorder.class.getName());

        for (int i = 0; i < 2; i++) {
            try (Connection connection = getJdbcConnection(properties);
                 Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("SELECT 1")) {
                    Assert.assertTrue(rs.next());
                }
            }
        }

        // each connection creates its own recorder, and all of them report to the global registry
        Assert.assertEquals(registry.find(MetricName.OPERATION_DURATION.getKey())
                .tag(MetricAttribute.DB_OPERATION_NAME.getKey(), "query").timers().size(), 1,
                "Unexpected meters: " + registry.getMeters());
    }

    @Test(groups = {"integration"})
    public void testConnectionWithoutThePropertyReportsNothing() throws Exception {
        try (Connection connection = getJdbcConnection();
             Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT 1")) {
                Assert.assertTrue(rs.next());
            }
        }

        Assert.assertNull(registry.find(MetricName.OPERATION_DURATION.getKey()).timer(),
                "a connection that did not ask for metrics must not report any");
    }
}
