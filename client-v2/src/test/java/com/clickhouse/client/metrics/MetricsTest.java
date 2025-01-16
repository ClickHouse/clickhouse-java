package com.clickhouse.client.metrics;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.internal.ServerSettings;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class MetricsTest extends BaseIntegrationTest {
    private MeterRegistry meterRegistry;
    @BeforeMethod
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
    }

    @AfterMethod
    void tearDown() {
        meterRegistry.clear();
        Metrics.globalRegistry.clear();
    }

    @Test(groups = { "integration" }, enabled = true)
    public void testRegisterMetrics() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();
        Client client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .serverSetting(ServerSettings.ASYNC_INSERT, "0")
                .serverSetting(ServerSettings.WAIT_END_OF_QUERY, "1")
                .useNewImplementation(System.getProperty("client.tests.useNewImplementation", "true").equals("true"))
                .addMetric(meterRegistry, "pool-test")
                .build();
        // currently there are  only 5 metrics that are monitored by micrometer (out of the box)
        assertEquals(meterRegistry.getMeters().size(), 5);
    }
}
