package com.clickhouse.client.metrics;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.internal.ServerSettings;
import com.clickhouse.client.api.query.QueryResponse;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class MetricsTest extends BaseIntegrationTest {
    private MeterRegistry meterRegistry;
    @BeforeMethod(groups = {"integration"})
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
    }

    @AfterMethod(groups = {"integration"})
    void tearDown() {
        meterRegistry.clear();
        Metrics.globalRegistry.clear();
    }

    @Test(groups = { "integration" }, enabled = true)
    public void testRegisterMetrics() throws Exception {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        boolean isSecure = isCloud();

        try (Client client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), isSecure)
                .setUsername("default")
                .setPassword(ClickHouseServerForTest.getPassword())
                .setDefaultDatabase(ClickHouseServerForTest.getDatabase())
                .serverSetting(ServerSettings.ASYNC_INSERT, "0")
                .serverSetting(ServerSettings.WAIT_END_OF_QUERY, "1")
                .registerClientMetrics(meterRegistry, "pool-test")
                .build()) {

            Gauge totalMax = meterRegistry.get("httpcomponents.httpclient.pool.total.max").gauge();
            Gauge available = meterRegistry.get("httpcomponents.httpclient.pool.total.connections").tags("state", "available").gauge();
            Gauge leased = meterRegistry.get("httpcomponents.httpclient.pool.total.connections").tags("state", "leased").gauge();

            System.out.println("totalMax:" + totalMax.value() + ", available: " + available.value() + ", leased: " + leased.value());
            Assert.assertEquals((int)totalMax.value(), Integer.parseInt(ClientConfigProperties.HTTP_MAX_OPEN_CONNECTIONS.getDefaultValue()));
            Assert.assertEquals((int)available.value(), 1);
            Assert.assertEquals((int)leased.value(), 0);

            try (QueryResponse response = client.query("SELECT 1").get()) {
                Assert.assertEquals((int)available.value(), 0);
                Assert.assertEquals((int)leased.value(), 1);
            }

            Assert.assertEquals((int)available.value(), 1);
            Assert.assertEquals((int)leased.value(), 0);
        }
        // currently there are  only 5 metrics that are monitored by micrometer (out of the box)
        assertEquals(meterRegistry.getMeters().size(), 5);
    }
}
