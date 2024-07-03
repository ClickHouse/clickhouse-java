package com.clickhouse.client;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.enums.ProxyType;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.insert.SamplePOJO;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ProxyTests extends BaseIntegrationTest{
    private Client client;
    ToxiproxyContainer toxiproxy = null;
    ToxiproxyClient toxiproxyClient = null;
    Proxy proxy = null;

    @BeforeMethod(groups = { "integration" }, enabled = false)
    public void setUp() throws IOException {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        toxiproxy = new ToxiproxyContainer(ClickHouseServerForTest.getProxyImage())
                .withNetwork(ClickHouseServerForTest.getNetwork());
        toxiproxy.start();

        toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        proxy = toxiproxyClient.createProxy("clickhouse", "0.0.0.0:8666",
                ClickHouseServerForTest.hasClickHouseContainer()
                        ? "clickhouse:" + ClickHouseProtocol.HTTP.getDefaultPort()
                        : ClickHouseServerForTest.getClickHouseAddress(ClickHouseProtocol.HTTP, true));

        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), false)
                .setUsername("default")
                .setPassword("")
                .addProxy(ProxyType.HTTP, toxiproxy.getHost(), toxiproxy.getMappedPort(8666))
                .build();
    }

    @AfterMethod(groups = { "integration" }, enabled = false)
    public void teardown() {
        if (toxiproxy != null) {
            toxiproxy.stop();
        }
    }

    private void createTable(String tableQuery) throws ClickHouseException {
        try (ClickHouseClient client = ClickHouseClient.builder().config(new ClickHouseConfig())
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build()) {
            client.read(getServer(ClickHouseProtocol.HTTP)).query(tableQuery).executeAndWait().close();
        }
    }


    @Test(groups = { "integration" }, enabled = false)
    public void simpleProxyTest() throws Exception {
        String tableName = "simple_pojo_enable_proxy_table";
        String createSQL = SamplePOJO.generateTableCreateSQL(tableName);
        System.out.println(createSQL);
        createTable(createSQL);

        client.register(SamplePOJO.class, client.getTableSchema(tableName, "default"));
        List<Object> simplePOJOs = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            simplePOJOs.add(new SamplePOJO());
        }
        proxy.enable();
        InsertResponse response = client.insert(tableName, simplePOJOs).get(120, TimeUnit.SECONDS);

        OperationMetrics metrics = response.getMetrics();
        assertEquals(simplePOJOs.size(), metrics.getMetric(ServerMetrics.NUM_ROWS_WRITTEN).getLong());
        assertEquals(simplePOJOs.size(), response.getWrittenRows());
        assertTrue(metrics.getMetric(ClientMetrics.OP_DURATION).getLong() > 0);
        assertTrue(metrics.getMetric(ClientMetrics.OP_SERIALIZATION).getLong() > 0);
    }

    @Test(groups = { "integration" }, enabled = false)
    public void simpleDisabledProxyTest() throws Exception {
        String tableName = "simple_pojo_disable_proxy_table";
        String createSQL = SamplePOJO.generateTableCreateSQL(tableName);
        System.out.println(createSQL);
        createTable(createSQL);

        client.register(SamplePOJO.class, client.getTableSchema(tableName, "default"));
        List<Object> simplePOJOs = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            simplePOJOs.add(new SamplePOJO());
        }
        proxy.disable();
        try {
            InsertResponse response = client.insert(tableName, simplePOJOs).get(120, TimeUnit.SECONDS);
            fail("Should have thrown exception.");
        } catch (Exception e) {
            assertTrue(e instanceof ClientException);
        }
    }
}
