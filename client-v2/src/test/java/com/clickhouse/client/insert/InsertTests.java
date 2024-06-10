package com.clickhouse.client.insert;

import com.clickhouse.client.BaseIntegrationTest;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class InsertTests extends BaseIntegrationTest {
    private Client client;
    private InsertSettings settings;

    @BeforeMethod(groups = { "integration" }, enabled = true)
    public void setUp() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort(), false)
                .setUsername("default")
                .setPassword("")
                .build();
        settings = new InsertSettings()
                .setDeduplicationToken(RandomStringUtils.randomAlphabetic(36))
                .setQueryId(String.valueOf(UUID.randomUUID()));
    }

    private void createTable(String tableQuery) throws ClickHouseException {
        try (ClickHouseClient client = ClickHouseClient.builder().config(new ClickHouseConfig())
                        .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                        .build()) {
            client.read(getServer(ClickHouseProtocol.HTTP)).query(tableQuery).executeAndWait().close();
        }
    }

    @Test(groups = { "integration" }, enabled = true)
    public void insertSimplePOJOs() throws Exception {
        String tableName = "simple_pojo_table";
        String createSQL = SamplePOJO.generateTableCreateSQL(tableName);
        System.out.println(createSQL);
        createTable(createSQL);

        client.register(SamplePOJO.class, SamplePOJO.generateTableSchema(tableName));
        List<Object> simplePOJOs = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            simplePOJOs.add(new SamplePOJO());
        }
        InsertResponse response = client.insert(tableName, simplePOJOs, settings).get(30, TimeUnit.SECONDS);

        OperationMetrics metrics = response.getMetrics();
        assertEquals(simplePOJOs.size(), metrics.getMetric(ServerMetrics.NUM_ROWS_WRITTEN).getLong());
        assertEquals(simplePOJOs.size(), response.getWrittenRows());
        assertTrue(metrics.getMetric(ClientMetrics.OP_DURATION).getLong() > 0);
        assertTrue(metrics.getMetric(ClientMetrics.OP_SERIALIZATION).getLong() > 0);
    }
}
