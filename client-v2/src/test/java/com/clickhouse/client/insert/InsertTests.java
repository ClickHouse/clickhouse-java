package com.clickhouse.client.insert;

import com.clickhouse.client.*;
import com.clickhouse.client.api.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.Client;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.testng.Assert.*;

public class InsertTests extends BaseIntegrationTest {
    private Client client;
    private InsertSettings settings;

    @BeforeMethod(groups = { "unit" }, enabled = true)
    public void setUp() {
        ClickHouseNode node = getServer(ClickHouseProtocol.HTTP);
        client = new Client.Builder()
                .addEndpoint(Protocol.HTTP, node.getHost(), node.getPort())
                .addUsername("default")
                .addPassword("")
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

    @Test(groups = { "unit" }, enabled = true)
    public void insertSimplePOJOs() throws ClickHouseException, IOException, ExecutionException, InterruptedException, InvocationTargetException, IllegalAccessException {
        String tableName = "simple_pojo_table";
        String createSQL = SamplePOJO.generateTableCreateSQL(tableName);
        System.out.println(createSQL);
        createTable(createSQL);

        client.register(SamplePOJO.class, SamplePOJO.generateTableSchema(tableName));
        List<Object> simplePOJOs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            simplePOJOs.add(new SamplePOJO());
        }
        InsertResponse response = client.insert(tableName, simplePOJOs, settings);

        ClickHouseResponseSummary summary = response.getSummary();
        assertNotEquals(summary, null);
        assertEquals(simplePOJOs.size(), summary.getWrittenRows());
    }
}
